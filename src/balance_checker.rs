use alloy::contract::SolCallBuilder;
use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use alloy::transports::BoxTransport;
use anyhow::{Context, Result};
use futures::future::try_join_all;
use log::{debug, error, info, warn};
use polars::io::parquet::*;
use polars::prelude::*;
use std::future::IntoFuture;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use ERC20::ERC20Instance;

const BATCH_SIZE: usize = 1000;

sol! {
    #[sol(rpc)]
    contract ERC20 {
        function balanceOf(address owner) external view returns (uint256);
    }
}

use std::collections::HashMap;

fn create_token_symbol_mapping() -> HashMap<Address, String> {
    let mut symbols = HashMap::new();
    symbols.insert(
        Address::from_str("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9").unwrap(),
        "USDT".to_string(),
    );
    symbols.insert(
        Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831").unwrap(),
        "USDC".to_string(),
    );
    symbols.insert(
        Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1").unwrap(),
        "DAI".to_string(),
    );
    symbols.insert(
        Address::from_str("0x912CE59144191C1204E64559FE8253a0e49E6548").unwrap(),
        "ARB".to_string(),
    );
    symbols.insert(
        Address::from_str("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1").unwrap(),
        "WETH".to_string(),
    );
    symbols
}

async fn fetch_balance(
    contract: Arc<ERC20Instance<BoxTransport, Arc<impl Provider>>>,
    addr: Address,
) -> Result<Option<String>> {
    // Add retry logic with exponential backoff
    let mut retries = 0;
    let max_retries = 3;
    let mut delay = Duration::from_millis(1000);

    loop {
        match contract.balanceOf(addr).call().await {
            Ok(balance) => {
                info!("Successfully fetched balance for address {}", addr);
                return Ok(Some(balance._0.to_string()));
            }
            Err(e) => {
                if retries >= max_retries {
                    error!(
                        "Failed to fetch balance after {} retries for address {}: {}",
                        max_retries, addr, e
                    );
                    return Ok(None);
                }
                warn!("Retry {} for address {}: {}", retries + 1, addr, e);
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff
                retries += 1;
            }
        }
    }
}

async fn fetch_all_balances(
    provider: Arc<impl alloy::providers::Provider>,
    token_addresses: &[Address],
    account_addresses: &[Address],
) -> Result<Vec<Vec<Option<String>>>> {
    let mut all_balances = Vec::new();

    for &token_addr in token_addresses {
        let contract = Arc::new(ERC20::new(token_addr, Arc::clone(&provider)));
        let mut all_token_balances = Vec::new();

        // Process in smaller batches with delay between batches
        for addresses_chunk in account_addresses.chunks(500) {
            let mut balance_futures = Vec::new();

            for &addr in addresses_chunk {
                let contract = Arc::clone(&contract);
                balance_futures.push(fetch_balance(contract, addr));
            }

            let chunk_balances = try_join_all(balance_futures).await?;
            all_token_balances.extend(chunk_balances);

            // Add delay between batches to avoid rate limiting
            // tokio::time::sleep(Duration::from_millis(2000)).await;
        }

        all_balances.push(all_token_balances);
    }

    Ok(all_balances)
}

/// Save profiling results to a parquet file
fn save_profiling_results(
    total_time: f64,
    read_addresses_time: f64,
    fetch_balances_time: f64,
    output_path: &str,
) -> Result<()> {
    info!("Saving profiling results to parquet file");

    let operation = Series::new(
        "operation".into(),
        &["total_execution", "read_addresses", "fetch_balances"],
    );
    let time_seconds = Series::new(
        "time_seconds".into(),
        &[total_time, read_addresses_time, fetch_balances_time],
    );

    let df = DataFrame::new(vec![
        polars::prelude::Column::Series(operation),
        polars::prelude::Column::Series(time_seconds),
    ])
    .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;

    df.lazy()
        .sink_parquet(output_path, ParquetWriteOptions::default())
        .map_err(|e| anyhow::anyhow!("Failed to write parquet file: {}", e))?;

    info!("Successfully saved profiling results to {}", output_path);
    Ok(())
}

/// Read addresses from a parquet file
fn read_addresses_from_parquet(file_path: &str) -> Result<Vec<Address>> {
    info!("Reading addresses from parquet file: {}", file_path);

    // Read the parquet file
    let df = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())
        .map_err(|e| {
            error!("Failed to scan parquet file: {}", e);
            e
        })?
        .select([col("address")])
        .collect()
        .map_err(|e| {
            error!("Failed to collect parquet data: {}", e);
            e
        })?;

    info!("Successfully loaded parquet file");

    // Convert all columns to strings and collect unique addresses
    let mut addresses = Vec::new();
    for column in df.get_columns() {
        if let Ok(series_str) = column.cast(&DataType::String) {
            if let Ok(str_iter) = series_str.str() {
                // Process each address string, logging errors but continuing
                for opt_str in str_iter {
                    if let Some(addr_str) = opt_str {
                        match Address::from_str(addr_str) {
                            Ok(address) => addresses.push(address),
                            Err(e) => warn!("Failed to parse address {}: {}", addr_str, e),
                        }
                    }
                }
            } else {
                warn!("Failed to convert column to string iterator, skipping");
            }
        } else {
            warn!("Failed to cast column to string, skipping");
        }
    }

    // Remove duplicates
    addresses.sort_unstable();
    addresses.dedup();

    info!("Found {} unique valid addresses", addresses.len());
    Ok(addresses)
}

/// Save token balances to a parquet file
fn save_token_balances(
    account_addresses: &[Address],
    token_addresses: &[Address],
    all_balances: &[Vec<Option<String>>],
    token_symbols: &HashMap<Address, String>,
    output_path: &str,
) -> Result<()> {
    info!("Saving token balances to parquet file");

    let mut account_addr_vec = Vec::new();
    let mut token_addr_vec = Vec::new();
    let mut token_symbol_vec = Vec::new();
    let mut balance_vec = Vec::new();

    // Flatten the data into vectors
    for (token_idx, token_addr) in token_addresses.iter().enumerate() {
        let un = &"UNKNOWN".to_string();
        let symbol = token_symbols.get(token_addr).unwrap_or(un);
        for (account_idx, account_addr) in account_addresses.iter().enumerate() {
            account_addr_vec.push(account_addr.to_string());
            token_addr_vec.push(token_addr.to_string());
            token_symbol_vec.push(symbol.clone());
            balance_vec.push(match &all_balances[token_idx][account_idx] {
                Some(balance) => balance,
                None => "0",
            });
        }
    }

    // Create DataFrame
    let df = DataFrame::new(vec![
        Column::Series(Series::new("account_address".into(), account_addr_vec)),
        Column::Series(Series::new("token_address".into(), token_addr_vec)),
        Column::Series(Series::new("token_symbol".into(), token_symbol_vec)),
        Column::Series(Series::new("balance".into(), balance_vec)),
    ])
    .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;

    // Save to parquet file
    df.lazy()
        .sink_parquet(output_path, ParquetWriteOptions::default())
        .map_err(|e| anyhow::anyhow!("Failed to write parquet file: {}", e))?;

    info!("Successfully saved token balances to {}", output_path);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();

    // Initialize logger
    env_logger::init();
    info!("Starting token balance checker application");

    // Initialize provider with increased timeout
    info!("Initializing Ethereum provider");
    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .on_builtin("https://arbitrum-mainnet.core.chainstack.com/xxxxxxx")
        .await
        .map_err(|e| {
            error!("Failed to initialize provider: {}", e);
            e
        })?;
    let provider = Arc::new(provider);
    info!("Provider successfully initialized");

    // List of token addresses
    info!("Setting up token addresses");
    let token_addresses = vec![
        Address::from_str("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9")?, // USDT
        Address::from_str("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")?, // USDC
        Address::from_str("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1")?, // DAI
        Address::from_str("0x912CE59144191C1204E64559FE8253a0e49E6548")?, // ARB
        Address::from_str("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")?, // WETH
    ];
    info!("Configured {} tokens for checking", token_addresses.len());

    // Read account addresses
    let read_addresses_start = Instant::now();
    let account_addresses = read_addresses_from_parquet("trainig_data.parquet")
        .context("Failed to read addresses from parquet file")?;
    let read_addresses_time = read_addresses_start.elapsed().as_secs_f64();
    info!(
        "Address reading completed in {:.2} seconds",
        read_addresses_time
    );

    if account_addresses.is_empty() {
        warn!("No valid addresses found in the parquet file");
        return Ok(());
    }

    // Create token symbol mapping
    info!("Creating token symbol mapping");
    let token_symbols = create_token_symbol_mapping();

    // Fetch all balances
    info!("Starting balance fetching");
    let fetch_balances_start = Instant::now();
    let all_balances =
        fetch_all_balances(Arc::clone(&provider), &token_addresses, &account_addresses).await?;
    let fetch_balances_time = fetch_balances_start.elapsed().as_secs_f64();
    info!(
        "Balance fetching completed in {:.2} seconds",
        fetch_balances_time
    );

    // Print results
    info!("Processing and displaying results");
    for (token_addr, balances) in token_addresses.iter().zip(all_balances.iter()) {
        let symbol = token_symbols.get(token_addr).unwrap();
        info!("Processing results for token: {} ({})", token_addr, symbol);
        println!("\nToken: {} ({})", token_addr, symbol);
        println!("----------------------------------------");
        for (address, balance_opt) in account_addresses.iter().zip(balances.iter()) {
            match balance_opt {
                Some(balance) => {
                    info!("Address: {} - Balance: {} {}", address, balance, symbol);
                    println!("Address: {}", address);
                    println!("Balance: {} {}\n", balance, symbol);
                }
                None => {
                    warn!("No balance data available for address: {}", address);
                    println!("Address: {}", address);
                    println!("Balance: Failed to fetch\n");
                }
            }
        }
    }

    let total_time = start_time.elapsed().as_secs_f64();
    info!("Token balance check completed in {:.2} seconds", total_time);

    // Save results
    save_token_balances(
        &account_addresses,
        &token_addresses,
        &all_balances,
        &token_symbols,
        "token_balances.parquet",
    )?;

    save_profiling_results(
        total_time,
        read_addresses_time,
        fetch_balances_time,
        "profiling_results.parquet",
    )?;

    Ok(())
}
