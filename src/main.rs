use glob::glob;
use hypersync_client::{
    format::{Address, Hex},
    Client, ClientConfig, Decoder, StreamConfig,
};
use polars::lazy::dsl::col;
use polars::prelude::*;
use std::{collections::HashMap, sync::Arc, time::Instant};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init().unwrap();

    let network = "arbitrum";

    let config = ClientConfig {
        url: Some(Url::parse(&format!("https://{network}.hypersync.xyz/")).unwrap()),
        ..Default::default()
    };

    let client = Arc::new(Client::new(config).unwrap());

    let from_block = get_latest_block_number()?;

    let query = serde_json::from_value(serde_json::json!({
        "from_block": from_block,
        "logs": [{
            "topics": [
                ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
            ]
        }],
        "field_selection": {
            "log": [
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
                "transaction_hash",
                "block_number",
                "address"
            ],
            "transaction": [
                "from",
                "to",
                "gas_used",
                "hash",
                "block_number"
            ]
        }
    }))
    .unwrap();

    println!("Starting the stream");
    let mut receiver = client.stream(query, StreamConfig::default()).await.unwrap();

    let decoder = Decoder::from_signatures(&[
        "Transfer(address indexed from, address indexed to, uint amount)",
    ])
    .unwrap();

    // Vectors to store data for DataFrame
    let mut block_numbers: Vec<String> = Vec::new();
    let mut tx_hashes: Vec<String> = Vec::new();
    let mut contract_addresses: Vec<String> = Vec::new();
    let mut from_addresses: Vec<String> = Vec::new();
    let mut to_addresses: Vec<String> = Vec::new();
    let mut amounts_str: Vec<String> = Vec::new(); // Store amounts as strings to preserve full precision
    let mut gas_used: Vec<u64> = Vec::new();

    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        // Create a HashMap of transactions keyed by their hash
        let mut tx_map: HashMap<String, (Address, Address)> = HashMap::new();

        // Process transactions first
        for batch in res.data.transactions {
            for tx in batch {
                if let (Some(from), Some(to), Some(hash), Some(gas)) =
                    (tx.from, tx.to, tx.hash, tx.gas_used)
                {
                    tx_map.insert(hash.encode_hex(), (from, to));
                }
            }
        }

        // Process logs and associate them with transactions
        for batch in res.data.logs {
            for log in batch {
                if let (Ok(Some(decoded_log)), Some(tx_hash), Some(block_number)) = (
                    decoder.decode_log(&log),
                    log.transaction_hash,
                    log.block_number,
                ) {
                    let amount = decoded_log.body[0].as_uint().unwrap();
                    let from = decoded_log.indexed[0].as_address().unwrap();
                    let to = decoded_log.indexed[1].as_address().unwrap();
                    let contract_address = log.address.unwrap();

                    // Get associated transaction data
                    if let Some((tx_from, tx_to)) = tx_map.get(&tx_hash.encode_hex()) {
                        // Store data in vectors
                        block_numbers.push(block_number.to_string());
                        tx_hashes.push(tx_hash.encode_hex());
                        contract_addresses.push(contract_address.encode_hex());
                        from_addresses.push(from.to_string());
                        to_addresses.push(to.to_string());
                        // Convert amount to string to preserve full precision
                        amounts_str.push(amount.0.to_string());
                    }
                }
            }
        }

        println!("Processed block: {}", res.next_block);

        // Save to parquet file every 5000 blocks
        if block_numbers.len() >= 5000 {
            save_to_parquet(
                &block_numbers,
                &tx_hashes,
                &contract_addresses,
                &from_addresses,
                &to_addresses,
                &amounts_str,
                &network,
            )?;

            // Clear vectors after saving
            block_numbers.clear();
            tx_hashes.clear();
            contract_addresses.clear();
            from_addresses.clear();
            to_addresses.clear();
            amounts_str.clear();
        }
    }

    // Save any remaining data
    if !block_numbers.is_empty() {
        save_to_parquet(
            &block_numbers,
            &tx_hashes,
            &contract_addresses,
            &from_addresses,
            &to_addresses,
            &amounts_str,
            &network,
        )?;
    }

    Ok(())
}

fn save_to_parquet(
    block_numbers: &[String],
    tx_hashes: &[String],
    contract_addresses: &[String],
    from_addresses: &[String],
    to_addresses: &[String],
    amounts: &[String],
    network: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use polars::prelude::*;

    let df = DataFrame::new(vec![
        Series::new("block_number".into(), block_numbers.to_vec()).into(),
        Series::new("tx_hash".into(), tx_hashes.to_vec()).into(),
        Series::new("contract_address".into(), contract_addresses.to_vec()).into(),
        Series::new("from_address".into(), from_addresses.to_vec()).into(),
        Series::new("to_address".into(), to_addresses.to_vec()).into(),
        Series::new("amount".into(), amounts.to_vec()).into(),
    ])?;

    // Generate filename with timestamp
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("erc20_transfers_{}_{}.parquet", network, timestamp);

    // Save to parquet file with compression
    let mut file = std::fs::File::create(filename.clone())?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;

    println!("Saved {} records to {}", df.height(), filename);
    Ok(())
}

fn get_latest_block_number() -> Result<u64, Box<dyn std::error::Error>> {
    let mut latest_block = 0u64;

    // Search for all parquet files matching the pattern
    for entry in glob("erc20_transfers_*.parquet")? {
        match entry {
            Ok(path) => {
                // Read the parquet file
                let df = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?
                    .select([col("block_number")])
                    .collect()?;

                // Get the maximum block number from this file
                if let Some(max_block) = df
                    .column("block_number")?
                    .str()?
                    .into_iter()
                    .filter_map(|opt_val| opt_val.and_then(|val| val.parse::<u64>().ok()))
                    .max()
                {
                    latest_block = latest_block.max(max_block);
                }
            }
            Err(e) => println!("Error reading file: {}", e),
        }
    }

    Ok(latest_block)
}
