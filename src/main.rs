use ethers::types::U64;
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

    let from_block = get_latest_block_number(&network)?;

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
    let mut amounts_str: Vec<String> = Vec::new();
    let mut gas_used: Vec<u64> = Vec::new();
    let mut tx_block_numbers: Vec<String> = Vec::new();
    let mut log_data: Vec<String> = Vec::new();
    let mut topic0: Vec<String> = Vec::new();
    let mut topic1: Vec<String> = Vec::new();
    let mut topic2: Vec<String> = Vec::new();
    let mut topic3: Vec<String> = Vec::new();

    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        // Create a HashMap of transactions keyed by their hash
        let mut tx_map: HashMap<String, (Address, Address, u64, String)> = HashMap::new();

        // Process transactions first
        for batch in res.data.transactions {
            for tx in batch {
                if let (Some(from), Some(to), Some(hash), Some(gas), Some(block_number)) =
                    (tx.from, tx.to, tx.hash, tx.gas_used, tx.block_number)
                {
                    let gas = U64::from_big_endian(&gas);
                    tx_map.insert(
                        hash.encode_hex(),
                        (from, to, gas.as_u64(), block_number.to_string()),
                    );
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
                    if let Some((tx_from, tx_to, gas, tx_block)) = tx_map.get(&tx_hash.encode_hex())
                    {
                        // Store data in vectors
                        block_numbers.push(block_number.to_string());
                        tx_hashes.push(tx_hash.encode_hex());
                        contract_addresses.push(contract_address.encode_hex());
                        from_addresses.push(from.to_string());
                        to_addresses.push(to.to_string());
                        amounts_str.push(amount.0.to_string());
                        gas_used.push(*gas);
                        tx_block_numbers.push(tx_block.clone());
                        log_data.push(log.data.map_or_else(String::new, |d| d.encode_hex()));
                    }
                }
            }
        }

        println!(
            "Processed block: {}, Saving to Parquet? {}",
            res.next_block,
            &block_numbers.len()
        );

        // Save to parquet file every 5000 blocks
        if block_numbers.len() >= 5000 {
            save_to_parquet(
                &block_numbers,
                &tx_hashes,
                &contract_addresses,
                &from_addresses,
                &to_addresses,
                &amounts_str,
                &gas_used,
                &tx_block_numbers,
                &log_data,
                &network,
            )?;

            block_numbers.clear();
            tx_hashes.clear();
            contract_addresses.clear();
            from_addresses.clear();
            to_addresses.clear();
            amounts_str.clear();
            gas_used.clear();
            tx_block_numbers.clear();
            log_data.clear();
            topic0.clear();
            topic1.clear();
            topic2.clear();
            topic3.clear();
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
            &gas_used,
            &tx_block_numbers,
            &log_data,
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
    gas_used: &[u64],
    tx_block_numbers: &[String],
    log_data: &[String],
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
        Series::new("gas_used".into(), gas_used.to_vec()).into(),
        Series::new("tx_block_number".into(), tx_block_numbers.to_vec()).into(),
        Series::new("log_data".into(), log_data.to_vec()).into(),
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

fn get_latest_block_number(network: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let mut latest_block = 0u64;

    // Search for all parquet files matching the pattern
    for entry in glob(&format!("erc20_transfers_{}*.parquet", network))? {
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
