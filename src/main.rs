use ethers::{abi::AbiEncode, types::U64};
use glob::glob;
use hypersync_client::{
    format::{Address, Hex},
    Client, ClientConfig, Decoder, StreamConfig,
};
use polars::lazy::dsl::col;
use polars::prelude::*;
use std::env;
use std::{collections::HashMap, sync::Arc};
use url::Url;

mod transaction_writer;
use transaction_writer::{LogRecord, TransactionWriter}; // Import the `Person` struct

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init().unwrap();

    let mut db_writer = TransactionWriter::new().await;
    let network_id: i32 = 1;
    let network = "eth";

    db_writer.init().await?;

    let bearer_token = env::var("HYPERSYNC_BEARER_TOKEN").unwrap();

    let config = ClientConfig {
        url: Some(Url::parse(&format!("https://{network}.hypersync.xyz/")).unwrap()),
        bearer_token: Some(bearer_token),

        ..Default::default()
    };

    let client = Arc::new(Client::new(config).unwrap());

    // let from_block = get_latest_block_number(&network)?;

    let query = serde_json::from_value(serde_json::json!({
        "from_block": 0,
        "logs": [{
            "topics": [
                // ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
            ]
        }],
        "transactions": [
            // get all transactions
        ],
        "field_selection": {
            "log": [
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
                // The contract address from which the event originated.
                "address",
                // transaction hash
                "block_number",
                "transaction_hash",
                "transaction_index",
            ],
            "transaction": [
                // If transaction is executed successfully. This is the `statusCode`
                "status",
                "from",
                "to",
                "gas",
                "gas_price",
                "gas_used",
                // The total amount of gas used in the block until this transaction was executed.
                "cumulative_gas_used",
                // The sum of the base fee and tip paid per unit of gas.
                "effective_gas_price",
                "input",
                "block_number",
                // transaction hash
                "hash",
                "transaction_index",
                // The fee associated with a transaction on the Layer 1, it is calculated as l1GasPrice multiplied by l1GasUsed
                "l1_fee",
                // The gas price for transactions on the Layer 1
                "l1_gas_price",
                // The amount of gas consumed by a transaction on the Layer 1
                "l1_gas_used",
                // A multiplier applied to the actual gas usage on Layer 1 to calculate the dynamic costs.
                // If set to 1, it has no impact on the L1 gas usage
                "l1_fee_scalar",
                // Amount of gas spent on L1 calldata in units of L2 gas.
                "gas_used_for_l1",
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
    let mut log_records: Vec<LogRecord> = Vec::new();

    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        // Create a HashMap of transactions keyed by their hash
        // let mut tx_map: HashMap<String, (Address, Address, u64, String)> = HashMap::new();

        println!("Num logs: {}", res.data.logs.len());
        println!("Num transactions: {:?}", res.data.transactions.len());

        // Process logs and associate them with transactions
        for batch in res.data.logs {
            for log in batch {
                if let (
                    Ok(Some(decoded_log)),
                    Some(tx_hash),
                    Some(block_number),
                    Some(tx_index),
                    Some(contract_address),
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                ) = (
                    decoder.decode_log(&log),
                    log.transaction_hash,
                    log.block_number,
                    log.transaction_index,
                    log.address,
                    log.topics[0]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[1]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[2]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[3]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                ) {
                    log_records.push(LogRecord {
                        network_id: network_id.to_string(),
                        block_number: block_number.to_string(),
                        tx_hash: tx_hash.encode_hex(),
                        tx_index: tx_index.to_string(),
                        contract_address: contract_address.encode_hex(),
                        data: log.data.map_or_else(String::new, |d| d.encode_hex()),
                        topic0: topic0,
                        topic1: topic1,
                        topic2: topic2,
                        topic3: topic3,
                    });
                }
            }
        }
        println!(
            "Processed block: {}, Saving to {} logs",
            res.next_block,
            log_records.len()
        );
        db_writer.write_logs(&log_records).await?;
        log_records.clear();
        println!("===> DONE writing");

        // Process logs and associate them with transactions
        for batch in res.data.transactions {
            for log in batch {
                if let (
                    Ok(Some(decoded_log)),
                    Some(tx_hash),
                    Some(block_number),
                    Some(tx_index),
                    Some(contract_address),
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                ) = (
                    decoder.decode_log(&log),
                    log.transaction_hash,
                    log.block_number,
                    log.transaction_index,
                    log.address,
                    log.topics[0]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[1]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[2]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                    log.topics[3]
                        .as_ref()
                        .map_or_else(String::new, |v| v.encode_hex()),
                ) {
                    log_records.push(LogRecord {
                        network_id: network_id.to_string(),
                        block_number: block_number.to_string(),
                        tx_hash: tx_hash.encode_hex(),
                        tx_index: tx_index.to_string(),
                        contract_address: contract_address.encode_hex(),
                        data: log.data.map_or_else(String::new, |d| d.encode_hex()),
                        topic0: topic0,
                        topic1: topic1,
                        topic2: topic2,
                        topic3: topic3,
                    });
                }
            }
        }
        println!(
            "Processed block: {}, Saving to {} logs",
            res.next_block,
            log_records.len()
        );
        db_writer.write_logs(&log_records).await?;
        log_records.clear();
        println!("===> DONE writing");

        // println!(
        //     "Processed block: {}, Saving to Parquet? {}",
        //     res.next_block,
        //     &block_numbers.len()
        // );

        // // Save to parquet file every 5000 blocks
        // if block_numbers.len() >= 5 {
        //     db_writer
        //         .write(
        //             &block_numbers,
        //             &tx_hashes,
        //             &contract_addresses,
        //             &from_addresses,
        //             &to_addresses,
        //             &amounts_str,
        //             &gas_used,
        //             &tx_block_numbers,
        //             &log_data,
        //             &networks,
        //         )
        //         .await?;

        //     block_numbers.clear();
        //     tx_hashes.clear();
        //     contract_addresses.clear();
        //     from_addresses.clear();
        //     to_addresses.clear();
        //     amounts_str.clear();
        //     gas_used.clear();
        //     tx_block_numbers.clear();
        //     networks.clear();
        //     log_data.clear();
        //     topic0.clear();
        //     topic1.clear();
        //     topic2.clear();
        //     topic3.clear();
        // }
    }

    // Save any remaining data
    // if !block_numbers.is_empty() {
    //     save_to_parquet(
    //         &block_numbers,
    //         &tx_hashes,
    //         &contract_addresses,
    //         &from_addresses,
    //         &to_addresses,
    //         &amounts_str,
    //         &gas_used,
    //         &tx_block_numbers,
    //         &log_data,
    //         &network,
    //     )?;
    // }

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
    let filename = format!(
        "erc20_transfer_test_output_11_21/erc20_transfers_with_gas_from_block_{}_to_block_{}.parquet",
        block_numbers.first().unwrap(),
        block_numbers.last().unwrap()
    );

    // Save to parquet file with compression
    let mut file = std::fs::File::create(filename.clone())?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;

    println!("Saved {} records to {}", df.height(), filename);
    Ok(())
}

fn get_latest_block_number(network: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let mut latest_block = 0u64;

    // // Search for all parquet files matching the pattern
    // for entry in glob(&format!("erc20_transfers_{}*.parquet", network))? {
    //     match entry {
    //         Ok(path) => {
    //             // Read the parquet file
    //             let df = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())?
    //                 .select([col("block_number")])
    //                 .collect()?;

    //             // Get the maximum block number from this file
    //             if let Some(max_block) = df
    //                 .column("block_number")?
    //                 .str()?
    //                 .into_iter()
    //                 .filter_map(|opt_val| opt_val.and_then(|val| val.parse::<u64>().ok()))
    //                 .max()
    //             {
    //                 latest_block = latest_block.max(max_block);
    //             }
    //         }
    //         Err(e) => println!("Error reading file: {}", e),
    //     }
    // }

    Ok(latest_block)
}
