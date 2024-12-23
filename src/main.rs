use ethers::{abi::AbiEncode, types::U64};
use hypersync_client::{
    format::{Address, Hex},
    Client, ClientConfig, Decoder, StreamConfig,
};
use rustls::crypto::CryptoProvider;
use std::env;
use std::{collections::HashMap, sync::Arc};
use url::Url;
mod transaction_writer;
use rustls::crypto::ring as provider;
use transaction_writer::{LogRecord, TransactionRecord, TransactionWriter}; // Import the `Person` struct

async fn sync_block_chain(
    db_writer: &mut TransactionWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let network_id: i32 = 1;
    let network = "eth";

    let bearer_token = env::var("HYPERSYNC_BEARER_TOKEN").unwrap();

    let config = ClientConfig {
        url: Some(Url::parse(&format!("https://{network}.hypersync.xyz/")).unwrap()),
        bearer_token: Some(bearer_token),

        ..Default::default()
    };

    let client = Arc::new(Client::new(config).unwrap());

    let from_block = db_writer.get_latest_block_number().await? + 1;

    let query = serde_json::from_value(serde_json::json!({
        "from_block": from_block,
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
    let mut transaction_records: Vec<TransactionRecord> = Vec::new();

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
        fn csv_null() -> String {
            String::from("\\N")
        }

        // Process logs and associate them with transactions
        for batch in res.data.transactions {
            for t in batch {
                let status = t.status.unwrap().to_u8().to_string();
                let from = t.from.unwrap().encode_hex();
                let to = t.to.map_or_else(String::new, |f| f.encode_hex());
                let gas = t
                    .gas
                    .map_or_else(String::new, |f| U64::from_big_endian(&f).to_string());
                let gas_price = t
                    .gas_price
                    .map_or_else(String::new, |f| U64::from_big_endian(&f).to_string());
                let gas_used = t
                    .gas_used
                    .map_or_else(String::new, |f| U64::from_big_endian(&f).to_string());
                let cumulative_gas_used = t
                    .cumulative_gas_used
                    .map_or_else(String::new, |f| U64::from_big_endian(&f).to_string());
                let effective_gas_price = t
                    .effective_gas_price
                    .map_or_else(String::new, |f| U64::from_big_endian(&f).to_string());
                let input = t.input.map_or_else(String::new, |f| f.encode_hex());
                let block_number = t.block_number.map_or_else(String::new, |f| f.to_string());
                let hash = t.hash.map_or_else(String::new, |f| f.encode_hex());
                let transaction_index = t
                    .transaction_index
                    .map_or_else(String::new, |f| f.to_string());
                let l1_fee = t
                    .l1_fee
                    .map_or_else(csv_null, |f| U64::from_big_endian(&f).to_string());
                let l1_gas_price = t
                    .l1_gas_price
                    .map_or_else(csv_null, |f| U64::from_big_endian(&f).to_string());
                let l1_gas_used = t
                    .l1_gas_used
                    .map_or_else(csv_null, |f| U64::from_big_endian(&f).to_string());
                let l1_fee_scalar = t.l1_fee_scalar.map_or_else(csv_null, |f| f.to_string());
                let gas_used_for_l1 = t
                    .gas_used_for_l1
                    .map_or_else(csv_null, |f| U64::from_big_endian(&f).to_string());

                transaction_records.push(TransactionRecord {
                    network_id: network_id.to_string(),
                    status: status,
                    from: from,
                    to: to,
                    gas: gas,
                    gas_price: gas_price,
                    gas_used: gas_used,
                    cumulative_gas_used: cumulative_gas_used,
                    effective_gas_price: effective_gas_price,
                    input: input,
                    block_number: block_number,
                    hash: hash,
                    transaction_index: transaction_index,
                    l1_fee: l1_fee,
                    l1_gas_price: l1_gas_price,
                    l1_gas_used: l1_gas_used,
                    l1_fee_scalar: l1_fee_scalar,
                    gas_used_for_l1: gas_used_for_l1,
                });
            }
        }
        println!(
            "Processed block: {}, saving {} transaction_records",
            res.next_block,
            log_records.len()
        );
        println!(
            "Processed block: {}, saving {} logs",
            res.next_block,
            log_records.len()
        );

        db_writer
            .write(res.next_block - 1, &transaction_records, &log_records)
            .await?;
        log_records.clear();
        transaction_records.clear();
        println!("===> DONE writing logs & transactions");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the default cryptographic provider
    provider::default_provider().install_default().unwrap();

    env_logger::init().unwrap();

    let mut db_writer = TransactionWriter::new().await;

    db_writer.init().await?;
    let block_number = db_writer.get_latest_block_number().await?;
    println!("Latest block number: {}", block_number);

    // sync_block_chain().await
    let missing_ranges = db_writer.get_missing_block_ranges().await.unwrap();

    for range in missing_ranges.iter() {
        println!(
            "Missing range: {:?} -> {:?}",
            range.from_block, range.to_block
        );
        db_writer.clear_block_range(range);
    }

    Ok(())
}
