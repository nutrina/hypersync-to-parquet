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
mod transfers;
use ethers::types::{Filter, Log, TxHash};
use rustls::crypto::ring as provider;

use hex;
use transaction_writer::{LogRecord, TransactionRecord, TransactionWriter, TransferRecord}; // Import the `Person` struct
use transfers::{decode_transfer_event, Transfer}; // Import the `Person` struct

/*
from_block - inclusive
to_block - not inclusive
 */
async fn sync_block_chain(
    db_writer: &mut TransactionWriter,
    from_block: i64,
    to_block: i64,
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

    let query = serde_json::from_value(serde_json::json!({
        "from_block": from_block,
        "to_block": to_block,
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
                        id: String::from(""),
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

async fn sync_all_blockchain_blocks(
    db_writer: &mut TransactionWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let from_block = db_writer.get_latest_block_number().await? + 1;

    let block_number = db_writer.get_latest_block_number().await?;
    println!("Latest block number: {}", block_number);

    sync_block_chain(db_writer, from_block, 0).await?;
    Ok(())
}

async fn sync_missing_block_ranges(
    db_writer: &mut TransactionWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let missing_ranges = db_writer.get_missing_block_ranges().await.unwrap();
    for range in missing_ranges.iter() {
        println!(
            "Missing range: {:?} -> {:?}",
            range.from_block, range.to_block
        );
        db_writer.clear_block_range(range).await.unwrap();
        sync_block_chain(db_writer, range.from_block, range.to_block + 1).await?;
    }
    Ok(())
}

async fn create_erc20_transfer_records(
    db_writer: &mut TransactionWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut next_log_id: i32 = 0;
    let mut transfer_records: Vec<TransferRecord> = Vec::new();

    while let Some(erc20_logs) = db_writer.get_erc20_log_records(next_log_id).await.unwrap() {
        next_log_id = erc20_logs.last().unwrap().id.parse::<i32>()? + 1;

        for erc20_log in erc20_logs.iter() {
            let data = if erc20_log.data.len() >= 2 {
                erc20_log.data[2..].to_string()
            } else {
                String::new()
            };

            let mut topics: Vec<TxHash> = Vec::new();

            if erc20_log.topic0.len() > 0 {
                topics.push(erc20_log.topic0.parse()?);
            }
            if erc20_log.topic1.len() > 0 {
                topics.push(erc20_log.topic1.parse()?);
            }
            if erc20_log.topic2.len() > 0 {
                topics.push(erc20_log.topic2.parse()?);
            }
            if erc20_log.topic3.len() > 0 {
                topics.push(erc20_log.topic3.parse()?);
            }

            let eth_event_log = Log {
                address: erc20_log.contract_address.parse().unwrap(),
                topics: topics,
                data: hex::decode(data).unwrap().into(),
                ..Default::default()
            };

            // Decode the log
            match decode_transfer_event(eth_event_log) {
                Ok(event) => {
                    transfer_records.push(TransferRecord {
                        id: String::from(""),
                        log_id: erc20_log.id.clone(),
                        network_id: erc20_log.network_id.clone(),
                        block_number: erc20_log.block_number.clone(),
                        transfer_type: String::from("1"),
                        tx_hash: erc20_log.tx_hash.clone(),
                        tx_index: erc20_log.tx_index.clone(),
                        contract_address: erc20_log.contract_address.clone(),
                        from_address: String::from("0x") + hex::encode(event.from.as_bytes()).as_str(),
                        to_address: String::from("0x") + hex::encode(event.to.as_bytes()).as_str(),
                        amount: event.value.to_string(),
                    });
                }
                Err(e) => eprintln!("Failed to decode event: {}", e),
            }
        }

        db_writer.write_transfers(&transfer_records).await?;
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

    // // ---- Sync everything ....
    // sync_all_blockchain_blocks(&mut db_writer).await?;
    // // ---- End syncing everything

    // // ---- Sync only the missing blocks
    // sync_missing_block_ranges(&mut db_writer).await?;
    // // ---- End Sync only the missing blocks

    // ---- Sync only the missing blocks
    create_erc20_transfer_records(&mut db_writer).await?;
    // ---- End Sync only the missing blocks

    // hash=0x2adf58cb7c7ee6177705a4f99fbe8c51f5b57d5fc4d434755a6afe79a5a43553
    // let log = Log {
    //     address: "0xd3804bdd39fce95ab4fd5449bc40b94dbd1a303f".parse().unwrap(),
    //     topics: vec![
    //         "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".parse()?,
    //         "0x0000000000000000000000004859357af7b96393768073923b21e7686771123e".parse()?,
    //         "0x00000000000000000000000011bc81a1e929078c0c7c37d4fab8d85506dce24d".parse()?,
    //     ],
    //     data: hex::decode("0000000000000000000000000000000000000000000000000000000008e18f40").unwrap().into(),
    //     ..Default::default()
    // };

    // // Decode the log
    // match decode_transfer_event(log) {
    //     Ok(event) => println!("Decoded Transfer Event: {:?}", event),
    //     Err(e) => eprintln!("Failed to decode event: {}", e),
    // }
    Ok(())
}
