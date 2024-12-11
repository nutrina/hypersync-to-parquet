use bigdecimal::BigDecimal;
use bytes::Bytes;
use futures::SinkExt;
use std::env;
use tokio_postgres::{config::Config, Client, NoTls, Transaction};

pub struct TransactionWriter {
    db_user: String,
    db_password: String,
    db_host: String,
    db_name: String,
    client: Option<Client>,
    copy_stmt: String,
}

pub struct LogRecord {
    pub network_id: String,
    pub block_number: String,
    pub tx_hash: String,
    pub tx_index: String,
    pub contract_address: String,
    pub data: String,
    pub topic0: String,
    pub topic1: String,
    pub topic2: String,
    pub topic3: String,
}

pub struct TransactionRecord {
    pub network_id: String,
    pub status: String,
    pub from: String,
    pub to: String,
    pub gas: String,
    pub gas_price: String,
    pub gas_used: String,
    pub cumulative_gas_used: String,
    pub effective_gas_price: String,
    pub input: String,
    pub block_number: String,
    pub hash: String,
    pub transaction_index: String,
    pub l1_fee: String,
    pub l1_gas_price: String,
    pub l1_gas_used: String,
    pub l1_fee_scalar: String,
    pub gas_used_for_l1: String,
}

async fn write_transactions(
    client: &Client,
    transaction_records: &[TransactionRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    // Stream the data to the database

    println!(
        "write_logs, num transaction_records {}",
        transaction_records.len()
    );

    // Use COPY IN for bulk insertion
    let copy_stmt = String::from("COPY transactions (network_id, status, from_address, to_address, gas, gas_price, gas_used, cumulative_gas_used, effective_gas_price, input, block_number, tx_hash, tx_index, l1_fee, l1_gas_price, l1_gas_used, l1_fee_scalar, gas_used_for_l1) FROM STDIN ");

    let sink = client.copy_in(&copy_stmt).await?;
    let mut sink = Box::pin(sink); // Pin the sink
                                   // let mut writer = sink.into_writer();
                                   // let sink = client.copy_in(self.copy_stmt.as_str()).await?;
                                   // let writer = CopyInWriter::new(sink);
                                   // pin_mut!(writer);

    for t in transaction_records {
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            t.network_id,
            t.status,
            t.from,
            t.to,
            t.gas,
            t.gas_price,
            t.gas_used,
            t.cumulative_gas_used,
            t.effective_gas_price,
            t.input,
            t.block_number,
            t.hash,
            t.transaction_index,
            t.l1_fee,
            t.l1_gas_price,
            t.l1_gas_used,
            t.l1_fee_scalar,
            t.gas_used_for_l1,
        );
        let buf = Bytes::from(line);
        sink.send(buf).await?;

        // writer.as_mut().write(&row).await?;
    }

    sink.flush().await?;
    sink.close().await?;

    Ok(())
}

async fn write_logs(
    client: &Client,
    log_records: &[LogRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    // Stream the data to the database

    println!("write_logs, num log_records {}", log_records.len());

    let copy_stmt = String::from("COPY logs (network_id, block_number, tx_hash, tx_index, log_data, contract_address, topic0, topic1, topic2, topic3) FROM STDIN ");
    let sink = client.copy_in(&copy_stmt).await?;
    let mut sink = Box::pin(sink); // Pin the sink

    for log_record in log_records {
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            &log_record.network_id,
            &log_record.block_number,
            &log_record.tx_hash,
            &log_record.tx_index,
            &log_record.data,
            &log_record.contract_address,
            &log_record.topic0,
            &log_record.topic1,
            &log_record.topic2,
            &log_record.topic3,
        );
        let buf = Bytes::from(line);
        sink.send(buf).await?;
    }

    sink.flush().await?;
    sink.close().await?;

    Ok(())
}

impl TransactionWriter {
    pub async fn new() -> Self {
        let db_user = env::var("DB_USER").unwrap();
        let db_password = env::var("DB_PASSWORD").unwrap();
        let db_host = env::var("DB_HOST").unwrap();
        let db_name = env::var("DB_NAME").unwrap();

        println!("DB user: {}", db_user);
        println!("DB host: {}", db_host);
        println!("DB name: {}", db_name);
        println!("DB pwd : {}", db_password);

        Self {
            db_host,
            db_name,
            db_password,
            db_user,
            copy_stmt: String::from("COPY transactions (network_id, status, from_address, to_address, gas, gas_price, gas_used, cumulative_gas_used, effective_gas_price, input, block_number, tx_hash, tx_index, l1_fee, l1_gas_price, l1_gas_used, l1_fee_scalar, gas_used_for_l1) FROM STDIN "),
            client: None,
        }
    }

    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (client, connection) = Config::new()
            .user(&self.db_user)
            .password(&self.db_password)
            .host(&self.db_host)
            .dbname(&self.db_name)
            .connect(NoTls)
            .await?;

        self.client = Some(client);

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(())
    }

    pub async fn write(
        &mut self,
        block_number: u64,
        transaction_records: &[TransactionRecord],
        log_records: &[LogRecord],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Start a new transaction
        let _block_number = block_number as i64; // TODO: use BigDecimal here?

        match &mut self.client {
            Some(client) => {
                let row = client
                    .query_one(
                        "INSERT INTO blocks (block_number) VALUES ($1) RETURNING id",
                        &[&_block_number],
                    )
                    .await?;

                let id: i32 = row.get(0);

                write_transactions(client, transaction_records).await?;
                write_logs(client, log_records).await?;

                client
                    .execute("UPDATE blocks SET status = '1' WHERE id = $1 ", &[&id])
                    .await?;
            }
            None => {
                println!("!!! No DB client !!!");
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "!!! No DB client !!!",
                )));
            }
        }
        Ok(())
    }

    pub async fn get_latest_block_number(&mut self) -> Result<i64, Box<dyn std::error::Error>> {
        // Query the latest block_number in transactions
        let max_block_number: i64 = match &self.client {
            Some(client) => {
                let row = client
                    .query_one("SELECT MAX(block_number::BIGINT) FROM transactions", &[])
                    .await?;

                let max_block_transactions: i64 = row.try_get(0).unwrap_or(-1);

                let row = client
                    .query_one("SELECT MAX(block_number::BIGINT) FROM logs", &[])
                    .await?;

                let max_block_logs: i64 = row.try_get(0).unwrap_or(-1);

                std::cmp::max(max_block_transactions, max_block_logs)
            }
            None => {
                println!("!!! No DB client !!!");
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "!!! No DB client !!!",
                )));
            }
        };

        Ok(max_block_number)
    }
}
