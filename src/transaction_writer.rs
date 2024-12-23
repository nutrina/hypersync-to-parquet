use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::SinkExt;
use hypersync_client::simple_types::Block;
use rustls::ClientConfig as RustlsClientConfig;
use std::env;
use std::{fs::File, io::BufReader};
use tokio_postgres::{config::Config, Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct TransactionWriter {
    // db_user: String,
    // db_password: String,
    // db_host: String,
    // db_port: u16,
    // db_name: String,
    // client: Option<Client>,
    pool: Pool,
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

#[derive(Debug)]
pub struct BlockRange {
    pub from_block: i64,
    pub to_block: i64,
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
        let db_port = env::var("DB_PORT").unwrap().parse::<u16>().unwrap();
        let ca_cert = env::var("CERT_FILE").unwrap();

        println!("DB user: {}", db_user);
        println!("DB host: {}", db_host);
        println!("DB name: {}", db_name);
        println!("CERT   : {}", ca_cert);

        let mut pg_config = tokio_postgres::Config::new();

        pg_config
            .user(&db_user)
            .password(&db_password)
            .dbname(&db_name)
            .host(&db_host)
            .port(db_port);

        let mgr: Option<Manager>;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        if ca_cert != "" {
            println!("Using TLS");

            let cert_file = File::open(ca_cert).unwrap();

            let mut buf = BufReader::new(cert_file);
            let mut root_store: rustls::RootCertStore = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut buf) {
                root_store.add(cert.unwrap()).unwrap();
            }

            let tls_config = RustlsClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let tls = MakeRustlsConnect::new(tls_config);

            mgr = Some(Manager::from_config(pg_config, tls, mgr_config));
            println!("Done initializing using Tls");
        } else {
            println!("Using NoTls");
            mgr = Some(Manager::from_config(pg_config, NoTls, mgr_config));
        }

        let pool = Pool::builder(mgr.unwrap()).max_size(16).build().unwrap();

        Self { pool }
        // Self {
        //     db_host,
        //     db_port,
        //     db_name,
        //     db_password,
        //     db_user,
        //     client: None,
        // }
    }

    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // let (client, connection) = Config::new()
        //     .user(&self.db_user)
        //     .password(&self.db_password)
        //     .host(&self.db_host)
        //     .dbname(&self.db_name)
        //     .connect(NoTls)
        //     .await?;

        // self.client = Some(client);

        // Spawn the connection handler
        // tokio::spawn(async move {
        //     if let Err(e) = connection.await {
        //         eprintln!("connection error: {}", e);
        //     }
        // });

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
        let client = self.pool.get().await.unwrap();

        let row = client
            .query_one(
                "INSERT INTO blocks (block_number) VALUES ($1) RETURNING id",
                &[&_block_number],
            )
            .await?;

        let id: i32 = row.get(0);

        write_transactions(&client, transaction_records).await?;
        write_logs(&client, log_records).await?;

        client
            .execute("UPDATE blocks SET status = '1' WHERE id = $1 ", &[&id])
            .await?;

        Ok(())
    }

    pub async fn get_latest_block_number(&mut self) -> Result<i64, Box<dyn std::error::Error>> {
        // Query the latest block_number in transactions
        let client = self.pool.get().await.unwrap();

        // let row = client
        //     .query_one("SELECT MAX(block_number::BIGINT) FROM transactions", &[])
        //     // .query_one("SELECT block_number::BIGINT FROM transactions ORDER BY block_number DESC LIMIT 1", &[])
        //     .await?;

        // let max_block_transactions: i64 = row.try_get(0).unwrap_or(-1);

        // let row = client
        //     .query_one("SELECT MAX(block_number::BIGINT) FROM logs", &[])
        //     // .query_one("SELECT block_number::BIGINT FROM logs ORDER BY block_number DESC LIMIT 1", &[])
        //     .await?;

        // let max_block_logs: i64 = row.try_get(0).unwrap_or(-1);

        let row = client
            .query_one("SELECT MAX(block_number::BIGINT) FROM blocks limit 1", &[])
            .await?;

        let max_block: i64 = row.try_get(0).unwrap_or(-1);

        Ok(max_block)
    }

    pub async fn get_missing_block_ranges(
        &mut self,
    ) -> Result<Vec<BlockRange>, Box<dyn std::error::Error>> {
        let mut result = Vec::new(); // Create an empty vector
        let client = self.pool.get().await.unwrap();

        let rows = client
            .query(
                "WITH blanks AS
  (SELECT id,
          status,
          block_number,
          LAG(block_number) OVER (
                                  ORDER BY id) AS previous_block_number,
                                 LAG(id) OVER (
                                               ORDER BY id) AS previous_id
   FROM blocks)
SELECT id,
       previous_id,
       status,
       block_number,
       previous_block_number
FROM blanks
WHERE status IS NULL",
                &[],
            )
            .await?;

        for row in rows.iter() {
            let from_block: i64 = row.try_get(4).unwrap();
            let to_block: i64 = row.try_get(3).unwrap();
            let item = BlockRange {
                from_block: from_block + 1, // The range needs to be inclusive
                to_block: to_block,
            };
            result.push(item);
        }

        Ok(result)
    }

    pub async fn clear_block_range(
        &mut self,
        range: &BlockRange,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.pool.get().await.unwrap();
        println!("Deleting entries in logs");
        client
            .execute(
                "DELETE FROM logs WHERE block_number >= $1 AND block_number <= $2",
                &[&range.from_block, &range.to_block],
            )
            .await?;
        println!("Deleting entries in transactions");
        client
            .execute(
                "DELETE FROM transactions WHERE block_number >= $1 AND block_number <= $2",
                &[&range.from_block, &range.to_block],
            )
            .await?;

        Ok(())
    }
}
