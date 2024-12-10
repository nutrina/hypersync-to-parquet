use bytes::Bytes;
use futures::SinkExt;
use std::env;
use tokio_postgres::types::{ToSql, Type};
// use postgres::CopyInWriter;
use tokio_postgres::{config::Config, Client, NoTls};

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
            copy_stmt: String::from("COPY transactions (block_number, tx_hash, contract_address, from_address, to_address, amount, gas_used, tx_block_number, log_data, network_id) FROM STDIN "),
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
        block_numbers: &[String],
        tx_hashes: &[String],
        contract_addresses: &[String],
        from_addresses: &[String],
        to_addresses: &[String],
        amounts: &[String],
        gas_used: &[String],
        tx_block_numbers: &[String],
        log_data: &[String],
        network: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Stream the data to the database

        println!("block_numbers {}", block_numbers.len());
        println!("tx_hashes {}", tx_hashes.len());
        println!("contract_addresses {}", contract_addresses.len());
        println!("from_addresses {}", from_addresses.len());
        println!("to_addresses {}", to_addresses.len());
        println!("amounts {}", amounts.len());
        println!("gas_used {}", gas_used.len());
        println!("tx_block_numbers {}", tx_block_numbers.len());
        println!("log_data {}", log_data.len());
        println!("network {}", network.len());

        match &self.client {
            Some(client) => {
                // Use COPY IN for bulk insertion
                let sink = client.copy_in(&self.copy_stmt).await?;
                let mut sink = Box::pin(sink); // Pin the sink
                                               // let mut writer = sink.into_writer();
                                               // let sink = client.copy_in(self.copy_stmt.as_str()).await?;
                                               // let writer = CopyInWriter::new(sink);
                                               // pin_mut!(writer);

                for i in 0..block_numbers.len() {
                    let line = format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        &block_numbers[i],
                        &tx_hashes[i],
                        &contract_addresses[i],
                        &from_addresses[i],
                        &to_addresses[i],
                        &amounts[i],
                        &gas_used[i],
                        &tx_block_numbers[i],
                        &log_data[i],
                        &network[i]
                    );
                    let buf = Bytes::from(line);
                    sink.send(buf).await?;

                    // writer.as_mut().write(&row).await?;
                }

                sink.flush().await?;
                sink.close().await?;
            }
            None => println!("No DB client!"),
        }
        Ok(())
    }

    pub async fn write_logs(
        &mut self,
        log_records: &[LogRecord],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Stream the data to the database

        println!("write_logs, num log_records {}", log_records.len());
        match &self.client {
            Some(client) => {
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
            }
            None => println!("!!! No DB client !!!"),
        }
        println!("write_logs, DONE");
        Ok(())
    }
}
