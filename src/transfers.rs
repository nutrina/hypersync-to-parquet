use ethers::abi::{RawLog, AbiDecode};
use ethers::contract::EthEvent;
use ethers::types::{Address, U256};
use ethers::types::{Filter, Log};

#[derive(Debug, EthEvent)]
pub struct Transfer {
    #[ethevent(indexed)]
    pub from: Address,
    #[ethevent(indexed)]
    pub to: Address,
    pub value: U256,
}

pub fn decode_transfer_event(log: Log) -> Result<Transfer, Box<dyn std::error::Error>> {
    let raw_log = RawLog {
        topics: log.topics,
        data: log.data.to_vec(), // Extract the raw bytes
    };
    let event = Transfer::decode_log(&raw_log)?;
    Ok(event)
}