use ethers::abi::{AbiDecode, RawLog};
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

#[derive(Debug, EthEvent)]
#[ethevent(
    name = "TransferSingle", 
    abi = "TransferSingle(address,address,address,uint256,uint256)"
)]
pub struct TransferErc1155 {
    #[ethevent(indexed)]
    pub operator: Address,
    #[ethevent(indexed)]
    pub from: Address,
    #[ethevent(indexed)]
    pub to: Address,
    pub id: U256,
    pub value: U256,
}

pub fn decode_transfer_event_single_erc1155(
    log: Log,
) -> Result<TransferErc1155, Box<dyn std::error::Error>> {
    let raw_log = RawLog {
        topics: log.topics,
        data: log.data.to_vec(), // Extract the raw bytes
    };
    let event = TransferErc1155::decode_log(&raw_log)?;
    Ok(event)
}


#[derive(Debug, EthEvent)]
#[ethevent(
    name = "TransferBatch",
    abi = "TransferBatch(address,address,address,uint256[],uint256[])"
)]
pub struct TransferErc1155Batch {
    #[ethevent(indexed)]
    pub operator: Address,
    #[ethevent(indexed)]
    pub from: Address,
    #[ethevent(indexed)]
    pub to: Address,
    pub ids: Vec<U256>,
    pub values: Vec<U256>,
}
pub fn decode_transfer_event_batch_erc1155(
    log: Log,
) -> Result<TransferErc1155Batch, Box<dyn std::error::Error>> {
    let raw_log = RawLog {
        topics: log.topics,
        data: log.data.to_vec(), // Extract the raw bytes
    };
    let event = TransferErc1155Batch::decode_log(&raw_log)?;
    Ok(event)
}