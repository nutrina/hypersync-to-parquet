CREATE TABLE
    logs (
        id SERIAL PRIMARY KEY,
        network_id BIGINT,
        block_number NUMERIC(78, 0) NOT NULL,
        tx_hash VARCHAR(66) NOT NULL,
        tx_index NUMERIC(78, 0) NOT NULL,
        log_data TEXT,
        contract_address VARCHAR(42),
        topic0 VARCHAR(66) NOT NULL,
        topic1 VARCHAR(66) NOT NULL,
        topic2 VARCHAR(66) NOT NULL,
        topic3 VARCHAR(66) NOT NULL
    );

CREATE INDEX idx_logs_composite ON logs (
    network_id,
    block_number,
    tx_hash,
    tx_index,
    contract_address,
    topic0,
    topic1,
    topic2,
    topic3
);