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


CREATE INDEX idx_logs_network_id ON logs (
    network_id
);

CREATE INDEX idx_logs_block_number ON logs (
    block_number
);

CREATE INDEX idx_logs_tx_hash ON logs (
    tx_hash
);

CREATE INDEX idx_logs_contract_address ON logs (
    contract_address
);

CREATE INDEX idx_logs_topic0 ON logs (
    topic0
);

