CREATE TABLE
    transactions (
        id BIGSERIAL PRIMARY KEY,
        network_id BIGINT,
        status INT NOT NULL,
        from_address VARCHAR(42) NOT NULL,
        to_address VARCHAR(42) NOT NULL,
        gas NUMERIC(78, 0) NOT NULL,
        gas_price NUMERIC(78, 0) NOT NULL,
        gas_used NUMERIC(78, 0) NOT NULL,
        cumulative_gas_used NUMERIC(78, 0) NOT NULL,
        effective_gas_price NUMERIC(78, 0) NOT NULL,
        input TEXT NOT NULL,
        block_number NUMERIC(78, 0) NOT NULL,
        tx_hash VARCHAR(66) NOT NULL,
        tx_index NUMERIC(78, 0) NOT NULL,
        l1_fee NUMERIC(78, 0) NULL,
        l1_gas_price NUMERIC(78, 0) NULL,
        l1_gas_used NUMERIC(78, 0) NULL,
        l1_fee_scalar NUMERIC(78, 0) NULL,
        gas_used_for_l1 NUMERIC(78, 0) NULL
    );

CREATE INDEX idx_transactions_from_address ON transactions (from_address);

CREATE INDEX idx_transactions_to_address ON transactions (to_address);

CREATE INDEX idx_transactions_tx_hash ON transactions (tx_hash);

CREATE INDEX idx_transactions_block_number ON transactions (block_number);

CREATE INDEX idx_transactions_status ON transactions (status);