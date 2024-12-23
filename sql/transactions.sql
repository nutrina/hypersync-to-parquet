CREATE TABLE
    transactions (
        id SERIAL PRIMARY KEY,
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

CREATE INDEX idx_transactions_composite ON transactions (
    network_id,
    status,
    from_address,
    to_address,
    gas,
    gas_price,
    gas_used,
    cumulative_gas_used,
    effective_gas_price,
    block_number,
    tx_hash,
    tx_index,
    l1_fee,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    gas_used_for_l1
);


CREATE INDEX idx_transactions_from_address ON transactions (
    from_address
);

CREATE INDEX idx_transactions_to_address ON transactions (
    to_address
);

DROP INDEX idx_transactions_tx_hash;
CREATE INDEX idx_transactions_tx_hash ON transactions (
    tx_hash
);

CREATE INDEX idx_transactions_block_number ON transactions (
    block_number
);

CREATE INDEX idx_transactions_status ON transactions (
    status
);
