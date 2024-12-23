CREATE TABLE
    transfers (
        id BIGSERIAL PRIMARY KEY,
        log_id BIGSERIAL NOT NULL,
        transfer_type BIGINT NOT NULL, -- 1 - ERC20
        network_id BIGINT NOT NULL,
        block_number NUMERIC(78, 0) NOT NULL,
        tx_hash VARCHAR(66) NOT NULL,
        tx_index NUMERIC(78, 0) NOT NULL,
        contract_address VARCHAR(42) NOT NULL,
        from_address VARCHAR(42) NOT NULL,
        to_address VARCHAR(42) NOT NULL,
        amount NUMERIC(78, 0) NOT NULL
    );

CREATE INDEX idx_transfers_network_id ON transfers (network_id);

CREATE INDEX idx_transfers_block_number ON transfers (block_number);

CREATE INDEX idx_transfers_tx_hash ON transfers (tx_hash);

CREATE INDEX idx_transfers_contract_address ON transfers (contract_address);

CREATE INDEX idx_transfers_from_address ON transfers (from_address);

CREATE INDEX idx_transfers_to_address ON transfers (to_address);