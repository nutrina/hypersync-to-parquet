CREATE TABLE
    blocks (
        id SERIAL PRIMARY KEY,
        block_number BIGINT NOT NULL,   -- TODO: use numeric here
        status INT NULL
    );

CREATE INDEX idx_blocks_composite ON blocks (block_number, status);