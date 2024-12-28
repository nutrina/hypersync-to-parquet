CREATE TABLE
    blocks (
        id BIGSERIAL PRIMARY KEY,
        block_number BIGINT NOT NULL,   -- TODO: use numeric here
        status INT NULL
    );

CREATE INDEX idx_blocks_composite ON blocks (block_number, status);

ALTER TABLE blocks
ADD CONSTRAINT unique_block_number UNIQUE (block_number);
