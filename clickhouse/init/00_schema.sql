CREATE DATABASE IF NOT EXISTS ethereum;

CREATE TABLE IF NOT EXISTS ethereum.logs
(
    -- UInt32 is sufficient for any chain ID (max ~4B).
    chain_id          UInt32,
    -- Delta codec turns sequential block numbers into tiny deltas before ZSTD.
    block_number      UInt64               CODEC(Delta, ZSTD(3)),
    -- Store hashes/topics/address as raw bytes (32/20 bytes) instead of hex strings
    -- (66/42 chars). Halves uncompressed size; ZSTD then compresses the rest.
    block_hash        FixedString(32)      CODEC(ZSTD(6)),
    -- DoubleDelta is ideal for monotonically increasing timestamps.
    timestamp         UInt32               CODEC(DoubleDelta, ZSTD(3)),
    transaction_hash  FixedString(32)      CODEC(ZSTD(6)),
    -- Delta on these small, quasi-sorted integers compresses them to near-zero.
    transaction_index UInt32               CODEC(Delta, ZSTD(3)),
    log_index         UInt32               CODEC(Delta, ZSTD(3)),
    address           FixedString(20)      CODEC(ZSTD(6)),
    -- ABI-encoded payload stored as raw bytes; high ZSTD level exploits repetitive
    -- zero-padding in ABI encoding.
    data              String               CODEC(ZSTD(9)),
    -- topic0 is the event signature hash, repeated for every matching log.
    topic0            FixedString(32)      CODEC(ZSTD(6)),
    topic1            Nullable(FixedString(32)) CODEC(ZSTD(6)),
    topic2            Nullable(FixedString(32)) CODEC(ZSTD(6)),
    topic3            Nullable(FixedString(32)) CODEC(ZSTD(6)),
    removed           UInt8
) ENGINE = ReplacingMergeTree()
-- chain_id first so all scans are partitioned by chain.
-- topic0 second so WHERE chain_id = x AND topic0 = <bytes> hits the primary index directly.
-- address third so adding it to the filter narrows to a single contract without a scan.
-- block_number + log_index last to keep deduplication correct and allow range scans within an event type.
ORDER BY (chain_id, topic0, address, block_number, log_index)
SETTINGS index_granularity = 8192;
