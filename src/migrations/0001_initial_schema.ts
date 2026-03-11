import type { ClickHouseClient } from "@clickhouse/client";

export async function up(client: ClickHouseClient): Promise<void> {
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS logs
      (
        chain_id          UInt32,
        block_number      UInt64                   CODEC(Delta, ZSTD(3)),
        block_hash        FixedString(32)          CODEC(ZSTD(6)),
        timestamp         UInt32                   CODEC(DoubleDelta, ZSTD(3)),
        transaction_hash  FixedString(32)          CODEC(ZSTD(6)),
        transaction_index UInt32                   CODEC(Delta, ZSTD(3)),
        log_index         UInt32                   CODEC(Delta, ZSTD(3)),
        address           FixedString(20)          CODEC(ZSTD(6)),
        data              String                   CODEC(ZSTD(9)),
        topic0            FixedString(32)          CODEC(ZSTD(6)),
        topic1            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic2            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic3            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        removed           UInt8
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (chain_id, topic0, block_number, log_index, address)
      SETTINGS index_granularity = 8192
    `,
  });

  //    Add bloom filter skip index on address.
  //    Compensates for address not being a leading sort column.
  //    Storage overhead: negligible (< 10 MB at production scale).
  await client.command({
    query: `
      ALTER TABLE logs
      ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 1
    `,
  });
}
