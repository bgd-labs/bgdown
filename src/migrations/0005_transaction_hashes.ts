import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

export async function up(client: ClickHouseClient): Promise<void> {
  // 1. Create the lookup table.
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${env.CLICKHOUSE_DB}.transaction_hashes
      (
        chain_id         UInt32,
        transaction_id   UInt64           CODEC(Delta, ZSTD(3)),
        transaction_hash FixedString(32)  CODEC(ZSTD(6))
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (chain_id, transaction_id)
      SETTINGS index_granularity = 8192
    `,
  });

  // 2. Backfill from existing logs.
  //    On an empty DB this is a no-op. On production (6.4B rows) expect ~30-60 min.
  //    transaction_id = block_number * 100000 + transaction_index is unique per tx
  //    and quasi-monotonic in the logs sort order, making Delta codec effective.
  await client.command({
    query: `
      INSERT INTO ${env.CLICKHOUSE_DB}.transaction_hashes
      SELECT DISTINCT
        chain_id,
        toUInt64(block_number) * 100000 + transaction_index AS transaction_id,
        transaction_hash
      FROM ${env.CLICKHOUSE_DB}.logs
    `,
  });

  // 3. Add transaction_id to logs AFTER timestamp (where transaction_hash used to
  //    live) so the column order matches the RowBinary serializer. The DEFAULT
  //    expression lets old parts compute the value on the fly without a rewrite.
  await client.command({
    query: `
      ALTER TABLE ${env.CLICKHOUSE_DB}.logs
        ADD COLUMN IF NOT EXISTS transaction_id UInt64
          DEFAULT toUInt64(block_number) * 100000 + transaction_index
          CODEC(Delta, ZSTD(3))
          AFTER timestamp
    `,
  });

  // 4. Drop transaction_hash from logs — it now lives in transaction_hashes.
  await client.command({
    query: `ALTER TABLE ${env.CLICKHOUSE_DB}.logs DROP COLUMN IF EXISTS transaction_hash`,
  });

  // 5. Drop block_hash from logs — it's already stored in the blocks table
  //    (chain_id, number, hash) and can be recovered via JOIN on block_number.
  //    At production scale this saves ~77 GiB compressed.
  await client.command({
    query: `ALTER TABLE ${env.CLICKHOUSE_DB}.logs DROP COLUMN IF EXISTS block_hash`,
  });
}
