import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

const DB = env.CLICKHOUSE_DB;

export async function up(client: ClickHouseClient): Promise<void> {
  // Dictionary for block hash lookups: (chain_id, number) → hash.
  // Backed by the blocks table, loaded into memory as a flat_hash_map.
  // Used via dictGet() in log queries to avoid JOIN on the blocks table.
  await client.command({
    query: `
      CREATE DICTIONARY IF NOT EXISTS ${DB}.dict_block_hash
      (
        chain_id UInt32,
        number   UInt64,
        hash     FixedString(32)
      )
      PRIMARY KEY chain_id, number
      SOURCE(CLICKHOUSE(
        DATABASE '${DB}'
        TABLE 'blocks'
        QUERY 'SELECT chain_id, number, hash FROM ${DB}.blocks'
      ))
      LAYOUT(COMPLEX_KEY_HASHED())
      LIFETIME(0)
    `,
  });

  // Dictionary for transaction hash lookups: (chain_id, transaction_id) → transaction_hash.
  await client.command({
    query: `
      CREATE DICTIONARY IF NOT EXISTS ${DB}.dict_tx_hash
      (
        chain_id         UInt32,
        transaction_id   UInt64,
        transaction_hash FixedString(32)
      )
      PRIMARY KEY chain_id, transaction_id
      SOURCE(CLICKHOUSE(
        DATABASE '${DB}'
        TABLE 'transaction_hashes'
        QUERY 'SELECT chain_id, transaction_id, transaction_hash FROM ${DB}.transaction_hashes'
      ))
      LAYOUT(COMPLEX_KEY_HASHED())
      LIFETIME(0)
    `,
  });
}
