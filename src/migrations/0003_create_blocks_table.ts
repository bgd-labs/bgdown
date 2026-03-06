import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

export async function up(client: ClickHouseClient): Promise<void> {
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${env.CLICKHOUSE_DB}.blocks
      (
        chain_id                  UInt32,
        number                    UInt64                       CODEC(Delta, ZSTD(3)),
        hash                      FixedString(32)              CODEC(ZSTD(6)),
        parent_hash               FixedString(32)              CODEC(ZSTD(6)),
        nonce                     UInt64                       CODEC(ZSTD(3)),
        sha3_uncles               FixedString(32)              CODEC(ZSTD(6)),
        logs_bloom                FixedString(256)             CODEC(ZSTD(9)),
        transactions_root         FixedString(32)              CODEC(ZSTD(6)),
        state_root                FixedString(32)              CODEC(ZSTD(6)),
        receipts_root             FixedString(32)              CODEC(ZSTD(6)),
        miner                     FixedString(20)              CODEC(ZSTD(6)),
        difficulty                UInt64                       CODEC(ZSTD(3)),
        total_difficulty          String                       CODEC(ZSTD(9)),
        extra_data                String                       CODEC(ZSTD(9)),
        size                      UInt64                       CODEC(ZSTD(3)),
        gas_limit                 UInt64                       CODEC(ZSTD(3)),
        gas_used                  UInt64                       CODEC(ZSTD(3)),
        timestamp                 UInt32                       CODEC(DoubleDelta, ZSTD(3)),
        base_fee_per_gas          Nullable(UInt64)             CODEC(ZSTD(3)),
        blob_gas_used             Nullable(UInt64)             CODEC(ZSTD(3)),
        excess_blob_gas           Nullable(UInt64)             CODEC(ZSTD(3)),
        parent_beacon_block_root  Nullable(FixedString(32))    CODEC(ZSTD(6)),
        withdrawals_root          Nullable(FixedString(32))    CODEC(ZSTD(6)),
        withdrawals               String                       CODEC(ZSTD(9)),
        uncles                    String                       CODEC(ZSTD(9)),
        mix_hash                  FixedString(32)              CODEC(ZSTD(6)),
        l1_block_number           Nullable(UInt64)             CODEC(ZSTD(3)),
        send_count                Nullable(String)             CODEC(ZSTD(9)),
        send_root                 Nullable(FixedString(32))    CODEC(ZSTD(6))
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (chain_id, number)
      SETTINGS index_granularity = 8192
    `,
  });
}
