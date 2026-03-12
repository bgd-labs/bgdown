import { createClient } from "@clickhouse/client";
import type { SUPPORTED_CHAIN_IDS } from "./chains.ts";
import env from "./env.ts";

export const clickhouse = createClient({
  url: env.CLICKHOUSE_URL,
  username: env.CLICKHOUSE_USERNAME,
  password: env.CLICKHOUSE_PASSWORD,
});

export const DEFAULT_LIMIT = 1_000;
export const MAX_LIMIT = 1_000_000;

export async function fetchHeight(
  table: "logs" | "blocks",
  chainId: (typeof SUPPORTED_CHAIN_IDS)[number],
): Promise<number> {
  const column = table === "logs" ? "block_number" : "number";
  const result = await clickhouse.query({
    query: `SELECT max(${column}) AS height FROM ${table} WHERE chain_id = {chainId: UInt32}`,
    query_params: { chainId },
    format: "JSONEachRow",
  });
  const [row] = await result.json<{ height: string }>();
  return Number(row?.height ?? 0);
}

export async function fetchStats(
  table: "logs" | "blocks",
  chainId: (typeof SUPPORTED_CHAIN_IDS)[number],
): Promise<{
  total: number;
  maxIndexedBlock: number;
  compressedSize: string;
  compressionRatio: number;
}> {
  const column = table === "logs" ? "block_number" : "number";
  const partsFilter =
    table === "logs"
      ? "table IN ('logs', 'transaction_hashes')"
      : "table = 'blocks'";

  const result = await clickhouse.query({
    query: `
      SELECT
        count() AS total,
        max(${column}) AS max_block,
        (SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.parts WHERE ${partsFilter} AND active) AS compressed,
        (SELECT round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) FROM system.parts WHERE ${partsFilter} AND active) AS ratio
      FROM ${table}
      WHERE chain_id = {chainId: UInt32}
    `,
    query_params: { chainId },
    format: "JSONEachRow",
  });

  const [row] = await result.json<{
    total: string;
    max_block: string;
    compressed: string;
    ratio: number;
  }>();

  return {
    total: Number(row?.total ?? 0),
    maxIndexedBlock: Number(row?.max_block ?? 0),
    compressedSize: row?.compressed ?? "0 B",
    compressionRatio: row?.ratio ?? 0,
  };
}
