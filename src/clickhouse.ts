import { createClient } from "@clickhouse/client";
import { all } from "better-all";
import env from "./env";

export const clickhouse = createClient({
  url: env.CLICKHOUSE_URL,
  username: env.CLICKHOUSE_USERNAME,
  password: env.CLICKHOUSE_PASSWORD,
  database: env.CLICKHOUSE_DB,
  compression: {
    response: true,
    request: true,
  },
  request_timeout: 60_000,
});

export const DEFAULT_LIMIT = 1_000;
export const MAX_LIMIT = 1_000_000;

export function clampLimit(limit: number | undefined): number {
  return Math.min(limit ?? DEFAULT_LIMIT, MAX_LIMIT);
}

export async function fetchHeight(
  table: "logs" | "blocks",
  chainId: string,
): Promise<number> {
  const column = table === "logs" ? "block_number" : "number";
  const result = await clickhouse.query({
    query: `SELECT max(${column}) AS height FROM ethereum.${table} WHERE chain_id = {chainId: UInt32}`,
    query_params: { chainId },
    format: "JSONEachRow",
  });
  const [row] = await result.json<{ height: string }>();
  return Number(row?.height ?? 0);
}

export async function fetchStats(
  table: "logs" | "blocks",
  chainId: string,
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

  const { countResult, partsResult } = await all({
    countResult: () =>
      clickhouse.query({
        query: `SELECT count() AS total, max(${column}) AS max_block FROM ethereum.${table} WHERE chain_id = {chainId: UInt32}`,
        query_params: { chainId },
        format: "JSONEachRow",
      }),
    partsResult: () =>
      clickhouse.query({
        query: `SELECT formatReadableSize(sum(data_compressed_bytes)) AS compressed, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.parts WHERE ${partsFilter} AND active`,
        format: "JSONEachRow",
      }),
  });

  const [counts] = await countResult.json<{
    total: string;
    max_block: string;
  }>();
  const [parts] = await partsResult.json<{
    compressed: string;
    ratio: number;
  }>();

  return {
    total: Number(counts?.total ?? 0),
    maxIndexedBlock: Number(counts?.max_block ?? 0),
    compressedSize: parts?.compressed ?? "0 B",
    compressionRatio: parts?.ratio ?? 0,
  };
}
