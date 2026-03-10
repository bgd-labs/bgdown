import { createClient } from "@clickhouse/client";
import { all } from "better-all";
import { t } from "elysia";
import { LRUCache } from "lru-cache";
import env from "./env";

// ── ClickHouse client ───────────────────────────────────────────────────────

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

// ── Elysia schemas ──────────────────────────────────────────────────────────

export const Log = t.Object({
  address: t.String({
    description: "Address of the contract that emitted the log",
  }),
  blockHash: t.String({ description: "Hash of the block containing this log" }),
  blockNumber: t.Number({ description: "Block number containing this log" }),
  timestamp: t.Number({ description: "Unix timestamp of the block" }),
  data: t.String({ description: "ABI-encoded non-indexed log parameters" }),
  logIndex: t.Number({ description: "Index of this log within the block" }),
  topics: t.Array(t.String(), {
    description: "Indexed log topics; topics[0] is the event signature hash",
  }),
  transactionHash: t.String({
    description: "Hash of the transaction that emitted this log",
  }),
  transactionIndex: t.Number({
    description: "Index of the transaction within the block",
  }),
});

export const Block = t.Object({
  number: t.Number({ description: "Block number" }),
  hash: t.String({ description: "Block hash" }),
  parentHash: t.String({ description: "Parent block hash" }),
  nonce: t.String({ description: "Block nonce" }),
  sha3Uncles: t.String({ description: "SHA3 of uncles data" }),
  logsBloom: t.String({ description: "Bloom filter for logs" }),
  transactionsRoot: t.String({ description: "Root of transaction trie" }),
  stateRoot: t.String({ description: "Root of state trie" }),
  receiptsRoot: t.String({ description: "Root of receipts trie" }),
  miner: t.String({ description: "Address of the block miner/validator" }),
  difficulty: t.String({ description: "Block difficulty" }),
  totalDifficulty: t.String({
    description: "Total chain difficulty at this block",
  }),
  extraData: t.String({ description: "Extra data field" }),
  size: t.String({ description: "Block size in bytes" }),
  gasLimit: t.String({ description: "Gas limit" }),
  gasUsed: t.String({ description: "Gas used" }),
  timestamp: t.Number({ description: "Unix timestamp" }),
  baseFeePerGas: t.Nullable(
    t.String({ description: "Base fee per gas (post EIP-1559)" }),
  ),
  blobGasUsed: t.Nullable(
    t.String({ description: "Blob gas used (post EIP-4844)" }),
  ),
  excessBlobGas: t.Nullable(
    t.String({ description: "Excess blob gas (post EIP-4844)" }),
  ),
  parentBeaconBlockRoot: t.Nullable(
    t.String({ description: "Parent beacon block root (post Dencun)" }),
  ),
  withdrawalsRoot: t.Nullable(
    t.String({ description: "Withdrawals trie root (post Shanghai)" }),
  ),
  withdrawals: t.String({ description: "Withdrawals JSON array" }),
  uncles: t.String({ description: "Uncle hashes JSON array" }),
  mixHash: t.String({ description: "Mix hash" }),
  l1BlockNumber: t.Nullable(
    t.Number({ description: "L1 block number (L2 chains only)" }),
  ),
  sendCount: t.Nullable(t.String({ description: "Send count (Arbitrum)" })),
  sendRoot: t.Nullable(t.String({ description: "Send root (Arbitrum)" })),
});

// ── Row types ───────────────────────────────────────────────────────────────

export interface LogQueryRow {
  block_number: string;
  timestamp: string;
  transaction_id: string; // UInt64 as decimal string
  transaction_index: string;
  log_index: string;
  address_hex: string;
  data_hex: string;
  topic0_hex: string;
  topic1_hex: string | null;
  topic2_hex: string | null;
  topic3_hex: string | null;
}

export interface BlockQueryRow {
  number: string;
  hash_hex: string;
  parent_hash_hex: string;
  nonce: string;
  sha3_uncles_hex: string;
  logs_bloom_hex: string;
  transactions_root_hex: string;
  state_root_hex: string;
  receipts_root_hex: string;
  miner_hex: string;
  difficulty: string;
  total_difficulty: string;
  extra_data_hex: string;
  size: string;
  gas_limit: string;
  gas_used: string;
  timestamp: string;
  base_fee_per_gas: string | null;
  blob_gas_used: string | null;
  excess_blob_gas: string | null;
  parent_beacon_block_root_hex: string | null;
  withdrawals_root_hex: string | null;
  withdrawals: string;
  uncles: string;
  mix_hash_hex: string;
  l1_block_number: string | null;
  send_count: string | null;
  send_root_hex: string | null;
}

// ── SQL select templates ────────────────────────────────────────────────────

export const BLOCK_SELECT = `
  number,
  concat('0x', lower(hex(hash)))                  AS hash_hex,
  concat('0x', lower(hex(parent_hash)))            AS parent_hash_hex,
  toString(nonce)                                  AS nonce,
  concat('0x', lower(hex(sha3_uncles)))            AS sha3_uncles_hex,
  concat('0x', lower(hex(logs_bloom)))             AS logs_bloom_hex,
  concat('0x', lower(hex(transactions_root)))      AS transactions_root_hex,
  concat('0x', lower(hex(state_root)))             AS state_root_hex,
  concat('0x', lower(hex(receipts_root)))          AS receipts_root_hex,
  concat('0x', lower(hex(miner)))                  AS miner_hex,
  toString(difficulty)                             AS difficulty,
  total_difficulty,
  concat('0x', lower(hex(extra_data)))             AS extra_data_hex,
  toString(size)                                   AS size,
  toString(gas_limit)                              AS gas_limit,
  toString(gas_used)                               AS gas_used,
  timestamp,
  if(isNull(base_fee_per_gas), NULL, toString(assumeNotNull(base_fee_per_gas))) AS base_fee_per_gas,
  if(isNull(blob_gas_used), NULL, toString(assumeNotNull(blob_gas_used)))       AS blob_gas_used,
  if(isNull(excess_blob_gas), NULL, toString(assumeNotNull(excess_blob_gas)))   AS excess_blob_gas,
  if(isNull(parent_beacon_block_root), NULL, concat('0x', lower(hex(assumeNotNull(parent_beacon_block_root))))) AS parent_beacon_block_root_hex,
  if(isNull(withdrawals_root), NULL, concat('0x', lower(hex(assumeNotNull(withdrawals_root)))))                 AS withdrawals_root_hex,
  withdrawals,
  uncles,
  concat('0x', lower(hex(mix_hash)))               AS mix_hash_hex,
  if(isNull(l1_block_number), NULL, toString(assumeNotNull(l1_block_number))) AS l1_block_number,
  send_count,
  if(isNull(send_root), NULL, concat('0x', lower(hex(assumeNotNull(send_root))))) AS send_root_hex
`;

// block_hash and transaction_hash are fetched separately via enrichLogs()
// to avoid JOIN over 100M+ row lookup tables.
export const LOG_SELECT = `
  block_number,
  timestamp,
  transaction_id,
  transaction_index,
  log_index,
  concat('0x', lower(hex(address)))                                           AS address_hex,
  concat('0x', lower(hex(data)))                                              AS data_hex,
  concat('0x', lower(hex(topic0)))                                            AS topic0_hex,
  if(isNull(topic1), NULL, concat('0x', lower(hex(assumeNotNull(topic1))))) AS topic1_hex,
  if(isNull(topic2), NULL, concat('0x', lower(hex(assumeNotNull(topic2))))) AS topic2_hex,
  if(isNull(topic3), NULL, concat('0x', lower(hex(assumeNotNull(topic3))))) AS topic3_hex
`;

// ── Constants ───────────────────────────────────────────────────────────────

export const DEFAULT_LIMIT = 1_000;
export const MAX_LIMIT = 1_000_000;

export function clampLimit(limit: number | undefined): number {
  return Math.min(limit ?? DEFAULT_LIMIT, MAX_LIMIT);
}

// ── Cursor helpers ──────────────────────────────────────────────────────────

export function decodeCursor(cursor: string): {
  blockNumber: number;
  logIndex: number;
} {
  const [blockNumber, logIndex] = Buffer.from(cursor, "base64url")
    .toString()
    .split(":")
    .map(Number);
  return { blockNumber: blockNumber ?? 0, logIndex: logIndex ?? 0 };
}

export function encodeBlockCursor(blockNumber: number): string {
  return Buffer.from(`${blockNumber}`).toString("base64url");
}

export function decodeBlockCursor(cursor: string): number {
  return Number(Buffer.from(cursor, "base64url").toString());
}

// ── Hash lookups (with LRU cache) ───────────────────────────────────────────

// Block hashes and tx hashes are immutable once finalized — safe to cache.
const blockHashCache = new LRUCache<string, string>({ max: 50_000 });
const txHashCache = new LRUCache<string, string>({ max: 200_000 });

// Fetch block_hash for a set of block numbers in one primary-key lookup.
async function fetchBlockHashes(
  chainId: string,
  blockNumbers: string[],
): Promise<Map<string, string>> {
  try {
    if (blockNumbers.length === 0) return new Map();
    const out = new Map<string, string>();
    const missing: string[] = [];
    for (const num of blockNumbers) {
      const cached = blockHashCache.get(`${chainId}:${num}`);
      if (cached !== undefined) out.set(num, cached);
      else missing.push(num);
    }
    if (missing.length > 0) {
      const result = await clickhouse.query({
        query: `SELECT toString(number) AS num, concat('0x', lower(hex(hash))) AS hash_hex
                FROM ethereum.blocks
                WHERE chain_id = {chainId: UInt32} AND number IN ({nums: Array(UInt64)})`,
        query_params: { chainId, nums: missing.map(Number) },
        format: "JSONEachRow",
      });
      const rows = await result.json<{ num: string; hash_hex: string }>();
      for (const r of rows) {
        blockHashCache.set(`${chainId}:${r.num}`, r.hash_hex);
        out.set(r.num, r.hash_hex);
      }
    }
    return out;
  } catch (err) {
    throw new Error(
      `fetchBlockHashes failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// Fetch transaction_hash for a set of transaction_ids in one primary-key lookup.
async function fetchTxHashes(
  chainId: string,
  txIds: string[],
): Promise<Map<string, string>> {
  try {
    if (txIds.length === 0) return new Map();
    const out = new Map<string, string>();
    const missing: string[] = [];
    for (const tid of txIds) {
      const cached = txHashCache.get(`${chainId}:${tid}`);
      if (cached !== undefined) out.set(tid, cached);
      else missing.push(tid);
    }
    if (missing.length > 0) {
      const result = await clickhouse.query({
        query: `SELECT toString(transaction_id) AS tid, concat('0x', lower(hex(transaction_hash))) AS hash_hex
                FROM ethereum.transaction_hashes
                WHERE chain_id = {chainId: UInt32} AND transaction_id IN ({tids: Array(UInt64)})`,
        query_params: { chainId, tids: missing.map(Number) },
        format: "JSONEachRow",
      });
      const rows = await result.json<{ tid: string; hash_hex: string }>();
      for (const r of rows) {
        txHashCache.set(`${chainId}:${r.tid}`, r.hash_hex);
        out.set(r.tid, r.hash_hex);
      }
    }
    return out;
  } catch (err) {
    throw new Error(
      `fetchTxHashes failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// ── Row enrichment / conversion ─────────────────────────────────────────────

// Enrich raw log rows with block_hash and transaction_hash via parallel lookups.
export async function enrichLogs(
  chainId: string,
  rows: LogQueryRow[],
): Promise<(typeof Log.static)[]> {
  const blockNums = [...new Set(rows.map((r) => r.block_number))];
  const txIds = [...new Set(rows.map((r) => r.transaction_id))];
  const [blockHashes, txHashes] = await Promise.all([
    fetchBlockHashes(chainId, blockNums),
    fetchTxHashes(chainId, txIds),
  ]);
  return rows.map((row) => {
    const topics = [
      row.topic0_hex,
      row.topic1_hex,
      row.topic2_hex,
      row.topic3_hex,
    ].filter((t): t is string => t !== null && t !== "");
    return {
      address: row.address_hex,
      blockHash: blockHashes.get(String(row.block_number)) ?? "0x",
      blockNumber: Number(row.block_number),
      timestamp: Number(row.timestamp),
      data: row.data_hex,
      logIndex: Number(row.log_index),
      topics,
      transactionHash: txHashes.get(String(row.transaction_id)) ?? "0x",
      transactionIndex: Number(row.transaction_index),
    };
  });
}

export function rowToBlock(row: BlockQueryRow): typeof Block.static {
  return {
    number: Number(row.number),
    hash: row.hash_hex,
    parentHash: row.parent_hash_hex,
    nonce: row.nonce,
    sha3Uncles: row.sha3_uncles_hex,
    logsBloom: row.logs_bloom_hex,
    transactionsRoot: row.transactions_root_hex,
    stateRoot: row.state_root_hex,
    receiptsRoot: row.receipts_root_hex,
    miner: row.miner_hex,
    difficulty: row.difficulty,
    totalDifficulty: row.total_difficulty,
    extraData: row.extra_data_hex,
    size: row.size,
    gasLimit: row.gas_limit,
    gasUsed: row.gas_used,
    timestamp: Number(row.timestamp),
    baseFeePerGas: row.base_fee_per_gas,
    blobGasUsed: row.blob_gas_used,
    excessBlobGas: row.excess_blob_gas,
    parentBeaconBlockRoot: row.parent_beacon_block_root_hex,
    withdrawalsRoot: row.withdrawals_root_hex,
    withdrawals: row.withdrawals,
    uncles: row.uncles,
    mixHash: row.mix_hash_hex,
    l1BlockNumber: row.l1_block_number ? Number(row.l1_block_number) : null,
    sendCount: row.send_count,
    sendRoot: row.send_root_hex,
  };
}

// ── Shared route helpers ────────────────────────────────────────────────────

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
