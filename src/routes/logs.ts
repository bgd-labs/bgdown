import { Elysia, sse, t } from "elysia";
import { LRUCache } from "lru-cache";
import {
  clampLimit,
  clickhouse,
  DEFAULT_LIMIT,
  fetchHeight,
  fetchStats,
  MAX_LIMIT,
} from "../clickhouse.ts";
import { decodeCursor } from "../utils/cursor.ts";
import { hexCol, nullableHexCol, select } from "../utils/sql.ts";

const Log = t.Object({
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

interface LogQueryRow {
  block_number: string;
  timestamp: string;
  transaction_id: string;
  transaction_index: string;
  log_index: string;
  address_hex: string;
  data_hex: string;
  topic0_hex: string;
  topic1_hex: string | null;
  topic2_hex: string | null;
  topic3_hex: string | null;
}

const LOG_SELECT = select(
  "block_number",
  "timestamp",
  "transaction_id",
  "transaction_index",
  "log_index",
  hexCol("address"),
  hexCol("data"),
  hexCol("topic0"),
  nullableHexCol("topic1"),
  nullableHexCol("topic2"),
  nullableHexCol("topic3"),
);

function decodeLogCursor(cursor: string): {
  blockNumber: number;
  logIndex: number;
} {
  const [blockNumber, logIndex] = decodeCursor(cursor).split(":").map(Number);
  return { blockNumber: blockNumber ?? 0, logIndex: logIndex ?? 0 };
}

// Block hashes and tx hashes are immutable once finalized — safe to cache.
function createHashLookup(opts: {
  maxSize: number;
  query: string;
  keyParam: string;
  keyField: string;
  label: string;
}): (chainId: string, keys: string[]) => Promise<Map<string, string>> {
  const cache = new LRUCache<string, string>({ max: opts.maxSize });
  return async (chainId, keys) => {
    if (keys.length === 0) return new Map();
    const out = new Map<string, string>();
    const missing: string[] = [];
    for (const key of keys) {
      const cached = cache.get(`${chainId}:${key}`);
      if (cached !== undefined) out.set(key, cached);
      else missing.push(key);
    }
    if (missing.length > 0) {
      try {
        const result = await clickhouse.query({
          query: opts.query,
          query_params: { chainId, [opts.keyParam]: missing.map(Number) },
          format: "JSONEachRow",
        });
        const rows = await result.json<{ key: string; hash_hex: string }>();
        for (const r of rows) {
          cache.set(`${chainId}:${r.key}`, r.hash_hex);
          out.set(r.key, r.hash_hex);
        }
      } catch (err) {
        throw new Error(
          `${opts.label} failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
    return out;
  };
}

const fetchBlockHashes = createHashLookup({
  maxSize: 50_000,
  query: `SELECT toString(number) AS key, concat('0x', lower(hex(hash))) AS hash_hex
          FROM ethereum.blocks
          WHERE chain_id = {chainId: UInt32} AND number IN ({nums: Array(UInt64)})`,
  keyParam: "nums",
  keyField: "key",
  label: "fetchBlockHashes",
});

const fetchTxHashes = createHashLookup({
  maxSize: 200_000,
  query: `SELECT toString(transaction_id) AS key, concat('0x', lower(hex(transaction_hash))) AS hash_hex
          FROM ethereum.transaction_hashes
          WHERE chain_id = {chainId: UInt32} AND transaction_id IN ({tids: Array(UInt64)})`,
  keyParam: "tids",
  keyField: "key",
  label: "fetchTxHashes",
});

async function enrichLogs(
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

export const logRoutes = new Elysia()
  .get(
    "/logs/height",
    async ({ params }) => ({
      height: await fetchHeight("logs", params.chainId),
    }),
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      response: {
        200: t.Object({
          height: t.Number({
            description: "Last indexed block number with logs",
          }),
        }),
      },
    },
  )
  .get(
    "/logs/stats",
    async ({ params }) => fetchStats("logs", params.chainId),
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      response: {
        200: t.Object({
          total: t.Number({
            description: "Total number of indexed logs",
          }),
          maxIndexedBlock: t.Number({
            description: "Highest block number with logs indexed",
          }),
          compressedSize: t.String({
            description: "Compressed size of the logs table on disk",
          }),
          compressionRatio: t.Number({
            description: "Uncompressed / compressed ratio",
          }),
        }),
      },
    },
  )
  .get(
    "/logs",
    async function* ({ params, query }) {
      const limit = clampLimit(query.limit);
      const cursor = query.cursor ? decodeLogCursor(query.cursor) : null;

      const topicHex = query.topic.slice(2).padStart(64, "0");
      const emitterHex = (query.emitter ?? query.address)
        ?.toLowerCase()
        .slice(2)
        .padStart(40, "0");

      const lowerClause = cursor
        ? "AND (block_number, log_index) > ({cursorBlock: UInt64}, {cursorLogIndex: UInt32})"
        : query.fromBlock !== undefined
          ? "AND block_number >= {fromBlock: UInt64}"
          : "";
      const upperClause =
        query.toBlock !== undefined
          ? "AND block_number <= {toBlock: UInt64}"
          : "";
      const addressClause = emitterHex
        ? "AND address = unhex({emitterHex: String})"
        : "";

      const result = await clickhouse.query({
        query: `
          SELECT ${LOG_SELECT}
          FROM logs
          WHERE
            chain_id = {chainId: UInt32}
            AND topic0 = unhex({topicHex: String})
            ${lowerClause}
            ${upperClause}
            ${addressClause}
          ORDER BY block_number, log_index
          LIMIT {limit: UInt32}
        `,
        query_params: {
          chainId: params.chainId,
          topicHex,
          limit,
          ...(cursor
            ? {
                cursorBlock: cursor.blockNumber,
                cursorLogIndex: cursor.logIndex,
              }
            : {}),
          ...(query.fromBlock !== undefined && !cursor
            ? { fromBlock: query.fromBlock }
            : {}),
          ...(query.toBlock !== undefined ? { toBlock: query.toBlock } : {}),
          ...(emitterHex ? { emitterHex } : {}),
        },
        format: "JSONEachRow",
      });

      for await (const chunk of result.stream()) {
        const logs = await enrichLogs(
          params.chainId,
          chunk.map((r) => r.json<LogQueryRow>()),
        );
        yield sse({
          data: logs,
        });
      }
    },
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      query: t.Object({
        topic: t.String({
          description:
            "Event signature hash (topic0) — required, maps directly to the primary index",
          examples: [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          ],
        }),
        emitter: t.Optional(
          t.String({
            description:
              "Contract address; combined with topic for a full primary-key lookup",
            examples: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
          }),
        ),
        address: t.Optional(
          t.String({
            description:
              "Alias for emitter — contract address that emitted the log",
            examples: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
          }),
        ),
        fromBlock: t.Optional(
          t.Numeric({
            description:
              "First block to include (inclusive). Ignored when cursor is provided.",
            examples: [18_000_000],
          }),
        ),
        toBlock: t.Optional(
          t.Numeric({
            description: "Last block to include (inclusive).",
            examples: [18_001_000],
          }),
        ),
        cursor: t.Optional(
          t.String({
            description:
              "Opaque pagination cursor from the previous response's nextCursor",
            examples: ["MTAwMDAwMDA6MA"],
          }),
        ),
        limit: t.Optional(
          t.Numeric({
            description: `Number of logs to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
            examples: [100],
          }),
        ),
      }),
    },
  )
  .get(
    "/log/:blockNumber/:logIndex",
    async ({ params, status }) => {
      const result = await clickhouse.query({
        query: `
          SELECT ${LOG_SELECT}
          FROM logs
          WHERE chain_id = {chainId: UInt32}
            AND block_number = {blockNumber: UInt64}
            AND log_index = {logIndex: UInt32}
          LIMIT 1
        `,
        query_params: {
          chainId: params.chainId,
          blockNumber: params.blockNumber,
          logIndex: params.logIndex,
        },
        format: "JSONEachRow",
      });

      const rows = await result.json<LogQueryRow>();
      if (rows.length === 0) return status(404, "Log not found");

      const [log] = await enrichLogs(params.chainId, rows);
      return log;
    },
    {
      params: t.Object({
        chainId: t.String({ examples: ["1"] }),
        blockNumber: t.String({ examples: ["17000000"] }),
        logIndex: t.String({ examples: ["0"] }),
      }),
      response: {
        200: Log,
        404: t.String(),
      },
    },
  );
