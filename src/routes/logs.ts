import { Elysia, sse, t } from "elysia";

import {
  clampLimit,
  clickhouse,
  DEFAULT_LIMIT,
  fetchHeight,
  fetchStats,
  MAX_LIMIT,
} from "../clickhouse.ts";
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
  block_hash_hex: string;
  transaction_hash_hex: string;
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
  hexCol("block_hash"),
  hexCol("transaction_hash"),
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
  const [blockNumber, logIndex] = cursor.split("-").map(Number);
  return { blockNumber: blockNumber ?? 0, logIndex: logIndex ?? 0 };
}

function formatLogs(rows: LogQueryRow[]): (typeof Log.static)[] {
  return rows.map((row) => {
    const topics = [
      row.topic0_hex,
      row.topic1_hex,
      row.topic2_hex,
      row.topic3_hex,
    ].filter((t): t is string => t !== null && t !== "");
    return {
      address: row.address_hex,
      blockHash: row.block_hash_hex,
      blockNumber: Number(row.block_number),
      timestamp: Number(row.timestamp),
      data: row.data_hex,
      logIndex: Number(row.log_index),
      topics,
      transactionHash: row.transaction_hash_hex,
      transactionIndex: Number(row.transaction_index),
    };
  });
}

function buildLogsQuery(opts: {
  chainId: string;
  topic: string;
  address?: string;
  fromBlock?: number;
  toBlock?: number;
  cursor?: string;
  limit: number;
}) {
  const topicHex = opts.topic.slice(2).padStart(64, "0");
  const addressHex = opts.address?.toLowerCase().slice(2).padStart(40, "0");
  const cursor = opts.cursor ? decodeLogCursor(opts.cursor) : null;

  const lowerClause = cursor
    ? "AND (block_number, log_index) > ({cursorBlock: UInt64}, {cursorLogIndex: UInt32})"
    : opts.fromBlock !== undefined
      ? "AND block_number >= {fromBlock: UInt64}"
      : "";
  const upperClause =
    opts.toBlock !== undefined ? "AND block_number <= {toBlock: UInt64}" : "";
  const addressClause = addressHex
    ? "AND address = unhex({addressHex: String})"
    : "";

  return {
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
      chainId: opts.chainId,
      topicHex,
      limit: opts.limit,
      ...(cursor
        ? {
            cursorBlock: cursor.blockNumber,
            cursorLogIndex: cursor.logIndex,
          }
        : {}),
      ...(opts.fromBlock !== undefined && !cursor
        ? { fromBlock: opts.fromBlock }
        : {}),
      ...(opts.toBlock !== undefined ? { toBlock: opts.toBlock } : {}),
      ...(addressHex ? { addressHex } : {}),
    },
  };
}

const logsQueryParams = t.Object({
  topic: t.String({
    description:
      "Event signature hash (topic0) — required, maps directly to the primary index",
    examples: [
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    ],
  }),
  address: t.Optional(
    t.String({
      description: "Alias for emitter — contract address that emitted the log",
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
      description: "Pagination cursor in the format blockNumber-logIndex",
      examples: ["18000000-0"],
    }),
  ),
  limit: t.Optional(
    t.Numeric({
      description: `Number of logs to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
      examples: [100],
    }),
  ),
});

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
    async ({ params, query }) => {
      const limit = clampLimit(query.limit);
      const { query: sql, query_params } = buildLogsQuery({
        chainId: params.chainId,
        ...query,
        limit,
      });

      const result = await clickhouse.query({
        query: sql,
        query_params,
        format: "JSONEachRow",
      });

      const rows = await result.json<LogQueryRow>();
      const data = formatLogs(rows);

      const lastLog = data[data.length - 1];
      const nextCursor =
        data.length >= limit && lastLog
          ? `${lastLog.blockNumber}-${lastLog.logIndex}`
          : null;

      return { data, nextCursor };
    },
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      query: logsQueryParams,
      response: {
        200: t.Object({
          data: t.Array(Log),
          nextCursor: t.Nullable(
            t.String({
              description:
                "Cursor for the next page (blockNumber-logIndex), or null if this is the last page",
            }),
          ),
        }),
      },
    },
  )
  .get(
    "/logs/stream",
    async function* ({ params, query }) {
      const limit = clampLimit(query.limit);
      const { query: sql, query_params } = buildLogsQuery({
        chainId: params.chainId,
        ...query,
        limit,
      });

      const result = await clickhouse.query({
        query: sql,
        query_params,
        format: "JSONEachRow",
      });

      for await (const chunk of result.stream()) {
        const logs = formatLogs(chunk.map((r) => r.json<LogQueryRow>()));
        yield sse({
          data: logs,
        });
      }
    },
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      query: logsQueryParams,
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

      const [log] = formatLogs(rows);
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
