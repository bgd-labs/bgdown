import { openapi } from "@elysiajs/openapi";
import { Elysia, sse, t } from "elysia";
import { logger } from "elysia-logger";
import { rateLimit } from "elysia-rate-limit";
import { tokenSet } from "./auth";
import { CHAIN_BY_ID, getViemForChain } from "./chains";
import {
  clampLimit,
  clickhouse,
  DEFAULT_LIMIT,
  fetchHeight,
  fetchStats,
  MAX_LIMIT,
} from "./clickhouse";
import env from "./env";
import { getHypersyncForChain } from "./hypersync";
import {
  BLOCK_SELECT,
  Block,
  type BlockQueryRow,
  decodeBlockCursor,
  encodeBlockCursor,
  rowToBlock,
} from "./routes/blocks";
import {
  decodeCursor,
  enrichLogs,
  LOG_SELECT,
  Log,
  type LogQueryRow,
} from "./routes/logs";

new Elysia()
  .use(logger())
  .onError(({ log, error, request }) => {
    log.error(`Error on ${request.method} ${request.url}: ${error}`);
  })
  .use(
    openapi({
      documentation: { info: { title: "BGDown API", version: "1.0.0" } },
    }),
  )
  .get("/", ({ redirect }) => redirect("/openapi"))
  .get(
    "/chains",
    async () => {
      const result = await clickhouse.query({
        query: "SELECT DISTINCT chain_id FROM ethereum.logs ORDER BY chain_id",
        format: "JSONEachRow",
      });
      const rows = await result.json<{ chain_id: string }>();
      return rows.map(({ chain_id }) => {
        const id = Number(chain_id);
        return { id, name: CHAIN_BY_ID.get(id)?.name ?? `chain-${id}` };
      });
    },
    {
      response: {
        200: t.Array(
          t.Object({
            id: t.Number({ description: "EIP-155 chain ID" }),
            name: t.String({ description: "Chain name" }),
          }),
        ),
      },
    },
  )
  .guard(
    {
      beforeHandle: ({ query, status }) => {
        if (!tokenSet.has(query.token)) return status(401, "Unauthorized");
      },
      query: t.Object({
        token: t.String({
          description: "API token",
          examples: ["replace-with-secure-token"],
        }),
      }),
      response: {
        401: t.String(),
      },
    },
    (app) =>
      app
        .use(
          rateLimit({
            max: 600,
            duration: 60_000,
            generator: (req) =>
              new URL(req.url).searchParams.get("token") ??
              req.headers.get("x-forwarded-for") ??
              "",
          }),
        )
        .group("/:chainId", (app) =>
          app
            .derive(({ params }) => {
              const chainId = Number(params.chainId);
              return {
                viem: getViemForChain(chainId),
                hypersync: getHypersyncForChain(chainId),
              };
            })
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
              "/blocks/height",
              async ({ params }) => ({
                height: await fetchHeight("blocks", params.chainId),
              }),
              {
                params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
                response: {
                  200: t.Object({
                    height: t.Number({
                      description: "Last indexed block number",
                    }),
                  }),
                },
              },
            )
            .get(
              "/blocks/stats",
              async ({ params }) => fetchStats("blocks", params.chainId),
              {
                params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
                response: {
                  200: t.Object({
                    total: t.Number({
                      description: "Total number of indexed blocks",
                    }),
                    maxIndexedBlock: t.Number({
                      description: "Highest block number indexed",
                    }),
                    compressedSize: t.String({
                      description:
                        "Compressed size of the blocks table on disk",
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
                const cursor = query.cursor ? decodeCursor(query.cursor) : null;

                const topicHex = query.topic.slice(2).padStart(64, "0");
                const emitterHex = (query.emitter ?? query.address)
                  ?.toLowerCase()
                  .slice(2)
                  .padStart(40, "0");

                // Lower bound: cursor position takes priority over fromBlock when
                // paginating; fromBlock applies only on the first page.
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
                    FROM ethereum.logs
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
                    ...(query.toBlock !== undefined
                      ? { toBlock: query.toBlock }
                      : {}),
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
                    FROM ethereum.logs
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
            )
            .get(
              "/blocks",
              async ({ params, query }) => {
                const limit = clampLimit(query.limit);
                const cursor = query.cursor
                  ? decodeBlockCursor(query.cursor)
                  : null;

                const cursorClause = cursor
                  ? "AND number > {cursorBlock: UInt64}"
                  : "";

                const result = await clickhouse.query({
                  query: `
                    SELECT ${BLOCK_SELECT}
                    FROM ethereum.blocks
                    WHERE chain_id = {chainId: UInt32}
                      ${cursorClause}
                    ORDER BY number
                    LIMIT {limit: UInt32}
                  `,
                  query_params: {
                    chainId: params.chainId,
                    limit,
                    ...(cursor ? { cursorBlock: cursor } : {}),
                  },
                  format: "JSONEachRow",
                });

                const rows = await result.json<BlockQueryRow>();
                const blocks = rows.map(rowToBlock);

                const lastRow = rows.at(-1);
                const nextCursor =
                  rows.length === limit && lastRow
                    ? encodeBlockCursor(Number(lastRow.number))
                    : null;

                return { blocks, nextCursor };
              },
              {
                params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
                query: t.Object({
                  cursor: t.Optional(
                    t.String({
                      description:
                        "Opaque pagination cursor from the previous response's nextCursor",
                      examples: ["MTAwMDA"],
                    }),
                  ),
                  limit: t.Optional(
                    t.Numeric({
                      description: `Number of blocks to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
                      examples: [100],
                    }),
                  ),
                }),
                response: {
                  200: t.Object({
                    blocks: t.Array(Block),
                    nextCursor: t.Nullable(
                      t.String({
                        description:
                          "Pass as cursor in the next request to fetch the following page; null when no more results",
                      }),
                    ),
                  }),
                },
              },
            )
            .get(
              "/block/:blockNumber",
              async ({ params, status }) => {
                const result = await clickhouse.query({
                  query: `
                    SELECT ${BLOCK_SELECT}
                    FROM ethereum.blocks
                    WHERE chain_id = {chainId: UInt32}
                      AND number = {blockNumber: UInt64}
                    LIMIT 1
                  `,
                  query_params: {
                    chainId: params.chainId,
                    blockNumber: params.blockNumber,
                  },
                  format: "JSONEachRow",
                });

                const rows = await result.json<BlockQueryRow>();
                if (rows.length === 0) return status(404, "Block not found");

                return rowToBlock(rows[0]);
              },
              {
                params: t.Object({
                  chainId: t.String({ examples: ["1"] }),
                  blockNumber: t.String({ examples: ["17000000"] }),
                }),
                response: {
                  200: Block,
                  404: t.String(),
                },
              },
            ),
        ),
  )
  .listen(env.PORT);
