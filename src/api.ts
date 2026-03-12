import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { logger } from "elysia-logger";
import pino from "pino";
import { clickhouse } from "./clickhouse.ts";
import env from "./env.ts";
import { logRoutes } from "./routes/logs.ts";

const indexerLogger = pino({ level: env.LOG_LEVEL }).child({
  module: "indexer-supervisor",
});

function spawnIndexer() {
  const worker = new Worker(new URL("./indexer.ts", import.meta.url));

  worker.addEventListener("error", (event) => {
    indexerLogger.error({ error: event.message }, "indexer worker error");
  });

  worker.addEventListener("close", () => {
    indexerLogger.warn("indexer worker exited, restarting in 5s");
    setTimeout(spawnIndexer, 5_000);
  });

  indexerLogger.info("indexer worker started");
  return worker;
}

spawnIndexer();

new Elysia()
  .use(
    logger({
      level: env.LOG_LEVEL,
      transport: "json",
    }),
  )
  .onError(({ log, error, request }) => {
    log.error(`Error on ${request.method} ${request.url}: ${error}`);
  })
  .use(
    openapi({
      documentation: { info: { title: "BGDown API", version: "1.0.0" } },
      path: "/",
    }),
  )
  .get(
    "/chains",
    async () => {
      const result = await clickhouse.query({
        query: "SELECT DISTINCT chain_id FROM logs ORDER BY chain_id",
        format: "JSONEachRow",
      });
      const rows = await result.json<{ chain_id: string }>();
      return rows.map(({ chain_id }) => {
        const id = Number(chain_id);
        return {
          id,
        };
      });
    },
    {
      response: {
        200: t.Array(
          t.Object({
            id: t.Number({ description: "EIP-155 chain ID" }),
          }),
        ),
      },
    },
  )
  .use(logRoutes)
  .listen(env.PORT);
