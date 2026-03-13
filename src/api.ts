import { cors } from "@elysiajs/cors";
import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { logger } from "elysia-logger";
import pino from "pino";
import { CHAIN_BY_ID } from "./chains.ts";
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

async function discoverServers() {
  if (!env.PRIMARY) return [];
  const results = await Promise.allSettled(
    [...CHAIN_BY_ID.values()].map(async (chain) => {
      const url = `https://${chain.id}.logs.bgdlabs.com`;
      const r = await fetch(`${url}/spec.json`, {
        signal: AbortSignal.timeout(1_000),
      });
      if (!r.ok) throw new Error(`${r.status}`);
      return { url, description: chain.name };
    }),
  );
  return results.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
}

spawnIndexer();

const servers = await discoverServers();

new Elysia()
  .use(cors({ origin: /logs\.bgdlabs\.com$/ }))
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
      documentation: {
        info: { title: "BGDown API", version: "1.0.0" },
        ...(servers.length > 0 && { servers }),
      },
      path: "/",
      specPath: "/spec.json",
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
          url: `https://${id}.logs.bgdlabs.com`,
        };
      });
    },
    {
      response: {
        200: t.Array(
          t.Object({
            id: t.Number({ description: "EIP-155 chain ID" }),
            url: t.String({ description: "API URL for that chain" }),
          }),
        ),
      },
    },
  )
  .use(logRoutes)
  .listen(env.PORT);
