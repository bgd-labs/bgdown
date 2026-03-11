import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { logger } from "elysia-logger";
import { rateLimit } from "elysia-rate-limit";
import { tokenSet } from "./auth.ts";
import { CHAIN_BY_ID } from "./chains.ts";
import { clickhouse } from "./clickhouse.ts";
import env from "./env.ts";
import { logRoutes } from "./routes/logs.ts";

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
        query: "SELECT DISTINCT chain_id FROM logs ORDER BY chain_id",
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
        return;
      },
      query: t.Object({
        token: t.String({
          description: "API token",
          examples: ["replace-with-secure-token"],
          default: "",
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
        .group("/:chainId", (app) => app.use(logRoutes)),
  )
  .listen(env.PORT);
