import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { rateLimit } from "elysia-rate-limit";
import { auth } from "./auth";

const TRUEBLOCKS_URL = process.env.TRUEBLOCKS_URL ?? "http://trueblocks:8080";
const MAX_BLOCK_RANGE = 100_000n;

const TOML_PATH = process.env.TRUEBLOCKS_CONFIG ?? "./trueBlocks.toml";

const toml = await Bun.file(TOML_PATH).text();
const config = Bun.TOML.parse(toml) as {
  chains: Record<string, { chain: string; chainId: string }>;
};

const CHAIN_NAMES: Record<string, string> = Object.fromEntries(
  Object.values(config.chains).map(({ chainId, chain }) => [chainId, chain]),
);

const ChainId = t.Union(Object.keys(CHAIN_NAMES).map((id) => t.Literal(id)));

const Log = t.Object({
  address: t.String(),
  blockHash: t.String(),
  blockNumber: t.Number(),
  data: t.Optional(t.String()),
  logIndex: t.Number(),
  timestamp: t.Number(),
  topics: t.Array(t.String()),
  transactionHash: t.String(),
  transactionIndex: t.Number(),
});

new Elysia()
  .use(
    openapi({
      documentation: {
        info: { title: "TrueBlocks API", version: "1.0.0" },
        components: {
          securitySchemes: {
            bearerAuth: { type: "http", scheme: "bearer" },
          },
        },
        security: [{ bearerAuth: [] }],
      },
    }),
  )
  .get("/", ({ redirect }) => redirect("/openapi"))
  .use(
    rateLimit({
      max: 60,
      duration: 60_000,
      generator: (req) =>
        req.headers.get("authorization")?.slice(7) ??
        req.headers.get("authorization") ??
        "",
    }),
  )
  .use(auth)
  .get(
    "/:chainId/stats",
    async ({ params }) => {
      const chain = CHAIN_NAMES[params.chainId] ?? "";
      const volumes = ["/cache", "/unchained"] as const;
      const result: Record<string, number> = {};

      for (const volume of volumes) {
        let size = 0;
        const glob = new Bun.Glob("**");
        for await (const file of glob.scan({ cwd: `${volume}/${chain}` })) {
          size += (await Bun.file(`${volume}/${chain}/${file}`).stat()).size;
        }
        result[volume] = size;
      }

      return result;
    },
    {
      params: t.Object({ chainId: ChainId }),
      response: {
        200: t.Record(t.String(), t.Number()),
        401: t.String(),
      },
    },
  )
  .get(
    "/:chainId/logs",
    async ({ params, query, status, set }) => {
      const chain = CHAIN_NAMES[params.chainId] ?? "";

      let from = BigInt(query.from);
      let to = BigInt(query.to);

      if (to < from) {
        [from, to] = [to, from];
      }
      if (to - from > MAX_BLOCK_RANGE) {
        return status(400, `Block range must be at most ${MAX_BLOCK_RANGE}`);
      }

      const url = new URL(`${TRUEBLOCKS_URL}/blocks`);
      url.searchParams.set("blocks", `${from}-${to}`);
      url.searchParams.set("chain", chain);
      url.searchParams.set("logs", "true");
      url.searchParams.set("cache", "true");
      for (const address of query.emitter ?? []) {
        url.searchParams.append("emitter", address);
      }
      for (const topic of query.topic ?? []) {
        url.searchParams.append("topic", topic);
      }

      const response = await fetch(url);
      if (!response.ok) {
        return status(502, await response.text());
      }
      set.headers["Cache-Control"] = "public, max-age=31536000, immutable";

      const json = (await response.json()) as {
        data: Array<{ date?: unknown } & Record<string, unknown>>;
      };
      return (json.data ?? []).map(
        ({ date: _, ...log }) => log,
      ) as unknown as Array<typeof Log.static>;
    },
    {
      params: t.Object({ chainId: ChainId }),
      query: t.Object({
        from: t.Numeric(),
        to: t.Numeric(),
        emitter: t.Optional(t.Union([t.Array(t.String()), t.String()])),
        topic: t.Optional(t.Union([t.Array(t.String()), t.String()])),
      }),
      response: {
        200: t.Array(Log),
        400: t.String(),
        401: t.String(),
        502: t.String(),
      },
    },
  )
  .listen(3000);

console.log("Listening on http://localhost:3000");
