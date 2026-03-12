/** biome-ignore-all lint/style/noProcessEnv: this is the file re-mapping them */
import arkenv from "arkenv";
import { type } from "arktype";
import { SUPPORTED_CHAIN_IDS } from "./chains.ts";

const raw = arkenv({
  CLICKHOUSE_URL: "string.url = 'http://localhost:8123'",
  HYPERSYNC_API_KEY: "string",
  LOG_LEVEL: "'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal' = 'info'",
  PORT: "number.port = 3000",
  CHAIN_ID: type("number.integer")
    .narrow((id) =>
      SUPPORTED_CHAIN_IDS.some((supportedId) => supportedId === id),
    )
    .pipe((id) => id as (typeof SUPPORTED_CHAIN_IDS)[number]),
});

// Normalise the ClickHouse URL: fix scheme, remap native TCP port, then extract
// any credentials/database embedded in the URL so they can be passed explicitly
// to @clickhouse/client (which warns when the URL overrides explicit config).
const parsedUrl = new URL(
  raw.CLICKHOUSE_URL.replace(/^clickhouse:\/\//, "http://").replace(
    /:9000(\/|$)/,
    ":8123$1",
  ),
);

const CLICKHOUSE_USERNAME = parsedUrl.username || "default";
const CLICKHOUSE_PASSWORD = parsedUrl.password || "default";

// Strip credentials and path from the URL before handing it to the client.
parsedUrl.username = "";
parsedUrl.password = "";
parsedUrl.pathname = "/";

export default {
  ...raw,
  CLICKHOUSE_URL: parsedUrl.toString(),
  CLICKHOUSE_USERNAME,
  CLICKHOUSE_PASSWORD,
};
