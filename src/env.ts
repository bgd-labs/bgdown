/** biome-ignore-all lint/style/noProcessEnv: this is the file re-mapping them */
import arkenv from "arkenv";

export default arkenv({
  CLICKHOUSE_URL: "string.url = 'http://localhost:8123'",
  CLICKHOUSE_DB: "string = 'ethereum'",
  HYPERSYNC_API_KEY: "string",
  LOG_LEVEL: "'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal' = 'info'",
  PORT: "number.port = 3000",
  CHAIN_ID: "number = 1",
});
