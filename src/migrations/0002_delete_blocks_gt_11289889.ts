import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

export async function up(client: ClickHouseClient): Promise<void> {
  await client.command({
    query: `DELETE FROM ${env.CLICKHOUSE_DB}.logs WHERE block_number > 11289889`,
  });
}
