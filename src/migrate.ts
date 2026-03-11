import { createClient } from "@clickhouse/client";
import type pino from "pino";
import env from "./env.ts";

interface Migration {
  name: string;
  up: (client: ReturnType<typeof createClient>) => Promise<void>;
}

// Add new migrations here in order. The name must be unique and should match
// the filename so it is easy to find. Numbers are purely for ordering.
// TODO: Make fs-based
const migrations: Migration[] = [
  {
    name: "0001_initial_schema",
    up: (await import("./migrations/0001_initial_schema.ts")).up,
  },
];

export async function runMigrations(logger: pino.Logger): Promise<void> {
  // Connect to the `default` database first so we can create our target DB
  // without hitting a chicken-and-egg problem.
  const bootstrap = createClient({
    url: env.CLICKHOUSE_URL,
    username: env.CLICKHOUSE_USERNAME,
    password: env.CLICKHOUSE_PASSWORD,
    database: "default",
  });

  try {
    // 2. Ensure the migrations tracking table exists.
    await bootstrap.command({
      query: `
        CREATE TABLE IF NOT EXISTS migrations
        (
          name        String,
          applied_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY applied_at
      `,
    });

    // 3. Fetch already-applied migration names.
    const result = await bootstrap.query({
      query: `SELECT name FROM migrations`,
      format: "JSONEachRow",
    });
    const applied = new Set(
      (await result.json<{ name: string }>()).map((r) => r.name),
    );

    // 4. Apply pending migrations in order.
    let ran = 0;
    for (const migration of migrations) {
      if (applied.has(migration.name)) continue;

      logger.info({ migration: migration.name }, "applying migration");
      await migration.up(bootstrap);
      await bootstrap.insert({
        table: `migrations`,
        values: [{ name: migration.name }],
        format: "JSONEachRow",
      });
      ran++;
      logger.info({ migration: migration.name }, "migration applied");
    }

    if (ran === 0) {
      logger.info("all migrations already applied");
    } else {
      logger.info({ ran }, "migrations applied");
    }
  } finally {
    await bootstrap.close();
  }
}
