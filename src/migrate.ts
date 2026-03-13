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

export async function ensureMigrations(logger: pino.Logger): Promise<void> {
  if (env.PRIMARY) {
    logger.info(
      { sourceCommit: env.SOURCE_COMMIT },
      "PRIMARY node: running migrations",
    );
    await applyMigrations(logger);
  } else {
    logger.info(
      { sourceCommit: env.SOURCE_COMMIT },
      "SECONDARY node: waiting for PRIMARY to complete migrations",
    );
    await waitForPrimaryMigrations(logger);
  }
}

async function applyMigrations(logger: pino.Logger): Promise<void> {
  logger.info(
    { totalMigrations: migrations.length },
    "Starting migration check on PRIMARY node",
  );

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

    logger.info(
      {
        alreadyApplied: applied.size,
        remaining: migrations.length - applied.size,
      },
      "Migration status check",
    );

    // 4. Apply pending migrations in order.
    let ran = 0;
    for (const migration of migrations) {
      if (applied.has(migration.name)) continue;

      logger.info({ migration: migration.name }, "PRIMARY: applying migration");
      await migration.up(bootstrap);
      await bootstrap.insert({
        table: `migrations`,
        values: [{ name: migration.name }],
        format: "JSONEachRow",
      });
      ran++;
      logger.info(
        { migration: migration.name },
        "PRIMARY: migration applied successfully",
      );
    }

    if (ran === 0) {
      logger.info("PRIMARY: all migrations already applied, no action needed");
    } else {
      logger.info(
        { appliedCount: ran, totalApplied: applied.size + ran },
        "PRIMARY: migrations complete, ready to accept requests",
      );
    }
  } finally {
    await bootstrap.close();
  }
}

async function waitForPrimaryMigrations(logger: pino.Logger): Promise<void> {
  logger.info(
    { primaryUrl: env.PRIMARY_URL },
    "Secondary node waiting for PRIMARY to complete migrations before proceeding",
  );

  const primaryUrl = new URL("/health", env.PRIMARY_URL).toString();
  let healthy = false;
  let attempts = 0;

  while (!healthy) {
    attempts++;
    try {
      const res = await fetch(primaryUrl);
      const health = (await res.json()) as {
        status: string;
        sourceCommit: string;
      };

      if (health.status === "ok" && health.sourceCommit === env.SOURCE_COMMIT) {
        logger.info(
          { attempts, primaryCommit: health.sourceCommit },
          "Secondary: PRIMARY is healthy and on same commit, proceeding with indexing",
        );
        healthy = true;
      } else if (health.sourceCommit !== env.SOURCE_COMMIT) {
        logger.warn(
          {
            attempts,
            primaryCommit: health.sourceCommit,
            secondaryCommit: env.SOURCE_COMMIT,
          },
          "Secondary: PRIMARY is healthy but on different commit, waiting for deployment match",
        );
      } else {
        logger.warn(
          { attempts, status: health.status },
          "Secondary: PRIMARY migrations still in progress, retrying",
        );
      }
    } catch (err) {
      logger.warn(
        { attempts, error: String(err) },
        "Secondary: failed to reach PRIMARY health endpoint, will retry",
      );
    }

    if (!healthy) {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
