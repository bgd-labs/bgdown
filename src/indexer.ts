import pino from "pino";
import { CHAIN_BY_ID } from "./chains.ts";
import { clickhouse } from "./clickhouse.ts";
import env from "./env.ts";
import { getHypersyncForChain } from "./hypersync.ts";
import { runMigrations } from "./migrate.ts";
import { getChainState, runStream } from "./streamer.ts";
import { batchQueue } from "./utils/batch-queue.ts";
import { createDbWriter } from "./utils/sql.ts";
import { parseHyperSyncResponse } from "./validator.ts";

try {
  const chain = CHAIN_BY_ID.get(env.CHAIN_ID);
  if (!chain)
    throw new Error(`Chain ${env.CHAIN_ID} not found in chains config`);

  const logger = pino({ level: env.LOG_LEVEL }).child({
    chainId: env.CHAIN_ID,
  });

  if (env.PRIMARY) {
    await runMigrations(logger);
  } else {
    logger.info("Waiting for PRIMARY node to complete migrations...");

    const primaryUrl = `${env.PRIMARY_URL}/health`;
    let healthy = false;

    while (!healthy) {
      try {
        const res = await fetch(primaryUrl);
        const health = (await res.json()) as {
          status: string;
          sourceCommit: string;
        };

        if (
          health.status === "ok" &&
          health.sourceCommit === env.SOURCE_COMMIT
        ) {
          logger.info("PRIMARY is ready and on same commit, proceeding");
          healthy = true;
        } else {
          logger.warn(
            {
              status: health.status,
              primaryCommit: health.sourceCommit,
              localCommit: env.SOURCE_COMMIT,
            },
            "PRIMARY not ready or commit mismatch",
          );
        }
      } catch (err) {
        logger.warn(
          { error: String(err) },
          "Failed to reach PRIMARY health endpoint, retrying...",
        );
      }

      if (!healthy) {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  const hypersync = getHypersyncForChain(chain.id);

  let { startBlock, totalLogs } = await getChainState(clickhouse, chain.id);

  logger.info({ startBlock, totalLogs }, "connected, resuming ingestion");

  const heightStream = await hypersync.streamHeight();

  const queue = batchQueue(
    createDbWriter({ clickhouse, logger, chainId: chain.id }),
    {
      batchSize: 500_000,
      timeout: 30_000,
    },
  );

  while (true) {
    const event = await heightStream.recv();

    if (event === null) {
      logger.info("height stream closed, exiting");
      break;
    }

    if (event.type === "Reconnecting") {
      logger.warn(
        { delayMillis: event.delayMillis, errorMsg: event.errorMsg },
        "height stream reconnecting",
      );
      continue;
    }

    if (event.type !== "Height") {
      continue;
    }

    // HyperSync only advances its height over data that is already indexed
    // (finalized-ish), so we use it directly as our safe ceiling.
    const safeBlock = event.height - chain.reorgSafetyFallback;

    if (safeBlock <= startBlock) {
      logger.info(
        { safeBlock, startBlock },
        "at tip, waiting for next height event",
      );
      continue;
    }

    logger.info(
      { fromBlock: startBlock, toBlock: safeBlock },
      "started streaming",
    );

    try {
      const res = await runStream({
        hypersync,
        fromBlock: startBlock,
        toBlock: safeBlock,
        logger,
        initialTotalLogs: totalLogs,
        onEvents: async (items) => {
          const validatedEvents = parseHyperSyncResponse(items);
          await queue.enqueue(validatedEvents);
        },
      });

      // flush the remaining items in the queue
      await queue.flush();

      logger.info(
        {
          fromBlock: startBlock,
          toBlock: safeBlock,
          logsSynced: res.totalLogs,
        },
        "finished streaming",
      );

      totalLogs = res.totalLogs;
      startBlock = res.nextBlock;
    } catch (err) {
      logger.error(err, "error during sync iteration");
    }
  }
} catch (err) {
  pino().error(err, "fatal error");
  // If running as a worker, just let the error propagate so the parent
  // can handle it via the worker's "error" event. Only exit when running
  // as the main thread.
  if (Bun.isMainThread) process.exit(1);
}
