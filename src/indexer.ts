import pino from "pino";
import { CHAIN_BY_ID } from "./chains.ts";
import { clickhouse } from "./clickhouse.ts";
import env from "./env.ts";
import { getHypersyncForChain } from "./hypersync.ts";
import { ensureSchema } from "./schema.ts";
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

  await ensureSchema(logger);

  const hypersync = getHypersyncForChain(env.CHAIN_ID);

  let { startBlock, totalLogs } = await getChainState(clickhouse, env.CHAIN_ID);
  logger.info({ startBlock, totalLogs }, "connected, resuming ingestion");

  const heightStream = await hypersync.streamHeight();

  const queue = batchQueue(
    createDbWriter({ clickhouse, logger, chainId: env.CHAIN_ID }),
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
        onEvents: async (items) => {
          const t0 = performance.now();
          const validatedEvents = parseHyperSyncResponse(items);
          logger.info({ ms: (performance.now() - t0).toFixed(1) }, "schema validation");
          await queue.enqueue(validatedEvents);
        },
      });

      // flush the remaining items in the queue
      await queue.flush();

      startBlock = res.nextBlock;

      logger.info(
        {
          fromBlock: startBlock,
          toBlock: safeBlock,
          logsSynced: res.totalLogs,
        },
        "finished streaming",
      );
    } catch (err) {
      logger.error(err, "error during sync iteration");
    }
  }
} catch (err) {
  pino().error(err, "fatal error");
  process.exit(1);
}
