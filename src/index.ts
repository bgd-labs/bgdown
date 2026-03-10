import { createClient } from "@clickhouse/client";
import { HypersyncClient } from "@envio-dev/hypersync-client";
import pino from "pino";
import { CHAIN_BY_ID } from "./chains";
import env from "./env";
import type { BlockRow, LogRow, TxHashRow } from "./row-binary";
import { ensureSchema } from "./schema";
import {
  BLOCK_FLUSH_BATCH_SIZE,
  Flusher,
  flushBlockBatch,
  flushLogBatch,
  flushTxHashBatch,
  getChainState,
  LOG_FLUSH_BATCH_SIZE,
  runStream,
} from "./streamer";

try {
  const chain = CHAIN_BY_ID.get(env.CHAIN_ID);
  if (!chain)
    throw new Error(`Chain ${env.CHAIN_ID} not found in chains config`);

  const log = pino({ level: env.LOG_LEVEL }).child({ chainId: env.CHAIN_ID });

  const clickhouse = createClient({
    url: env.CLICKHOUSE_URL,
    username: env.CLICKHOUSE_USERNAME,
    password: env.CLICKHOUSE_PASSWORD,
    database: env.CLICKHOUSE_DB,
    clickhouse_settings: {
      async_insert: 1,
      wait_for_async_insert: 0,
    },
  });

  await ensureSchema(log);

  const hypersync = new HypersyncClient({
    url: chain.hypersyncUrl,
    apiToken: env.HYPERSYNC_API_KEY,
  });

  let { startBlock, totalLogs } = await getChainState(clickhouse, env.CHAIN_ID);
  log.info({ startBlock, totalLogs }, "connected, resuming ingestion");

  const heightStream = await hypersync.streamHeight();

  while (true) {
    const event = await heightStream.recv();

    if (event === null) {
      log.info("height stream closed, exiting");
      break;
    }

    if (event.type === "Reconnecting") {
      log.warn(
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
      log.info(
        { safeBlock, startBlock },
        "at tip, waiting for next height event",
      );
      continue;
    }

    log.info(
      { fromBlock: startBlock, toBlock: safeBlock },
      "started streaming",
    );

    try {
      const logFlusher = new Flusher<LogRow>(
        log,
        flushLogBatch,
        "logs",
        LOG_FLUSH_BATCH_SIZE,
        totalLogs,
      );
      const blockFlusher = new Flusher<BlockRow>(
        log,
        flushBlockBatch,
        "blocks",
        BLOCK_FLUSH_BATCH_SIZE,
      );
      const txHashFlusher = new Flusher<TxHashRow>(
        log,
        flushTxHashBatch,
        "tx_hashes",
        LOG_FLUSH_BATCH_SIZE,
      );
      const res = await runStream({
        hypersync,
        chainId: env.CHAIN_ID,
        fromBlock: startBlock,
        toBlock: safeBlock,
        log,
        logFlusher,
        blockFlusher,
        txHashFlusher,
      });
      await logFlusher.waitDrain();
      await blockFlusher.waitDrain();
      await txHashFlusher.waitDrain();

      startBlock = res.nextBlock;
      totalLogs = logFlusher.totalRows;

      log.info(
        {
          fromBlock: startBlock,
          toBlock: safeBlock,
          logsSynced: res.totalLogs,
        },
        "finished streaming",
      );
    } catch (err) {
      log.error(err, "error during sync iteration");
    }
  }
} catch (err) {
  pino().error(err, "fatal error");
  process.exit(1);
}
