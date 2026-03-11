import type { createClient } from "@clickhouse/client";
import {
  type EventResponse,
  type HypersyncClient,
  JoinMode,
  type Query,
} from "@envio-dev/hypersync-client";
import type pino from "pino";

export async function getChainState(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
) {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block, count() AS total_logs FROM logs WHERE chain_id = ${chainId}`,
    format: "JSONEachRow",
  });
  const rows = await result.json<{ max_block: string; total_logs: string }>();
  // Re-include the last indexed block in case the process crashed mid-block.
  // ReplacingMergeTree deduplicates any overlapping rows on merge.
  return {
    startBlock: Number(rows[0]?.max_block ?? 0),
    totalLogs: Number(rows[0]?.total_logs ?? 0),
  };
}

export async function runStream({
  hypersync,
  fromBlock,
  toBlock,
  logger,
  onEvents,
}: {
  hypersync: HypersyncClient;
  fromBlock: number;
  toBlock: number;
  logger: pino.Logger;
  onEvents: (events: EventResponse["data"]) => Promise<void>;
}) {
  const query = {
    fromBlock,
    toBlock,
    logs: [{ include: {} }],
    fieldSelection: {
      log: [
        "Removed",
        "LogIndex",
        "TransactionIndex",
        "TransactionHash",
        "BlockNumber",
        "BlockHash",
        "Address",
        "Data",
        "Topic0",
        "Topic1",
        "Topic2",
        "Topic3",
      ],
      block: ["Number", "Timestamp"],
    },
    joinMode: JoinMode.Default,
  } satisfies Query;

  const receiver = await hypersync.streamEvents(query, {
    hexOutput: "NoEncode",
    concurrency: 20,
  });

  let totalLogs = 0;

  while (true) {
    const res = await receiver.recv();
    if (res === null) {
      break;
    }

    totalLogs += res.data.length;

    logger.info(
      {
        progess: `block ${res.nextBlock - 1} of ${res.archiveHeight} (${Math.round((res.nextBlock / (res.archiveHeight ?? 0)) * 10000) / 100}%)`,
        chunkSize: res.data.length,
        totalLogs,
      },
      "received logs",
    );

    await onEvents(res.data);
  }

  return {
    nextBlock: toBlock + 1,
    totalLogs,
  };
}
