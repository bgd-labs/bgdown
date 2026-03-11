import { PassThrough } from "node:stream";
import type { createClient } from "@clickhouse/client";
import type pino from "pino";
import { writeParquet } from "tiny-parquet";
import type { parseHyperSyncResponse } from "../validator.ts";

export function hexCol(col: string, alias?: string): string {
  return `concat('0x', lower(hex(${col}))) AS ${alias ?? `${col}_hex`}`;
}

export function nullableHexCol(col: string, alias?: string): string {
  return `if(isNull(${col}), NULL, concat('0x', lower(hex(assumeNotNull(${col}))))) AS ${alias ?? `${col}_hex`}`;
}

export function select(...cols: string[]): string {
  return `\n  ${cols.join(",\n  ")}\n`;
}

export function createDbWriter({
  clickhouse,
  logger,
  chainId,
}: {
  clickhouse: ReturnType<typeof createClient>;
  logger: pino.Logger;
  chainId: number;
}) {
  return async (events: ReturnType<typeof parseHyperSyncResponse>) => {
    const count = events.length;
    const cols = {
      chain_id: new Array(count).fill(chainId),
      block_number: new Array(count),
      block_hash: new Array(count),
      timestamp: new Array(count),
      transaction_hash: new Array(count),
      transaction_index: new Array(count),
      log_index: new Array(count),
      address: new Array(count),
      data: new Array(count),
      topic0: new Array(count),
      topic1: new Array(count),
      topic2: new Array(count),
      topic3: new Array(count),
      removed: new Array(count),
    };

    for (let i = 0; i < count; i++) {
      const event = events[i];
      cols.block_number[i] = event.blockNumber;
      cols.block_hash[i] = event.blockHash;
      cols.timestamp[i] = event.timestamp;
      cols.transaction_hash[i] = event.transactionHash;
      cols.transaction_index[i] = event.transactionIndex;
      cols.log_index[i] = event.logIndex;
      cols.address[i] = event.address;
      cols.data[i] = event.data;
      cols.topic0[i] = event.topic0;
      cols.topic1[i] = event.topic1 ?? null;
      cols.topic2[i] = event.topic2 ?? null;
      cols.topic3[i] = event.topic3 ?? null;
      cols.removed[i] = event.removed;
    }

    console.time("build parquet");
    const arrayBuffer = await writeParquet(
      [
        { name: "chain_id", type: "int32" },
        { name: "block_number", type: "int64" },
        { name: "block_hash", type: "string" },
        { name: "timestamp", type: "int32" },
        { name: "transaction_hash", type: "string" },
        { name: "transaction_index", type: "int32" },
        { name: "log_index", type: "int32" },
        { name: "address", type: "string" },
        { name: "data", type: "string" },
        { name: "topic0", type: "string" },
        { name: "topic1", type: "string" },
        { name: "topic2", type: "string" },
        { name: "topic3", type: "string" },
        { name: "removed", type: "int32" },
      ],
      cols,
      { compression: "snappy" },
    );
    console.timeEnd("build parquet");

    // Wrap the ArrayBuffer in a Node Buffer
    const memoryBuffer = Buffer.from(arrayBuffer);
    logger.info(
      `Sending ${events.length} events (${(memoryBuffer.length / 1024).toFixed(2)} KB) to ClickHouse...`,
    );

    const stream = new PassThrough();
    stream.end(memoryBuffer);

    console.time("clickhouse insert");
    await clickhouse.insert({
      table: "logs",
      values: stream,
      format: "Parquet",
      clickhouse_settings: {
        async_insert: 1,
        wait_for_async_insert: 1,
      },
    });
    console.timeEnd("clickhouse insert");
  };
}
