import { PassThrough } from "node:stream";
import type { createClient } from "@clickhouse/client";
import * as arrow from "apache-arrow";
import {
  Compression,
  Table as WasmTable,
  WriterPropertiesBuilder,
  writeParquet,
} from "parquet-wasm";
import type pino from "pino";
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

const SCHEMA = new arrow.Schema([
  new arrow.Field("chain_id", new arrow.Uint32(), false),
  new arrow.Field("block_number", new arrow.Uint64(), false),
  new arrow.Field("block_hash", new arrow.FixedSizeBinary(32), false),
  new arrow.Field("timestamp", new arrow.Uint32(), false),
  new arrow.Field("transaction_hash", new arrow.FixedSizeBinary(32), false),
  new arrow.Field("transaction_index", new arrow.Uint32(), false),
  new arrow.Field("log_index", new arrow.Uint32(), false),
  new arrow.Field("address", new arrow.FixedSizeBinary(20), false),
  new arrow.Field("data", new arrow.Binary(), false),
  new arrow.Field("topic0", new arrow.FixedSizeBinary(32), false),
  new arrow.Field("topic1", new arrow.FixedSizeBinary(32), true),
  new arrow.Field("topic2", new arrow.FixedSizeBinary(32), true),
  new arrow.Field("topic3", new arrow.FixedSizeBinary(32), true),
  new arrow.Field("removed", new arrow.Uint8(), false),
]);

const WRITER_PROPS = new WriterPropertiesBuilder()
  .setCompression(Compression.UNCOMPRESSED)
  .build();

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
    const n = events.length;

    // Allocate Arrow buffers directly
    const chainIds = new Uint32Array(n).fill(chainId);
    const blockNums = new BigUint64Array(n);
    const blockHashes = new Uint8Array(n * 32);
    const timestamps = new Uint32Array(n);
    const txHashes = new Uint8Array(n * 32);
    const txIndices = new Uint32Array(n);
    const logIndices = new Uint32Array(n);
    const addrs = new Uint8Array(n * 20);
    const topic0 = new Uint8Array(n * 32);
    const topic1 = new Uint8Array(n * 32);
    const topic1Null = new Uint8Array(Math.ceil(n / 8));
    let topic1NullCount = 0;
    const topic2 = new Uint8Array(n * 32);
    const topic2Null = new Uint8Array(Math.ceil(n / 8));
    let topic2NullCount = 0;
    const topic3 = new Uint8Array(n * 32);
    const topic3Null = new Uint8Array(Math.ceil(n / 8));
    let topic3NullCount = 0;
    const removed = new Uint8Array(n);

    // Variable-length data needs two passes
    const dataRefs = new Array<Uint8Array>(n);
    let dataTotal = 0;

    for (let i = 0; i < n; i++) {
      const e = events[i];
      blockNums[i] = e.blockNumber;
      blockHashes.set(e.blockHash, i * 32);
      timestamps[i] = e.timestamp;
      txHashes.set(e.transactionHash, i * 32);
      txIndices[i] = e.transactionIndex;
      logIndices[i] = e.logIndex;
      addrs.set(e.address, i * 20);
      topic0.set(e.topic0, i * 32);

      if (e.topic1) {
        topic1.set(e.topic1, i * 32);
        topic1Null[i >> 3] |= 1 << (i & 7);
      } else {
        topic1NullCount++;
      }
      if (e.topic2) {
        topic2.set(e.topic2, i * 32);
        topic2Null[i >> 3] |= 1 << (i & 7);
      } else {
        topic2NullCount++;
      }
      if (e.topic3) {
        topic3.set(e.topic3, i * 32);
        topic3Null[i >> 3] |= 1 << (i & 7);
      } else {
        topic3NullCount++;
      }

      removed[i] = e.removed;
      dataRefs[i] = e.data;
      dataTotal += e.data.length;
    }

    // Pack variable-length data column
    const dataOffsets = new Int32Array(n + 1);
    const dataBytes = new Uint8Array(dataTotal);
    let offset = 0;
    for (let i = 0; i < n; i++) {
      dataOffsets[i] = offset;
      dataBytes.set(dataRefs[i], offset);
      offset += dataRefs[i].length;
    }
    dataOffsets[n] = offset;

    const t0 = performance.now();

    const batch = new arrow.RecordBatch(
      SCHEMA,
      arrow.makeData({
        type: new arrow.Struct(SCHEMA.fields),
        length: n,
        children: [
          arrow.makeData({
            type: new arrow.Uint32(),
            data: chainIds,
          }),
          arrow.makeData({
            type: new arrow.Uint64(),
            data: blockNums,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            data: blockHashes,
          }),
          arrow.makeData({
            type: new arrow.Uint32(),
            data: timestamps,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            data: txHashes,
          }),
          arrow.makeData({
            type: new arrow.Uint32(),
            data: txIndices,
          }),
          arrow.makeData({
            type: new arrow.Uint32(),
            data: logIndices,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(20),
            data: addrs,
          }),
          arrow.makeData({
            type: new arrow.Binary(),
            valueOffsets: dataOffsets,
            data: dataBytes,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            data: topic0,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            nullCount: topic1NullCount,
            nullBitmap: topic1Null,
            data: topic1,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            nullCount: topic2NullCount,
            nullBitmap: topic2Null,
            data: topic2,
          }),
          arrow.makeData({
            type: new arrow.FixedSizeBinary(32),
            nullCount: topic3NullCount,
            nullBitmap: topic3Null,
            data: topic3,
          }),
          arrow.makeData({ type: new arrow.Uint8(), length: n, data: removed }),
        ],
      }),
    );

    const ipc = arrow.tableToIPC(new arrow.Table(batch), "stream");
    const parquetBytes = writeParquet(
      WasmTable.fromIPCStream(ipc),
      WRITER_PROPS,
    );

    logger.info({ ms: (performance.now() - t0).toFixed(1) }, "build parquet");

    const buf = Buffer.from(parquetBytes);
    logger.info(
      `Sending ${n} events (${(buf.length / 1024).toFixed(2)} KB) to ClickHouse...`,
    );

    const stream = new PassThrough();
    stream.end(buf);

    const t1 = performance.now();
    await clickhouse.insert({
      table: "logs",
      values: stream,
      format: "Parquet",
      clickhouse_settings: {
        async_insert: 1,
        wait_for_async_insert: 1,
      },
    });
    logger.info(
      { ms: (performance.now() - t1).toFixed(1) },
      "clickhouse insert",
    );
  };
}
