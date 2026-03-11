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

function fixedBinary(
  values: (Uint8Array | undefined)[],
  byteWidth: number,
  nullable = false,
): arrow.Data {
  const count = values.length;
  const data = new Uint8Array(count * byteWidth);
  let nullBitmap: Uint8Array | undefined;
  let nullCount = 0;

  if (nullable) {
    nullBitmap = new Uint8Array(Math.ceil(count / 8));
    for (let i = 0; i < count; i++) {
      const v = values[i];
      if (v) {
        data.set(v, i * byteWidth);
        nullBitmap[i >> 3] |= 1 << (i & 7);
      } else {
        nullCount++;
      }
    }
  } else {
    for (let i = 0; i < count; i++) {
      data.set(values[i]!, i * byteWidth);
    }
  }

  return arrow.makeData({
    type: new arrow.FixedSizeBinary(byteWidth),
    length: count,
    nullCount,
    nullBitmap,
    data,
  });
}

function varBinary(values: Uint8Array[]): arrow.Data {
  const count = values.length;
  const valueOffsets = new Int32Array(count + 1);
  let total = 0;
  for (let i = 0; i < count; i++) {
    valueOffsets[i] = total;
    total += values[i].length;
  }
  valueOffsets[count] = total;

  const data = new Uint8Array(total);
  for (let i = 0; i < count; i++) {
    data.set(values[i], valueOffsets[i]);
  }

  return arrow.makeData({
    type: new arrow.Binary(),
    length: count,
    valueOffsets,
    data,
  });
}

const u8 = (data: Uint8Array, n: number) =>
  arrow.makeData({ type: new arrow.Uint8(), length: n, data });
const u32 = (data: Uint32Array, n: number) =>
  arrow.makeData({ type: new arrow.Uint32(), length: n, data });
const u64 = (data: BigUint64Array, n: number) =>
  arrow.makeData({ type: new arrow.Uint64(), length: n, data });

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

    const chainIds = new Uint32Array(n).fill(chainId);
    const blockNums = new BigUint64Array(n);
    const timestamps = new Uint32Array(n);
    const txIndices = new Uint32Array(n);
    const logIndices = new Uint32Array(n);
    const removed = new Uint8Array(n);
    const blockHashes = new Array<Uint8Array>(n);
    const txHashes = new Array<Uint8Array>(n);
    const addrs = new Array<Uint8Array>(n);
    const dataCol = new Array<Uint8Array>(n);
    const t0s = new Array<Uint8Array>(n);
    const t1s = new Array<Uint8Array | undefined>(n);
    const t2s = new Array<Uint8Array | undefined>(n);
    const t3s = new Array<Uint8Array | undefined>(n);

    for (let i = 0; i < n; i++) {
      const e = events[i];
      blockNums[i] = e.blockNumber;
      timestamps[i] = e.timestamp!;
      txIndices[i] = e.transactionIndex!;
      logIndices[i] = e.logIndex!;
      removed[i] = e.removed;
      blockHashes[i] = e.blockHash;
      txHashes[i] = e.transactionHash;
      addrs[i] = e.address;
      dataCol[i] = e.data;
      t0s[i] = e.topic0;
      t1s[i] = e.topic1;
      t2s[i] = e.topic2;
      t3s[i] = e.topic3;
    }

    console.time("build parquet");

    const batch = new arrow.RecordBatch(
      SCHEMA,
      arrow.makeData({
        type: new arrow.Struct(SCHEMA.fields),
        length: n,
        children: [
          u32(chainIds, n),
          u64(blockNums, n),
          fixedBinary(blockHashes, 32),
          u32(timestamps, n),
          fixedBinary(txHashes, 32),
          u32(txIndices, n),
          u32(logIndices, n),
          fixedBinary(addrs, 20),
          varBinary(dataCol),
          fixedBinary(t0s, 32),
          fixedBinary(t1s, 32, true),
          fixedBinary(t2s, 32, true),
          fixedBinary(t3s, 32, true),
          u8(removed, n),
        ],
      }),
    );

    const ipc = arrow.tableToIPC(new arrow.Table(batch), "stream");
    const parquetBytes = writeParquet(
      WasmTable.fromIPCStream(ipc),
      WRITER_PROPS,
    );

    console.timeEnd("build parquet");

    const buf = Buffer.from(parquetBytes);
    logger.info(
      `Sending ${n} events (${(buf.length / 1024).toFixed(2)} KB) to ClickHouse...`,
    );

    const stream = new PassThrough();
    stream.end(buf);

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
