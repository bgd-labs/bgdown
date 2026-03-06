import { createClient } from "@clickhouse/client";
import {
  type Block,
  HypersyncClient,
  JoinMode,
  type Log,
  type QueryResponse,
} from "@envio-dev/hypersync-client";

const HYPERSYNC_URL = "https://eth.hypersync.xyz";
const HYPERSYNC_API_KEY = "b5c5baee-7507-451c-bcfb-f0d1e790a5ab";
const CHAIN_ID = 1; // Ethereum mainnet

const CLICKHOUSE_URL = "http://localhost:8123";
const CLICKHOUSE_DB = "ethereum";

const FLUSH_BATCH_SIZE = 500_000;
const FLUSH_INTERVAL_MS = 30_000;

// Seconds to wait before re-checking the chain tip after catching up.
const POLL_INTERVAL_SECS = 10;

interface LogRow {
  chain_id: number;
  block_number: number;
  block_hash: Buffer;        // FixedString(32) — raw 32 bytes
  timestamp: number;
  transaction_hash: Buffer;  // FixedString(32) — raw 32 bytes
  transaction_index: number;
  log_index: number;
  address: Buffer;           // FixedString(20) — raw 20 bytes
  data: Buffer;              // String — raw ABI bytes
  topic0: Buffer;            // FixedString(32) — raw 32 bytes
  topic1: Buffer | null;     // Nullable(FixedString(32))
  topic2: Buffer | null;
  topic3: Buffer | null;
  removed: number;
}

function buildTimestampMap(blocks: Block[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const block of blocks) {
    if (block.number !== undefined && block.timestamp !== undefined) {
      map.set(block.number, block.timestamp);
    }
  }
  return map;
}

// Convert a 0x-prefixed hex string to a fixed-length Buffer of `len` bytes.
// Returns a zero-filled buffer for missing/empty values.
function hexBuf(hex: string | null | undefined, len: number): Buffer {
  if (!hex || hex.length < 3) return Buffer.alloc(len);
  return Buffer.from(hex.slice(2), "hex");
}

function logToRow(
  log: Log,
  chainId: number,
  timestamps: Map<number, number>,
): LogRow {
  return {
    chain_id: chainId,
    block_number: log.blockNumber ?? 0,
    block_hash: hexBuf(log.blockHash, 32),
    timestamp: timestamps.get(log.blockNumber ?? 0) ?? 0,
    transaction_hash: hexBuf(log.transactionHash, 32),
    transaction_index: log.transactionIndex ?? 0,
    log_index: log.logIndex ?? 0,
    address: hexBuf(log.address, 20),
    data: hexBuf(log.data, 0),
    topic0: hexBuf(log.topics[0], 32),
    topic1: log.topics[1] ? hexBuf(log.topics[1], 32) : null,
    topic2: log.topics[2] ? hexBuf(log.topics[2], 32) : null,
    topic3: log.topics[3] ? hexBuf(log.topics[3], 32) : null,
    removed: log.removed ? 1 : 0,
  };
}

// LEB128 variable-length unsigned integer (used by ClickHouse RowBinary for String lengths).
function varUInt(n: number): Buffer {
  const bytes: number[] = [];
  while (n > 0x7f) {
    bytes.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  bytes.push(n);
  return Buffer.from(bytes);
}

// Serialise a batch of rows into a single RowBinary buffer.
// Column order must match the schema exactly.
function serializeBatch(rows: LogRow[]): Buffer {
  const parts: Buffer[] = [];
  for (const row of rows) {
    const u32 = Buffer.allocUnsafe(4);
    u32.writeUInt32LE(row.chain_id);
    parts.push(u32);

    const u64 = Buffer.allocUnsafe(8);
    u64.writeBigUInt64LE(BigInt(row.block_number));
    parts.push(u64);

    parts.push(row.block_hash);

    const ts = Buffer.allocUnsafe(4);
    ts.writeUInt32LE(row.timestamp);
    parts.push(ts);

    parts.push(row.transaction_hash);

    const ti = Buffer.allocUnsafe(4);
    ti.writeUInt32LE(row.transaction_index);
    parts.push(ti);

    const li = Buffer.allocUnsafe(4);
    li.writeUInt32LE(row.log_index);
    parts.push(li);

    parts.push(row.address);

    // String: varint(len) + bytes
    parts.push(varUInt(row.data.length), row.data);

    parts.push(row.topic0);

    for (const topic of [row.topic1, row.topic2, row.topic3]) {
      if (topic === null) {
        parts.push(Buffer.from([1])); // null flag
      } else {
        parts.push(Buffer.from([0]), topic); // not-null flag + bytes
      }
    }

    parts.push(Buffer.from([row.removed]));
  }
  return Buffer.concat(parts);
}

async function getStartBlock(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
): Promise<number> {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block FROM ethereum.logs WHERE chain_id = ${chainId}`,
    format: "JSONEachRow",
  });
  const rows = await result.json<{ max_block: string }>();
  const maxBlock = Number(rows[0]?.max_block ?? 0);
  // Re-include the last indexed block in case the process crashed mid-block.
  // ReplacingMergeTree deduplicates any overlapping rows on merge.
  return maxBlock;
}

async function flushBatch(batch: LogRow[]): Promise<void> {
  if (batch.length === 0) return;
  const data = serializeBatch(batch);
  // @clickhouse/client doesn't expose RowBinary as an insert format, so we
  // use the HTTP interface directly. RowBinary is ~2× faster than JSON and
  // lets us store hashes/addresses as true binary rather than hex strings.
  const url = `${CLICKHOUSE_URL}/?query=${encodeURIComponent(`INSERT INTO ${CLICKHOUSE_DB}.logs FORMAT RowBinary`)}`;
  const res = await fetch(url, { method: "POST", body: new Uint8Array(data) });
  if (!res.ok) {
    throw new Error(`ClickHouse insert failed [${res.status}]: ${await res.text()}`);
  }
}

async function runStream(
  hypersync: HypersyncClient,
  chainId: number,
  fromBlock: number,
): Promise<number> {
  const query = {
    fromBlock,
    // Empty LogFilter matches every log on the chain.
    logs: [{}],
    fieldSelection: {
      log: [
        "Removed" as const,
        "LogIndex" as const,
        "TransactionIndex" as const,
        "TransactionHash" as const,
        "BlockHash" as const,
        "BlockNumber" as const,
        "Address" as const,
        "Data" as const,
        "Topic0" as const,
        "Topic1" as const,
        "Topic2" as const,
        "Topic3" as const,
      ],
      block: ["Number" as const, "Timestamp" as const],
    },
    joinMode: JoinMode.Default,
  };

  const receiver = await hypersync.stream(query, {});

  let batch: LogRow[] = [];
  let totalLogs = 0;
  let lastBlock = fromBlock;
  let lastFlushAt = Date.now();

  const flush = async () => {
    await flushBatch(batch);
    totalLogs += batch.length;
    batch = [];
    lastFlushAt = Date.now();
    console.log(
      `[${new Date().toISOString()}] Inserted ${totalLogs.toLocaleString()} logs total, next block: ${lastBlock.toLocaleString()}`,
    );
  };

  try {
    while (true) {
      const res: QueryResponse | null = await receiver.recv();

      if (res === null) {
        // Stream exhausted – we have reached the chain tip at stream start.
        await flush();
        break;
      }

      lastBlock = res.nextBlock;

      const timestamps = buildTimestampMap(res.data.blocks);
      for (const log of res.data.logs) {
        batch.push(logToRow(log, chainId, timestamps));
      }

      const elapsed = Date.now() - lastFlushAt;
      if (batch.length >= FLUSH_BATCH_SIZE || elapsed >= FLUSH_INTERVAL_MS) {
        await flush();
      }
    }
  } finally {
    // Always close the stream to release server-side resources.
    await receiver.close();
  }

  console.log(
    `[${new Date().toISOString()}] Stream finished. Inserted ${totalLogs.toLocaleString()} logs this run, next block: ${lastBlock.toLocaleString()}`,
  );

  return lastBlock;
}

async function main(): Promise<void> {
  const clickhouse = createClient({
    url: CLICKHOUSE_URL,
    username: "default",
    password: "",
    database: CLICKHOUSE_DB,
    clickhouse_settings: {
      async_insert: 1,
      wait_for_async_insert: 0,
    },
  });

  const hypersync = new HypersyncClient({
    url: HYPERSYNC_URL,
    apiToken: HYPERSYNC_API_KEY,
  });

  console.log("Connected. Checking last indexed block…");

  let startBlock = await getStartBlock(clickhouse, CHAIN_ID);
  console.log(`Resuming from block ${startBlock.toLocaleString()}`);

  // Continuous loop: stream until chain tip, then poll for new blocks.
  while (true) {
    startBlock = await runStream(hypersync, CHAIN_ID, startBlock);
    console.log(
      `Caught up to chain tip. Polling again in ${POLL_INTERVAL_SECS}s…`,
    );
    await Bun.sleep(POLL_INTERVAL_SECS * 1000);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
