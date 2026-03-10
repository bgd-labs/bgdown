import type { createClient } from "@clickhouse/client";
import {
  type Block,
  type HypersyncClient,
  JoinMode,
  type Log,
  type Query,
  type QueryResponse,
} from "@envio-dev/hypersync-client";
import type pino from "pino";
import env from "./env";
import {
  type BlockRow,
  type LogRow,
  serializeBatch,
  serializeBlockBatch,
  serializeTxHashBatch,
  type TxHashRow,
} from "./row-binary";
import { hexBuf } from "./utils/helpers";

export const LOG_FLUSH_BATCH_SIZE = 250_000;
export const BLOCK_FLUSH_BATCH_SIZE = 50_000;
const FLUSH_INTERVAL_MS = 10_000;

function buildTimestampMap(blocks: Block[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const block of blocks) {
    if (block.number !== undefined && block.timestamp !== undefined) {
      map.set(block.number, block.timestamp);
    }
  }
  return map;
}

function logToRow(
  log: Log,
  chainId: number,
  timestamps: Map<number, number>,
): LogRow {
  return {
    chain_id: chainId,
    block_number: log.blockNumber ?? 0,
    timestamp: timestamps.get(log.blockNumber ?? 0) ?? 0,
    transaction_id:
      BigInt(log.blockNumber ?? 0) * 100000n +
      BigInt(log.transactionIndex ?? 0),
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

function blockToRow(block: Block, chainId: number): BlockRow {
  const totalDiffHex = block.totalDifficulty
    ? `0x${block.totalDifficulty.toString(16)}`
    : "0x0";
  return {
    chain_id: chainId,
    number: block.number ?? 0,
    hash: hexBuf(block.hash, 32),
    parent_hash: hexBuf(block.parentHash, 32),
    nonce: block.nonce ?? 0n,
    sha3_uncles: hexBuf(block.sha3Uncles, 32),
    logs_bloom: hexBuf(block.logsBloom, 256),
    transactions_root: hexBuf(block.transactionsRoot, 32),
    state_root: hexBuf(block.stateRoot, 32),
    receipts_root: hexBuf(block.receiptsRoot, 32),
    miner: hexBuf(block.miner, 20),
    difficulty: block.difficulty ?? 0n,
    total_difficulty: Buffer.from(totalDiffHex, "utf8"),
    extra_data: hexBuf(block.extraData, 0),
    size: block.size ?? 0n,
    gas_limit: block.gasLimit ?? 0n,
    gas_used: block.gasUsed ?? 0n,
    timestamp: block.timestamp ?? 0,
    base_fee_per_gas: block.baseFeePerGas ?? null,
    blob_gas_used: block.blobGasUsed ?? null,
    excess_blob_gas: block.excessBlobGas ?? null,
    parent_beacon_block_root: block.parentBeaconBlockRoot
      ? hexBuf(block.parentBeaconBlockRoot, 32)
      : null,
    withdrawals_root: block.withdrawalsRoot
      ? hexBuf(block.withdrawalsRoot, 32)
      : null,
    withdrawals: Buffer.from(JSON.stringify(block.withdrawals ?? []), "utf8"),
    uncles: Buffer.from(JSON.stringify(block.uncles ?? []), "utf8"),
    mix_hash: hexBuf(block.mixHash, 32),
    l1_block_number: block.l1BlockNumber ?? null,
    send_count: block.sendCount ? Buffer.from(block.sendCount, "utf8") : null,
    send_root: block.sendRoot ? hexBuf(block.sendRoot, 32) : null,
  };
}

// @clickhouse/client doesn't expose RowBinary as an insert format, so we
// use the HTTP interface directly. RowBinary is ~2× faster than JSON and
// lets us store hashes/addresses as true binary rather than hex strings.
function flushRowBinary(
  insertSql: string,
  label: string,
): (batch: Buffer, log: pino.Logger) => Promise<void> {
  const url = `${env.CLICKHOUSE_URL}/?query=${encodeURIComponent(`INSERT INTO ${env.CLICKHOUSE_DB}.${insertSql} FORMAT RowBinary`)}`;
  const credentials = btoa(
    `${env.CLICKHOUSE_USERNAME}:${env.CLICKHOUSE_PASSWORD}`,
  );
  return async (data, log) => {
    const res = await fetch(url, {
      method: "POST",
      body: new Uint8Array(data),
      headers: { Authorization: `Basic ${credentials}` },
    });
    if (!res.ok) {
      const body = await res.text();
      log.error(
        { status: res.status, body },
        `ClickHouse ${label} insert failed`,
      );
      throw new Error(
        `ClickHouse ${label} insert failed [${res.status}]: ${body}`,
      );
    }
  };
}

const insertBlock = flushRowBinary("blocks", "block");
const insertLog = flushRowBinary(
  "logs (chain_id, block_number, timestamp, transaction_id, transaction_index, log_index, address, data, topic0, topic1, topic2, topic3, removed)",
  "log",
);
const insertTxHash = flushRowBinary("transaction_hashes", "tx_hash");

export async function flushBlockBatch(
  batch: BlockRow[],
  log: pino.Logger,
): Promise<void> {
  if (batch.length === 0) return;
  await insertBlock(serializeBlockBatch(batch), log);
}

export async function getChainState(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
): Promise<{ startBlock: number; totalLogs: number }> {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block, count() AS total_logs FROM ${env.CLICKHOUSE_DB}.logs WHERE chain_id = ${chainId}`,
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

export async function flushLogBatch(
  batch: LogRow[],
  log: pino.Logger,
): Promise<void> {
  if (batch.length === 0) return;
  await insertLog(serializeBatch(batch), log);
}

export async function flushTxHashBatch(
  batch: TxHashRow[],
  log: pino.Logger,
): Promise<void> {
  if (batch.length === 0) return;
  await insertTxHash(serializeTxHashBatch(batch), log);
}

export class Flusher<T> {
  private batch: T[] = [];
  private lastFlushAt = Date.now();
  private flushPromise: Promise<void> | null = null;
  private flushError: Error | null = null;

  constructor(
    private readonly log: pino.Logger,
    private readonly doFlush: (batch: T[], log: pino.Logger) => Promise<void>,
    private readonly label: string,
    private readonly batchSize: number,
    public totalRows: number = 0,
  ) {}

  async enqueue(rows: T[]) {
    if (this.flushError) throw this.flushError;
    if (rows.length === 0) return;

    this.batch.push(...rows);
    this.totalRows += rows.length;

    const now = Date.now();
    if (
      this.batch.length >= this.batchSize ||
      now - this.lastFlushAt >= FLUSH_INTERVAL_MS
    ) {
      await this.flush();
    }
  }

  private async flush() {
    if (this.batch.length === 0) return;

    if (this.flushPromise) {
      await this.flushPromise;
      if (this.flushError) throw this.flushError;
    }

    const rowsToFlush = this.batch;
    this.batch = [];
    this.lastFlushAt = Date.now();

    const count = rowsToFlush.length;
    this.flushPromise = this.doFlush(rowsToFlush, this.log)
      .then(() => {
        this.log.info(
          { count, total: this.totalRows, kind: this.label },
          "flushed batch",
        );
        this.flushPromise = null;
      })
      .catch((err) => {
        this.flushError = err;
        this.flushPromise = null;
      });
  }

  async waitDrain() {
    if (this.batch.length > 0) {
      await this.flush();
    }
    if (this.flushPromise) {
      await this.flushPromise;
    }
    if (this.flushError) throw this.flushError;
  }
}

type RunStreamConfig = {
  hypersync: HypersyncClient;
  chainId: number;
  fromBlock: number;
  toBlock: number;
  log: pino.Logger;
  logFlusher: Flusher<LogRow>;
  blockFlusher: Flusher<BlockRow>;
  txHashFlusher: Flusher<TxHashRow>;
};

export async function runStream({
  hypersync,
  chainId,
  fromBlock,
  toBlock,
  log,
  logFlusher,
  blockFlusher,
  txHashFlusher,
}: RunStreamConfig): Promise<{ nextBlock: number; totalLogs: number }> {
  const query = {
    fromBlock,
    toBlock,
    includeAllBlocks: true,
    logs: [{ include: {} }],
    fieldSelection: {
      log: [
        "Removed",
        "LogIndex",
        "TransactionIndex",
        "TransactionHash",
        "BlockNumber",
        "Address",
        "Data",
        "Topic0",
        "Topic1",
        "Topic2",
        "Topic3",
      ],
      block: [
        "Number",
        "Hash",
        "ParentHash",
        "Nonce",
        "Sha3Uncles",
        "LogsBloom",
        "TransactionsRoot",
        "StateRoot",
        "ReceiptsRoot",
        "Miner",
        "Difficulty",
        "TotalDifficulty",
        "ExtraData",
        "Size",
        "GasLimit",
        "GasUsed",
        "Timestamp",
        "Uncles",
        "BaseFeePerGas",
        "BlobGasUsed",
        "ExcessBlobGas",
        "ParentBeaconBlockRoot",
        "WithdrawalsRoot",
        "Withdrawals",
        "L1BlockNumber",
        "SendCount",
        "SendRoot",
        "MixHash",
      ],
    },
    joinMode: JoinMode.Default,
  } satisfies Query;

  const receiver = await hypersync.stream(query, {
    concurrency: 20,
  });

  let totalLogs = 0;
  let lastBlock = fromBlock;
  const totalBlocks = toBlock - fromBlock;
  let lastProgressLog = Date.now();

  try {
    while (true) {
      const res: QueryResponse | null = await receiver.recv();

      if (res === null) {
        break;
      }

      lastBlock = res.nextBlock;

      const now = Date.now();
      if (now - lastProgressLog >= 10_000) {
        const blocksProcessed = lastBlock - fromBlock;
        const pct =
          totalBlocks > 0
            ? ((blocksProcessed / totalBlocks) * 100).toFixed(1)
            : "100.0";
        log.info(
          { nextBlock: lastBlock, toBlock, pct: `${pct}%`, totalLogs },
          "progress",
        );
        lastProgressLog = now;
      }

      const timestamps = buildTimestampMap(res.data.blocks);

      // Build log rows and deduplicate tx hashes within this recv() batch.
      const txHashMap = new Map<bigint, TxHashRow>();
      const logBatch: LogRow[] = [];
      for (const l of res.data.logs) {
        const row = logToRow(l, chainId, timestamps);
        logBatch.push(row);
        if (!txHashMap.has(row.transaction_id)) {
          txHashMap.set(row.transaction_id, {
            chain_id: chainId,
            transaction_id: row.transaction_id,
            transaction_hash: hexBuf(l.transactionHash, 32),
          });
        }
      }
      totalLogs += logBatch.length;
      await logFlusher.enqueue(logBatch);
      await txHashFlusher.enqueue(Array.from(txHashMap.values()));

      const blockBatch = res.data.blocks.map((b) => blockToRow(b, chainId));
      await blockFlusher.enqueue(blockBatch);
    }
  } finally {
    await receiver.close();
  }

  log.info(
    { streamTotalLogs: totalLogs, nextBlock: lastBlock },
    "stream finished",
  );

  return { nextBlock: lastBlock, totalLogs };
}
