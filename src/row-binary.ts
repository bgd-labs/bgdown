export interface LogRow {
  chain_id: number;
  block_number: number;
  timestamp: number;
  transaction_id: bigint; // UInt64 = block_number * 100000 + transaction_index
  transaction_index: number;
  log_index: number;
  address: Buffer; // FixedString(20) — raw 20 bytes
  data: Buffer; // String — raw ABI bytes
  topic0: Buffer; // FixedString(32) — raw 32 bytes
  topic1: Buffer | null; // Nullable(FixedString(32))
  topic2: Buffer | null;
  topic3: Buffer | null;
  removed: number;
}

export interface TxHashRow {
  chain_id: number;
  transaction_id: bigint; // UInt64
  transaction_hash: Buffer; // FixedString(32)
}

export interface BlockRow {
  chain_id: number;
  number: number;
  hash: Buffer; // FixedString(32)
  parent_hash: Buffer; // FixedString(32)
  nonce: bigint; // UInt64
  sha3_uncles: Buffer; // FixedString(32)
  logs_bloom: Buffer; // FixedString(256)
  transactions_root: Buffer; // FixedString(32)
  state_root: Buffer; // FixedString(32)
  receipts_root: Buffer; // FixedString(32)
  miner: Buffer; // FixedString(20)
  difficulty: bigint; // UInt64
  total_difficulty: Buffer; // String — hex representation
  extra_data: Buffer; // String — raw bytes
  size: bigint; // UInt64
  gas_limit: bigint; // UInt64
  gas_used: bigint; // UInt64
  timestamp: number; // UInt32
  base_fee_per_gas: bigint | null; // Nullable(UInt64)
  blob_gas_used: bigint | null; // Nullable(UInt64)
  excess_blob_gas: bigint | null; // Nullable(UInt64)
  parent_beacon_block_root: Buffer | null; // Nullable(FixedString(32))
  withdrawals_root: Buffer | null; // Nullable(FixedString(32))
  withdrawals: Buffer; // String — JSON
  uncles: Buffer; // String — JSON
  mix_hash: Buffer; // FixedString(32)
  l1_block_number: number | null; // Nullable(UInt64)
  send_count: Buffer | null; // Nullable(String)
  send_root: Buffer | null; // Nullable(FixedString(32))
}

// Writes ClickHouse RowBinary format into a single pre-allocated buffer.
// Grows automatically if the estimate is too small; in practice the initial
// estimate is generous enough that no reallocation occurs.
function createWriter(estimatedSize: number) {
  let buf = Buffer.allocUnsafe(estimatedSize);
  let off = 0;

  function grow(needed: number) {
    const newBuf = Buffer.allocUnsafe(Math.max(buf.length * 2, off + needed));
    buf.copy(newBuf);
    buf = newBuf;
  }

  function uint8(v: number) {
    if (off + 1 > buf.length) grow(1);
    buf[off++] = v;
  }

  function uint32(v: number) {
    if (off + 4 > buf.length) grow(4);
    buf.writeUInt32LE(v, off);
    off += 4;
  }

  function uint64(v: bigint) {
    if (off + 8 > buf.length) grow(8);
    buf.writeUInt32LE(Number(v & 0xffffffffn), off);
    buf.writeUInt32LE(Number((v >> 32n) & 0xffffffffn), off + 4);
    off += 8;
  }

  // UInt64 from a number that fits in UInt32 (high 32 bits are zero).
  function uint64n(v: number) {
    if (off + 8 > buf.length) grow(8);
    buf.writeUInt32LE(v, off);
    buf.writeUInt32LE(0, off + 4);
    off += 8;
  }

  function varUInt(n: number) {
    // Max 5 bytes for a 32-bit LEB128.
    if (off + 5 > buf.length) grow(5);
    while (n > 0x7f) {
      buf[off++] = (n & 0x7f) | 0x80;
      n >>>= 7;
    }
    buf[off++] = n;
  }

  // FixedString(N) — copies exactly src.length bytes.
  function fixed(src: Buffer) {
    const len = src.length;
    if (off + len > buf.length) grow(len);
    src.copy(buf, off);
    off += len;
  }

  // ClickHouse String — LEB128 length prefix followed by raw bytes.
  function string(src: Buffer) {
    varUInt(src.length);
    fixed(src);
  }

  function nullableUint64(v: bigint | null) {
    if (v === null) {
      uint8(1);
    } else {
      uint8(0);
      uint64(v);
    }
  }

  function nullableUint64n(v: number | null) {
    if (v === null) {
      uint8(1);
    } else {
      uint8(0);
      uint64n(v);
    }
  }

  function nullableFixed(src: Buffer | null) {
    if (src === null) {
      uint8(1);
    } else {
      uint8(0);
      fixed(src);
    }
  }

  function nullableString(src: Buffer | null) {
    if (src === null) {
      uint8(1);
    } else {
      uint8(0);
      string(src);
    }
  }

  function result(): Buffer {
    return buf.subarray(0, off);
  }

  return {
    uint8,
    uint32,
    uint64,
    uint64n,
    fixed,
    string,
    nullableUint64,
    nullableUint64n,
    nullableFixed,
    nullableString,
    result,
  };
}

export function serializeBatch(rows: LogRow[]): Buffer {
  const w = createWriter(rows.length * 200);
  for (const row of rows) {
    w.uint32(row.chain_id);
    w.uint64n(row.block_number);
    w.uint32(row.timestamp);
    w.uint64(row.transaction_id);
    w.uint32(row.transaction_index);
    w.uint32(row.log_index);
    w.fixed(row.address);
    w.string(row.data);
    w.fixed(row.topic0);
    w.nullableFixed(row.topic1);
    w.nullableFixed(row.topic2);
    w.nullableFixed(row.topic3);
    w.uint8(row.removed);
  }
  return w.result();
}

export function serializeBlockBatch(rows: BlockRow[]): Buffer {
  const w = createWriter(rows.length * 1000);
  for (const row of rows) {
    w.uint32(row.chain_id);
    w.uint64n(row.number);
    w.fixed(row.hash);
    w.fixed(row.parent_hash);
    w.uint64(row.nonce);
    w.fixed(row.sha3_uncles);
    w.fixed(row.logs_bloom);
    w.fixed(row.transactions_root);
    w.fixed(row.state_root);
    w.fixed(row.receipts_root);
    w.fixed(row.miner);
    w.uint64(row.difficulty);
    w.string(row.total_difficulty);
    w.string(row.extra_data);
    w.uint64(row.size);
    w.uint64(row.gas_limit);
    w.uint64(row.gas_used);
    w.uint32(row.timestamp);
    w.nullableUint64(row.base_fee_per_gas);
    w.nullableUint64(row.blob_gas_used);
    w.nullableUint64(row.excess_blob_gas);
    w.nullableFixed(row.parent_beacon_block_root);
    w.nullableFixed(row.withdrawals_root);
    w.string(row.withdrawals);
    w.string(row.uncles);
    w.fixed(row.mix_hash);
    w.nullableUint64n(row.l1_block_number);
    w.nullableString(row.send_count);
    w.nullableFixed(row.send_root);
  }
  return w.result();
}

export function serializeTxHashBatch(rows: TxHashRow[]): Buffer {
  const w = createWriter(rows.length * 44);
  for (const row of rows) {
    w.uint32(row.chain_id);
    w.uint64(row.transaction_id);
    w.fixed(row.transaction_hash);
  }
  return w.result();
}
