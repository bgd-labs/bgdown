// ── Row types ───────────────────────────────────────────────────────────────

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

// ── Helpers ─────────────────────────────────────────────────────────────────

// Convert a 0x-prefixed hex string to a fixed-length Buffer of `len` bytes.
// Returns a zero-filled buffer for missing/empty values.
export function hexBuf(hex: string | null | undefined, len: number): Buffer {
  if (!hex || hex.length < 3) return Buffer.alloc(len);
  return Buffer.from(hex.slice(2), "hex");
}

// Returns the number of bytes needed to encode n as LEB128.
function varUIntSize(n: number): number {
  if (n === 0) return 1;
  let size = 0;
  let v = n;
  while (v > 0) {
    v >>>= 7;
    size++;
  }
  return size;
}

// Writes n as LEB128 into buf at offset and returns the new offset.
function writeVarUInt(buf: Buffer, n: number, offset: number): number {
  while (n > 0x7f) {
    buf[offset++] = (n & 0x7f) | 0x80;
    n >>>= 7;
  }
  buf[offset++] = n;
  return offset;
}

// Write a bigint as UInt64 LE using two UInt32 writes for compatibility.
function writeUInt64LE(buf: Buffer, value: bigint, offset: number): number {
  buf.writeUInt32LE(Number(value & 0xffffffffn), offset);
  buf.writeUInt32LE(Number((value >> 32n) & 0xffffffffn), offset + 4);
  return offset + 8;
}

// ── Serialization ───────────────────────────────────────────────────────────

// Serialise a batch of rows into a single RowBinary buffer.
// Pre-computes the exact size to avoid O(rows × 20) small Buffer allocations
// and the subsequent Buffer.concat() over millions of entries.
export function serializeBatch(rows: LogRow[]): Buffer {
  // First pass: compute total byte count.
  let size = 0;
  for (const row of rows) {
    size += 4; // chain_id UInt32
    size += 8; // block_number UInt64
    size += 4; // timestamp UInt32
    size += 8; // transaction_id UInt64
    size += 4; // transaction_index UInt32
    size += 4; // log_index UInt32
    size += 20; // address FixedString(20)
    size += varUIntSize(row.data.length) + row.data.length; // data String
    size += 32; // topic0 FixedString(32)
    size += 1 + (row.topic1 !== null ? 32 : 0); // topic1 Nullable(FixedString(32))
    size += 1 + (row.topic2 !== null ? 32 : 0); // topic2
    size += 1 + (row.topic3 !== null ? 32 : 0); // topic3
    size += 1; // removed UInt8
  }

  const buf = Buffer.allocUnsafe(size);
  let off = 0;

  for (const row of rows) {
    buf.writeUInt32LE(row.chain_id, off);
    off += 4;
    // Write UInt64 as two LE UInt32s. Block numbers fit in UInt32 for the
    // foreseeable future (Ethereum is at ~22M; max UInt32 is ~4.3B).
    buf.writeUInt32LE(row.block_number, off);
    off += 4;
    buf.writeUInt32LE(0, off);
    off += 4;
    buf.writeUInt32LE(row.timestamp, off);
    off += 4;
    off = writeUInt64LE(buf, row.transaction_id, off);
    buf.writeUInt32LE(row.transaction_index, off);
    off += 4;
    buf.writeUInt32LE(row.log_index, off);
    off += 4;
    row.address.copy(buf, off);
    off += 20;
    off = writeVarUInt(buf, row.data.length, off);
    row.data.copy(buf, off);
    off += row.data.length;
    row.topic0.copy(buf, off);
    off += 32;
    for (const topic of [row.topic1, row.topic2, row.topic3] as const) {
      if (topic === null) {
        buf[off++] = 1; // null flag
      } else {
        buf[off++] = 0; // non-null flag
        topic.copy(buf, off);
        off += 32;
      }
    }
    buf[off++] = row.removed;
  }

  return buf;
}

export function serializeBlockBatch(rows: BlockRow[]): Buffer {
  // First pass: compute total byte count.
  let size = 0;
  for (const row of rows) {
    size += 4; // chain_id UInt32
    size += 8; // number UInt64
    size += 32; // hash FixedString(32)
    size += 32; // parent_hash FixedString(32)
    size += 8; // nonce UInt64
    size += 32; // sha3_uncles FixedString(32)
    size += 256; // logs_bloom FixedString(256)
    size += 32; // transactions_root FixedString(32)
    size += 32; // state_root FixedString(32)
    size += 32; // receipts_root FixedString(32)
    size += 20; // miner FixedString(20)
    size += 8; // difficulty UInt64
    size +=
      varUIntSize(row.total_difficulty.length) + row.total_difficulty.length; // total_difficulty String
    size += varUIntSize(row.extra_data.length) + row.extra_data.length; // extra_data String
    size += 8; // size UInt64
    size += 8; // gas_limit UInt64
    size += 8; // gas_used UInt64
    size += 4; // timestamp UInt32
    size += 1 + (row.base_fee_per_gas !== null ? 8 : 0); // Nullable(UInt64)
    size += 1 + (row.blob_gas_used !== null ? 8 : 0);
    size += 1 + (row.excess_blob_gas !== null ? 8 : 0);
    size += 1 + (row.parent_beacon_block_root !== null ? 32 : 0); // Nullable(FixedString(32))
    size += 1 + (row.withdrawals_root !== null ? 32 : 0);
    size += varUIntSize(row.withdrawals.length) + row.withdrawals.length; // withdrawals String
    size += varUIntSize(row.uncles.length) + row.uncles.length; // uncles String
    size += 32; // mix_hash FixedString(32)
    size += 1 + (row.l1_block_number !== null ? 8 : 0); // Nullable(UInt64)
    size +=
      1 +
      (row.send_count !== null
        ? varUIntSize(row.send_count.length) + row.send_count.length
        : 0); // Nullable(String)
    size += 1 + (row.send_root !== null ? 32 : 0); // Nullable(FixedString(32))
  }

  const buf = Buffer.allocUnsafe(size);
  let off = 0;

  for (const row of rows) {
    buf.writeUInt32LE(row.chain_id, off);
    off += 4;
    // number UInt64
    buf.writeUInt32LE(row.number, off);
    off += 4;
    buf.writeUInt32LE(0, off);
    off += 4;
    row.hash.copy(buf, off);
    off += 32;
    row.parent_hash.copy(buf, off);
    off += 32;
    off = writeUInt64LE(buf, row.nonce, off);
    row.sha3_uncles.copy(buf, off);
    off += 32;
    row.logs_bloom.copy(buf, off);
    off += 256;
    row.transactions_root.copy(buf, off);
    off += 32;
    row.state_root.copy(buf, off);
    off += 32;
    row.receipts_root.copy(buf, off);
    off += 32;
    row.miner.copy(buf, off);
    off += 20;
    off = writeUInt64LE(buf, row.difficulty, off);
    off = writeVarUInt(buf, row.total_difficulty.length, off);
    row.total_difficulty.copy(buf, off);
    off += row.total_difficulty.length;
    off = writeVarUInt(buf, row.extra_data.length, off);
    row.extra_data.copy(buf, off);
    off += row.extra_data.length;
    off = writeUInt64LE(buf, row.size, off);
    off = writeUInt64LE(buf, row.gas_limit, off);
    off = writeUInt64LE(buf, row.gas_used, off);
    buf.writeUInt32LE(row.timestamp, off);
    off += 4;
    // Nullable UInt64 fields
    for (const val of [
      row.base_fee_per_gas,
      row.blob_gas_used,
      row.excess_blob_gas,
    ] as const) {
      if (val === null) {
        buf[off++] = 1;
      } else {
        buf[off++] = 0;
        off = writeUInt64LE(buf, val, off);
      }
    }
    // Nullable FixedString(32) fields
    for (const val of [
      row.parent_beacon_block_root,
      row.withdrawals_root,
    ] as const) {
      if (val === null) {
        buf[off++] = 1;
      } else {
        buf[off++] = 0;
        val.copy(buf, off);
        off += 32;
      }
    }
    // withdrawals String (JSON)
    off = writeVarUInt(buf, row.withdrawals.length, off);
    row.withdrawals.copy(buf, off);
    off += row.withdrawals.length;
    // uncles String (JSON)
    off = writeVarUInt(buf, row.uncles.length, off);
    row.uncles.copy(buf, off);
    off += row.uncles.length;
    // mix_hash FixedString(32)
    row.mix_hash.copy(buf, off);
    off += 32;
    // l1_block_number Nullable(UInt64)
    if (row.l1_block_number === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      buf.writeUInt32LE(row.l1_block_number, off);
      off += 4;
      buf.writeUInt32LE(0, off);
      off += 4;
    }
    // send_count Nullable(String)
    if (row.send_count === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      off = writeVarUInt(buf, row.send_count.length, off);
      row.send_count.copy(buf, off);
      off += row.send_count.length;
    }
    // send_root Nullable(FixedString(32))
    if (row.send_root === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      row.send_root.copy(buf, off);
      off += 32;
    }
  }

  return buf;
}

export function serializeTxHashBatch(rows: TxHashRow[]): Buffer {
  // RowBinary: chain_id UInt32 + transaction_id UInt64 + transaction_hash FixedString(32)
  const buf = Buffer.allocUnsafe(rows.length * (4 + 8 + 32));
  let off = 0;
  for (const row of rows) {
    buf.writeUInt32LE(row.chain_id, off);
    off += 4;
    off = writeUInt64LE(buf, row.transaction_id, off);
    row.transaction_hash.copy(buf, off);
    off += 32;
  }
  return buf;
}
