import { t } from "elysia";

export const Block = t.Object({
  number: t.Number({ description: "Block number" }),
  hash: t.String({ description: "Block hash" }),
  parentHash: t.String({ description: "Parent block hash" }),
  nonce: t.String({ description: "Block nonce" }),
  sha3Uncles: t.String({ description: "SHA3 of uncles data" }),
  logsBloom: t.String({ description: "Bloom filter for logs" }),
  transactionsRoot: t.String({ description: "Root of transaction trie" }),
  stateRoot: t.String({ description: "Root of state trie" }),
  receiptsRoot: t.String({ description: "Root of receipts trie" }),
  miner: t.String({ description: "Address of the block miner/validator" }),
  difficulty: t.String({ description: "Block difficulty" }),
  totalDifficulty: t.String({
    description: "Total chain difficulty at this block",
  }),
  extraData: t.String({ description: "Extra data field" }),
  size: t.String({ description: "Block size in bytes" }),
  gasLimit: t.String({ description: "Gas limit" }),
  gasUsed: t.String({ description: "Gas used" }),
  timestamp: t.Number({ description: "Unix timestamp" }),
  baseFeePerGas: t.Nullable(
    t.String({ description: "Base fee per gas (post EIP-1559)" }),
  ),
  blobGasUsed: t.Nullable(
    t.String({ description: "Blob gas used (post EIP-4844)" }),
  ),
  excessBlobGas: t.Nullable(
    t.String({ description: "Excess blob gas (post EIP-4844)" }),
  ),
  parentBeaconBlockRoot: t.Nullable(
    t.String({ description: "Parent beacon block root (post Dencun)" }),
  ),
  withdrawalsRoot: t.Nullable(
    t.String({ description: "Withdrawals trie root (post Shanghai)" }),
  ),
  withdrawals: t.String({ description: "Withdrawals JSON array" }),
  uncles: t.String({ description: "Uncle hashes JSON array" }),
  mixHash: t.String({ description: "Mix hash" }),
  l1BlockNumber: t.Nullable(
    t.Number({ description: "L1 block number (L2 chains only)" }),
  ),
  sendCount: t.Nullable(t.String({ description: "Send count (Arbitrum)" })),
  sendRoot: t.Nullable(t.String({ description: "Send root (Arbitrum)" })),
});

export interface BlockQueryRow {
  number: string;
  hash_hex: string;
  parent_hash_hex: string;
  nonce: string;
  sha3_uncles_hex: string;
  logs_bloom_hex: string;
  transactions_root_hex: string;
  state_root_hex: string;
  receipts_root_hex: string;
  miner_hex: string;
  difficulty: string;
  total_difficulty: string;
  extra_data_hex: string;
  size: string;
  gas_limit: string;
  gas_used: string;
  timestamp: string;
  base_fee_per_gas: string | null;
  blob_gas_used: string | null;
  excess_blob_gas: string | null;
  parent_beacon_block_root_hex: string | null;
  withdrawals_root_hex: string | null;
  withdrawals: string;
  uncles: string;
  mix_hash_hex: string;
  l1_block_number: string | null;
  send_count: string | null;
  send_root_hex: string | null;
}

export const BLOCK_SELECT = `
  number,
  concat('0x', lower(hex(hash)))                  AS hash_hex,
  concat('0x', lower(hex(parent_hash)))            AS parent_hash_hex,
  toString(nonce)                                  AS nonce,
  concat('0x', lower(hex(sha3_uncles)))            AS sha3_uncles_hex,
  concat('0x', lower(hex(logs_bloom)))             AS logs_bloom_hex,
  concat('0x', lower(hex(transactions_root)))      AS transactions_root_hex,
  concat('0x', lower(hex(state_root)))             AS state_root_hex,
  concat('0x', lower(hex(receipts_root)))          AS receipts_root_hex,
  concat('0x', lower(hex(miner)))                  AS miner_hex,
  toString(difficulty)                             AS difficulty,
  total_difficulty,
  concat('0x', lower(hex(extra_data)))             AS extra_data_hex,
  toString(size)                                   AS size,
  toString(gas_limit)                              AS gas_limit,
  toString(gas_used)                               AS gas_used,
  timestamp,
  if(isNull(base_fee_per_gas), NULL, toString(assumeNotNull(base_fee_per_gas))) AS base_fee_per_gas,
  if(isNull(blob_gas_used), NULL, toString(assumeNotNull(blob_gas_used)))       AS blob_gas_used,
  if(isNull(excess_blob_gas), NULL, toString(assumeNotNull(excess_blob_gas)))   AS excess_blob_gas,
  if(isNull(parent_beacon_block_root), NULL, concat('0x', lower(hex(assumeNotNull(parent_beacon_block_root))))) AS parent_beacon_block_root_hex,
  if(isNull(withdrawals_root), NULL, concat('0x', lower(hex(assumeNotNull(withdrawals_root)))))                 AS withdrawals_root_hex,
  withdrawals,
  uncles,
  concat('0x', lower(hex(mix_hash)))               AS mix_hash_hex,
  if(isNull(l1_block_number), NULL, toString(assumeNotNull(l1_block_number))) AS l1_block_number,
  send_count,
  if(isNull(send_root), NULL, concat('0x', lower(hex(assumeNotNull(send_root))))) AS send_root_hex
`;

export function encodeBlockCursor(blockNumber: number): string {
  return Buffer.from(`${blockNumber}`).toString("base64url");
}

export function decodeBlockCursor(cursor: string): number {
  return Number(Buffer.from(cursor, "base64url").toString());
}

export function rowToBlock(row: BlockQueryRow): typeof Block.static {
  return {
    number: Number(row.number),
    hash: row.hash_hex,
    parentHash: row.parent_hash_hex,
    nonce: row.nonce,
    sha3Uncles: row.sha3_uncles_hex,
    logsBloom: row.logs_bloom_hex,
    transactionsRoot: row.transactions_root_hex,
    stateRoot: row.state_root_hex,
    receiptsRoot: row.receipts_root_hex,
    miner: row.miner_hex,
    difficulty: row.difficulty,
    totalDifficulty: row.total_difficulty,
    extraData: row.extra_data_hex,
    size: row.size,
    gasLimit: row.gas_limit,
    gasUsed: row.gas_used,
    timestamp: Number(row.timestamp),
    baseFeePerGas: row.base_fee_per_gas,
    blobGasUsed: row.blob_gas_used,
    excessBlobGas: row.excess_blob_gas,
    parentBeaconBlockRoot: row.parent_beacon_block_root_hex,
    withdrawalsRoot: row.withdrawals_root_hex,
    withdrawals: row.withdrawals,
    uncles: row.uncles,
    mixHash: row.mix_hash_hex,
    l1BlockNumber: row.l1_block_number ? Number(row.l1_block_number) : null,
    sendCount: row.send_count,
    sendRoot: row.send_root_hex,
  };
}
