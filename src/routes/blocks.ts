import { Elysia, t } from "elysia";
import {
  clampLimit,
  clickhouse,
  DEFAULT_LIMIT,
  fetchHeight,
  fetchStats,
  MAX_LIMIT,
} from "../clickhouse";
import {
  hexCol,
  nullableHexCol,
  nullableStrCol,
  select,
  strCol,
} from "../utils/sql";

const Block = t.Object({
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

interface BlockQueryRow {
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

const BLOCK_SELECT = select(
  "number",
  hexCol("hash"),
  hexCol("parent_hash"),
  strCol("nonce"),
  hexCol("sha3_uncles"),
  hexCol("logs_bloom"),
  hexCol("transactions_root"),
  hexCol("state_root"),
  hexCol("receipts_root"),
  hexCol("miner"),
  strCol("difficulty"),
  "total_difficulty",
  hexCol("extra_data"),
  strCol("size"),
  strCol("gas_limit"),
  strCol("gas_used"),
  "timestamp",
  nullableStrCol("base_fee_per_gas"),
  nullableStrCol("blob_gas_used"),
  nullableStrCol("excess_blob_gas"),
  nullableHexCol("parent_beacon_block_root"),
  nullableHexCol("withdrawals_root"),
  "withdrawals",
  "uncles",
  hexCol("mix_hash"),
  nullableStrCol("l1_block_number"),
  "send_count",
  nullableHexCol("send_root"),
);

function encodeBlockCursor(blockNumber: number): string {
  return Buffer.from(`${blockNumber}`).toString("base64url");
}

function decodeBlockCursor(cursor: string): number {
  return Number(Buffer.from(cursor, "base64url").toString());
}

function rowToBlock(row: BlockQueryRow): typeof Block.static {
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

export const blockRoutes = new Elysia()
  .get(
    "/blocks/height",
    async ({ params }) => ({
      height: await fetchHeight("blocks", params.chainId),
    }),
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      response: {
        200: t.Object({
          height: t.Number({
            description: "Last indexed block number",
          }),
        }),
      },
    },
  )
  .get(
    "/blocks/stats",
    async ({ params }) => fetchStats("blocks", params.chainId),
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      response: {
        200: t.Object({
          total: t.Number({
            description: "Total number of indexed blocks",
          }),
          maxIndexedBlock: t.Number({
            description: "Highest block number indexed",
          }),
          compressedSize: t.String({
            description: "Compressed size of the blocks table on disk",
          }),
          compressionRatio: t.Number({
            description: "Uncompressed / compressed ratio",
          }),
        }),
      },
    },
  )
  .get(
    "/blocks",
    async ({ params, query }) => {
      const limit = clampLimit(query.limit);
      const cursor = query.cursor ? decodeBlockCursor(query.cursor) : null;

      const cursorClause = cursor ? "AND number > {cursorBlock: UInt64}" : "";

      const result = await clickhouse.query({
        query: `
          SELECT ${BLOCK_SELECT}
          FROM ethereum.blocks
          WHERE chain_id = {chainId: UInt32}
            ${cursorClause}
          ORDER BY number
          LIMIT {limit: UInt32}
        `,
        query_params: {
          chainId: params.chainId,
          limit,
          ...(cursor ? { cursorBlock: cursor } : {}),
        },
        format: "JSONEachRow",
      });

      const rows = await result.json<BlockQueryRow>();
      const blocks = rows.map(rowToBlock);

      const lastRow = rows.at(-1);
      const nextCursor =
        rows.length === limit && lastRow
          ? encodeBlockCursor(Number(lastRow.number))
          : null;

      return { blocks, nextCursor };
    },
    {
      params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
      query: t.Object({
        cursor: t.Optional(
          t.String({
            description:
              "Opaque pagination cursor from the previous response's nextCursor",
            examples: ["MTAwMDA"],
          }),
        ),
        limit: t.Optional(
          t.Numeric({
            description: `Number of blocks to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
            examples: [100],
          }),
        ),
      }),
      response: {
        200: t.Object({
          blocks: t.Array(Block),
          nextCursor: t.Nullable(
            t.String({
              description:
                "Pass as cursor in the next request to fetch the following page; null when no more results",
            }),
          ),
        }),
      },
    },
  )
  .get(
    "/block/:blockNumber",
    async ({ params, status }) => {
      const result = await clickhouse.query({
        query: `
          SELECT ${BLOCK_SELECT}
          FROM ethereum.blocks
          WHERE chain_id = {chainId: UInt32}
            AND number = {blockNumber: UInt64}
          LIMIT 1
        `,
        query_params: {
          chainId: params.chainId,
          blockNumber: params.blockNumber,
        },
        format: "JSONEachRow",
      });

      const rows = await result.json<BlockQueryRow>();
      if (rows.length === 0) return status(404, "Block not found");

      return rowToBlock(rows[0]);
    },
    {
      params: t.Object({
        chainId: t.String({ examples: ["1"] }),
        blockNumber: t.String({ examples: ["17000000"] }),
      }),
      response: {
        200: Block,
        404: t.String(),
      },
    },
  );
