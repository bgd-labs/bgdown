import { HypersyncClient } from "@envio-dev/hypersync-client";
import { type Chain, createPublicClient, http } from "viem";
import {
  arbitrum,
  avalanche,
  base,
  baseSepolia,
  bsc,
  celo,
  gnosis,
  ink,
  linea,
  mainnet,
  mantle,
  megaeth,
  optimism,
  plasma,
  polygon,
  scroll,
  sonic,
} from "viem/chains";
import env from "./env";

const CHAINS = [
  mainnet,
  optimism,
  bsc,
  gnosis,
  polygon,
  sonic,
  megaeth,
  mantle,
  base,
  plasma,
  arbitrum,
  celo,
  avalanche,
  ink,
  linea,
  baseSepolia,
  scroll,
] satisfies Chain[];

interface ChainConfig {
  readonly id: number;
  readonly name: string;
  readonly hypersyncUrl: string;
  /**
   * How many blocks behind the HyperSync tip we stop indexing.
   * HyperSync only surfaces finalized/safe data so this is a secondary guard,
   * but it protects against short reorgs on chains where finality is not
   * instant (primarily Polygon and BSC).
   */
  readonly reorgSafetyFallback: number;
}

// Per-chain reorg safety margins.
// L2s with a single sequencer (Arbitrum, Base, OP, Linea, Scroll, Ink, Mantle,
// Plasma, Base Sepolia, MegaETH, Sonic) have no L2 reorgs → 5 (small buffer
// for HyperSync edge cases only).
// L1s / PoS chains with near-instant finality (Ethereum, Gnosis, Celo,
// Avalanche) → 12.
// BSC Parlia → 15.
// Polygon (historically long reorgs) → 64.
const REORG_FALLBACK: Partial<Record<number, number>> = {
  1: 12, // Ethereum mainnet
  10: 5, // Optimism
  56: 15, // BSC
  100: 12, // Gnosis
  137: 64, // Polygon
  146: 5, // Sonic
  5000: 5, // Mantle
  6342: 5, // MegaETH
  8453: 5, // Base
  42161: 5, // Arbitrum
  42220: 12, // Celo
  43114: 12, // Avalanche
  57073: 5, // Ink
  59144: 5, // Linea
  84532: 5, // Base Sepolia
  361: 5, // Plasma
  534352: 5, // Scroll
};

const DEFAULT_REORG_FALLBACK = 64;

function toChainConfig(chain: Chain): ChainConfig {
  return {
    id: chain.id,
    name: chain.name,
    hypersyncUrl: `https://${chain.id}.hypersync.xyz`,
    reorgSafetyFallback: REORG_FALLBACK[chain.id] ?? DEFAULT_REORG_FALLBACK,
  };
}

export const CHAIN_BY_ID: ReadonlyMap<number, ChainConfig> = new Map(
  CHAINS.map((c) => [c.id, toChainConfig(c)]),
);

// ── Per-chain client caches ───────────────────────────────────────────────────

const viemCache = new Map<number, ReturnType<typeof createPublicClient>>();
const hypersyncCache = new Map<number, HypersyncClient>();

export function getViemForChain(chainId: number) {
  if (!viemCache.has(chainId)) {
    const chain = CHAINS.find((c) => c.id === chainId);
    viemCache.set(
      chainId,
      createPublicClient({ chain, transport: http() }) as ReturnType<
        typeof createPublicClient
      >,
    );
  }
  // biome-ignore lint/style/noNonNullAssertion: we know it's there because we just set it if it wasn't
  return viemCache.get(chainId)!;
}

export function getHypersyncForChain(chainId: number) {
  if (!hypersyncCache.has(chainId)) {
    const config = CHAIN_BY_ID.get(chainId);
    hypersyncCache.set(
      chainId,
      new HypersyncClient({
        url: config?.hypersyncUrl ?? `https://${chainId}.hypersync.xyz`,
        apiToken: env.HYPERSYNC_API_KEY,
      }),
    );
  }
  // biome-ignore lint/style/noNonNullAssertion: we know it's there because we just set it if it wasn't
  return hypersyncCache.get(chainId)!;
}
