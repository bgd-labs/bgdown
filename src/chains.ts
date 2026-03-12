import type { Chain } from "viem";
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

// Per-chain reorg safety margins.
// L2s with a single sequencer (Arbitrum, Base, OP, Linea, Scroll, Ink, Mantle,
// Plasma, Base Sepolia, MegaETH, Sonic) have no L2 reorgs → 5 (small buffer
// for HyperSync edge cases only).
// L1s / PoS chains with near-instant finality (Ethereum, Gnosis, Celo,
// Avalanche) → 12.
// BSC Parlia → 15.
// Polygon (historically long reorgs) → 64.
const CHAINS = [
  {
    ...mainnet,
    reorgSafetyFallback: 12,
  },
  { ...optimism, reorgSafetyFallback: 5 },
  { ...bsc, reorgSafetyFallback: 15 },
  { ...gnosis, reorgSafetyFallback: 12 },
  { ...polygon, reorgSafetyFallback: 64 },
  { ...sonic, reorgSafetyFallback: 5 },
  { ...megaeth, reorgSafetyFallback: 5 },
  { ...mantle, reorgSafetyFallback: 5 },
  { ...base, reorgSafetyFallback: 5 },
  { ...plasma, reorgSafetyFallback: 5 },
  { ...arbitrum, reorgSafetyFallback: 5 },
  { ...celo, reorgSafetyFallback: 12 },
  { ...avalanche, reorgSafetyFallback: 12 },
  { ...ink, reorgSafetyFallback: 5 },
  { ...linea, reorgSafetyFallback: 5 },
  { ...baseSepolia, reorgSafetyFallback: 5 },
  { ...scroll, reorgSafetyFallback: 5 },
] satisfies (Chain & {
  reorgSafetyFallback: number;
})[];

function toChainConfig(chain: (typeof CHAINS)[number]) {
  return {
    id: chain.id,
    name: chain.name,
    hypersyncUrl: `https://${chain.id}.hypersync.xyz`,
    reorgSafetyFallback: chain.reorgSafetyFallback,
  };
}

export const CHAIN_BY_ID = new Map(CHAINS.map((c) => [c.id, toChainConfig(c)]));

export const SUPPORTED_CHAIN_IDS = CHAINS.map((c) => c.id) as [
  (typeof CHAINS)[number]["id"],
  ...(typeof CHAINS)[number]["id"][],
];
