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
}

function toChainConfig(chain: Chain): ChainConfig {
  return {
    id: chain.id,
    name: chain.name,
    hypersyncUrl: `https://${chain.id}.hypersync.xyz`,
  };
}

export const CHAIN_BY_ID: ReadonlyMap<number, ChainConfig> = new Map(
  CHAINS.map((c) => [c.id, toChainConfig(c)]),
);

// ── Per-chain client caches ───────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const viemCache = new Map<number, any>();
const hypersyncCache = new Map<number, HypersyncClient>();

export function getViemForChain(chainId: number) {
  if (!viemCache.has(chainId)) {
    const chain = CHAINS.find((c) => c.id === chainId);
    viemCache.set(
      chainId,
      createPublicClient({ chain, transport: http(env.RPC_URL) }),
    );
  }
  return viemCache.get(chainId) as ReturnType<typeof createPublicClient>;
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
  return hypersyncCache.get(chainId)!;
}

// ── Convenience helpers (use CHAIN_ID from env) ───────────────────────────────

const getViem = () => getViemForChain(env.CHAIN_ID);

export async function getFinalizedBlock(): Promise<number> {
  const block = await getViem().getBlock({ blockTag: "finalized" });
  return Number(block.number);
}

export async function getHeadBlock(): Promise<number> {
  const block = await getViem().getBlock({ blockTag: "latest" });
  return Number(block.number);
}
