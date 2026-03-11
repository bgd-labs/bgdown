import { HypersyncClient } from "@envio-dev/hypersync-client";
import { CHAIN_BY_ID } from "./chains.ts";
import env from "./env.ts";

const hypersyncCache = new Map<number, HypersyncClient>();

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
