import type { EventResponse } from "@envio-dev/hypersync-client";
import { LRUCache } from "lru-cache";

const hexToBytes = (
  expectedLength?: number,
  cache?: LRUCache<string, Uint8Array>,
) => {
  return (hex: string) => {
    if (cache) {
      const cached = cache.get(hex);
      if (cached) return cached;
    }

    let cleanHex = hex.replace(/^0x/, "");

    if (expectedLength) {
      // Pad the string with leading zeros so it is EXACTLY the right character length (2 chars per byte)
      cleanHex = cleanHex.padStart(expectedLength * 2, "0");
    }

    const bytes = new Uint8Array(Buffer.from(cleanHex, "hex"));
    if (cache) {
      cache.set(hex, bytes);
    }
    return bytes;
  };
};

const blockHashCache = new LRUCache<string, Uint8Array>({ max: 50_000 });
const transactionHashCache = new LRUCache<string, Uint8Array>({ max: 100_000 });
const topic0Cache = new LRUCache<string, Uint8Array>({ max: 10_000 });
const addressCache = new LRUCache<string, Uint8Array>({ max: 20_000 });

export const parseHyperSyncResponse = (events: EventResponse["data"]) => {
  return events.map(({ block, log }) => {
    return {
      blockNumber: BigInt(log.blockNumber!),
      blockHash: hexToBytes(32, blockHashCache)(log.blockHash!),
      transactionHash: hexToBytes(
        32,
        transactionHashCache,
      )(log.transactionHash!),
      transactionIndex: log.transactionIndex!,
      logIndex: log.logIndex!,
      address: hexToBytes(20, addressCache)(log.address!),
      data: hexToBytes()(log.data!),
      topic0: hexToBytes(32, topic0Cache)(log.topics[0] ?? ""),
      topic1: log.topics[1] ? hexToBytes(32)(log.topics[1]) : undefined,
      topic2: log.topics[2] ? hexToBytes(32)(log.topics[2]) : undefined,
      topic3: log.topics[3] ? hexToBytes(32)(log.topics[3]) : undefined,
      removed: log.removed ? 1 : 0,
      timestamp: block!.timestamp!,
    };
  });
};
