let cacheTime = 0;
let cachedAddresses: Map<number, string[]> = new Map();

const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

export async function getSafeAddresses(chainId: number): Promise<string[]> {
  const now = Date.now();
  if (now - cacheTime < CACHE_TTL_MS && cachedAddresses.has(chainId)) {
    return cachedAddresses.get(chainId) || [];
  }

  const url =
    "https://raw.githubusercontent.com/bgd-labs/aave-address-book/main/safe.csv";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch safe.csv: ${res.statusText}`);
  const text = await res.text();
  const lines = text.split("\n").slice(1); // skip header

  const newMap = new Map<number, string[]>();
  for (const line of lines) {
    if (!line.trim()) continue;
    const parts = line.split(",");
    if (parts.length >= 3) {
      const addr = parts[0].trim();
      const cId = parseInt(parts[2].trim(), 10);
      if (!newMap.has(cId)) newMap.set(cId, []);
      newMap.get(cId)?.push(addr);
    }
  }

  cachedAddresses = newMap;
  cacheTime = now;

  return cachedAddresses.get(chainId) || [];
}
