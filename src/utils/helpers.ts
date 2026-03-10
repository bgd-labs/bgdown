// Convert a 0x-prefixed hex string to a fixed-length Buffer of `len` bytes.
// Returns a zero-filled buffer for missing/empty values.
export function hexBuf(hex: string | null | undefined, len: number): Buffer {
  if (!hex || hex.length < 3) return Buffer.alloc(len);
  return Buffer.from(hex.slice(2), "hex");
}
