export function decodeCursor(cursor: string): string {
  return Buffer.from(cursor, "base64url").toString();
}
