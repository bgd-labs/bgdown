export function encodeCursor(value: string): string {
  return Buffer.from(value).toString("base64url");
}

export function decodeCursor(cursor: string): string {
  return Buffer.from(cursor, "base64url").toString();
}
