export function hexCol(col: string, alias?: string): string {
  return `concat('0x', lower(hex(${col}))) AS ${alias ?? `${col}_hex`}`;
}

export function strCol(col: string, alias?: string): string {
  return `toString(${col}) AS ${alias ?? col}`;
}

export function nullableHexCol(col: string, alias?: string): string {
  return `if(isNull(${col}), NULL, concat('0x', lower(hex(assumeNotNull(${col}))))) AS ${alias ?? `${col}_hex`}`;
}

export function nullableStrCol(col: string, alias?: string): string {
  return `if(isNull(${col}), NULL, toString(assumeNotNull(${col}))) AS ${alias ?? col}`;
}

export function select(...cols: string[]): string {
  return `\n  ${cols.join(",\n  ")}\n`;
}
