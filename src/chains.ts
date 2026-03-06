interface ChainConfig {
  readonly id: number;
  readonly name: string;
  readonly hypersyncUrl: string;
}

const CHAINS = [
  {
    id: 1,
    name: "Ethereum",
    hypersyncUrl: "https://eth.hypersync.xyz",
  },
  {
    id: 10,
    name: "OP Mainnet",
    hypersyncUrl: "https://optimism.hypersync.xyz",
  },
  {
    id: 56,
    name: "BNB Smart Chain",
    hypersyncUrl: "https://bsc.hypersync.xyz",
  },
  {
    id: 100,
    name: "Gnosis",
    hypersyncUrl: "https://gnosis.hypersync.xyz",
  },
  {
    id: 137,
    name: "Polygon",
    hypersyncUrl: "https://polygon.hypersync.xyz",
  },
  {
    id: 146,
    name: "Sonic",
    hypersyncUrl: "https://sonic.hypersync.xyz",
  },
  {
    id: 4326,
    name: "MegaETH",
    hypersyncUrl: "https://megaeth.hypersync.xyz",
  },
  {
    id: 5000,
    name: "Mantle",
    hypersyncUrl: "https://mantle.hypersync.xyz",
  },
  {
    id: 8453,
    name: "Base",
    hypersyncUrl: "https://base.hypersync.xyz",
  },
  {
    id: 9745,
    name: "Plasma",
    hypersyncUrl: "https://plasma.hypersync.xyz",
  },
  {
    id: 42161,
    name: "Arbitrum One",
    hypersyncUrl: "https://arbitrum.hypersync.xyz",
  },
  {
    id: 42220,
    name: "Celo",
    hypersyncUrl: "https://celo.hypersync.xyz",
  },
  {
    id: 43114,
    name: "Avalanche",
    hypersyncUrl: "https://avalanche.hypersync.xyz",
  },
  {
    id: 57073,
    name: "Ink",
    hypersyncUrl: "https://ink.hypersync.xyz",
  },
  {
    id: 59144,
    name: "Linea Mainnet",
    hypersyncUrl: "https://linea.hypersync.xyz",
  },
  {
    id: 84532,
    name: "Base Sepolia",
    hypersyncUrl: "https://base-sepolia.hypersync.xyz",
  },
  {
    id: 534352,
    name: "Scroll",
    hypersyncUrl: "https://scroll.hypersync.xyz",
  },
] as const satisfies ChainConfig[];

export const CHAIN_BY_ID: ReadonlyMap<number, ChainConfig> = new Map(
  CHAINS.map((c) => [c.id, c]),
);
