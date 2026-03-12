# eth-logs-downloader

A high-performance Ethereum logs indexing and retrieval system.

## Stack
- **Runtime:** Bun
- **API:** Elysia
- **Database:** ClickHouse
- **Ingestion:** Hypersync

## Getting Started
1. Copy `.env.example` to `.env.local` and fill in necessary values:
   `cp .env.example .env.local`
2. Run `docker-compose up -d` for ClickHouse.
3. `bun run indexer` to start syncing logs.
4. `bun run api` to start the web server at port 3000.

## Development
- `bun run dev:api`: Run API server in watch mode.
- `bun run dev:indexer`: Run indexer in watch mode.
- `bun run lint`: Run Biome checks.
- `bun run typecheck`: Run TypeScript checks.
- `bun run reset`: Clear the local database.
