# AGENTS.md

Welcome to the `eth-logs-downloader` project. This document provides core context and instructions for AI agents working on this codebase.

## Project Overview

The `eth-logs-downloader` is a high-performance Ethereum logs indexing and retrieval system. It uses **Hypersync** for fast data ingestion and **ClickHouse** for efficient storage and querying.

## Technology Stack

- **Runtime:** [Bun](https://bun.sh/)
- **Web Framework:** [Elysia](https://elysiajs.com/)
- **Database:** [ClickHouse](https://clickhouse.com/)
- **Data Ingestion:** [Hypersync](https://hypersync.xyz/)
- **Coding Standards:** [Biome](https://biomejs.dev/) for linting & formatting.
- **Type Safety:** TypeScript + [ArkType](https://arktype.io/) for runtime validation.

## Key Project Components

- `src/api.ts`: Entry point for the Elysia API server.
- `src/indexer.ts`: Entry point for the data indexer that syncs logs to ClickHouse.
- `src/routes/`: Contains API route definitions (e.g., `logs.ts`).
- `src/clickhouse.ts`: ClickHouse client configuration and helper functions.
- `src/schema.ts`: Defines the ClickHouse table schemas.
- `src/migrations/`: Contains SQL migration files for ClickHouse.

## Common Development Commands

- `bun run api`: Starts the API server.
- `bun run indexer`: Starts the log indexer.
- `bun run lint`: Runs Biome check on the source code.
- `bun run lint:fix`: Automatically fixes linting issues.
- `bun run typecheck`: Runs TypeScript type checking.
- `bun run reset`: Resets the local Docker environment (ClickHouse).

## Guidelines for Agents

- **Use Bun:** Always use `bun` instead of `npm` or `yarn`.
- **Async Operations:** Always use `better-all` instead of `Promise.all` for parallel async operations.
- **Biome:** Ensure code is formatted and linted with biome. Use `bun run lint:fix` before committing.
- **ClickHouse Migrations:** When modifying the database schema, add a new migration file in `src/migrations/`.
- **Performance:** This project handles large datasets. Be mindful of query performance and memory usage when streaming data.
