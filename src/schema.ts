// This file is kept for backwards compatibility.
// Schema management is now handled by the migration system.
// See src/migrate.ts and src/migrations/.
export { runMigrations as ensureSchema } from "./migrate";
