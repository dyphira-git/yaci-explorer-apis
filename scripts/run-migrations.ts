#!/usr/bin/env npx tsx
/**
 * Migration Runner
 *
 * Executes SQL migration files in order, tracking applied migrations
 * in a `schema_migrations` table to avoid re-running.
 *
 * Used as a Fly.io release command to run migrations before deploy.
 */

import { readdir, readFile } from "fs/promises"
import { join } from "path"
import pg from "pg"

const MIGRATIONS_DIR = join(import.meta.dirname, "..", "migrations")

async function run() {
	const dbUri = process.env.PGRST_DB_URI || process.env.DATABASE_URL
	if (!dbUri) {
		console.error("[migrate] No PGRST_DB_URI or DATABASE_URL set, skipping migrations")
		process.exit(1)
	}

	const client = new pg.Client({ connectionString: dbUri })
	await client.connect()
	console.log("[migrate] Connected to database")

	// Ensure tracking table exists
	const { rowCount: tableExisted } = await client.query(`
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'schema_migrations'
	`)

	await client.query(`
		CREATE TABLE IF NOT EXISTS public.schema_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)

	// Bootstrap: if tracking table was just created but the DB already has
	// migrations applied (tables exist), seed it with pre-existing files
	// so we don't re-run them.
	if (!tableExisted) {
		const { rowCount: hasSchema } = await client.query(`
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'api' AND table_name = 'blocks_raw'
		`)
		if (hasSchema && hasSchema > 0) {
			console.log("[migrate] Bootstrapping: DB has existing schema, seeding tracking table")
			const files = (await readdir(MIGRATIONS_DIR))
				.filter((f) => f.endsWith(".sql") && !f.endsWith(".backup"))
				.sort()
			// Seed all files that existed before this runner was introduced (001-020)
			const preExisting = files.filter((f) => {
				const num = parseInt(f.split("_")[0], 10)
				return num <= 20
			})
			for (const file of preExisting) {
				await client.query(
					"INSERT INTO public.schema_migrations (filename) VALUES ($1) ON CONFLICT DO NOTHING",
					[file]
				)
			}
			console.log(`[migrate] Seeded ${preExisting.length} pre-existing migrations`)
		}
	}

	// Get already-applied migrations
	const { rows: applied } = await client.query(
		"SELECT filename FROM public.schema_migrations ORDER BY filename"
	)
	const appliedSet = new Set(applied.map((r: { filename: string }) => r.filename))
	console.log(`[migrate] ${appliedSet.size} migrations already applied`)

	// List migration files in order
	const files = (await readdir(MIGRATIONS_DIR))
		.filter((f) => f.endsWith(".sql") && !f.endsWith(".backup"))
		.sort()

	let ranCount = 0
	for (const file of files) {
		if (appliedSet.has(file)) continue

		console.log(`[migrate] Applying ${file}...`)
		const sql = await readFile(join(MIGRATIONS_DIR, file), "utf-8")

		try {
			await client.query(sql)
			await client.query(
				"INSERT INTO public.schema_migrations (filename) VALUES ($1)",
				[file]
			)
			ranCount++
			console.log(`[migrate] Applied ${file}`)
		} catch (err) {
			console.error(`[migrate] FAILED on ${file}:`, err)
			await client.end()
			process.exit(1)
		}
	}

	await client.end()

	if (ranCount === 0) {
		console.log("[migrate] No new migrations to apply")
	} else {
		console.log(`[migrate] Applied ${ranCount} new migration(s)`)
	}
}

run().catch((err) => {
	console.error("[migrate] Fatal error:", err)
	process.exit(1)
})
