#!/usr/bin/env npx tsx
/**
 * Backfill Rewards Data
 *
 * Re-extracts validator_rewards from finalize_block_events WITHOUT the
 * old /1e18 division, then rebuilds rt_hourly_rewards and refreshes
 * materialized views.
 *
 * This is separated from migration 057 because it processes ~8M+ rows
 * and takes too long for a deploy release command.
 *
 * Usage: npx tsx scripts/backfill-rewards.ts
 * Or via fly: fly ssh console --app yaci-explorer-apis -C "npx tsx scripts/backfill-rewards.ts"
 */

import pg from "pg"

async function run() {
	const dbUri = process.env.PGRST_DB_URI || process.env.DATABASE_URL
	if (!dbUri) {
		console.error("[backfill-rewards] No PGRST_DB_URI or DATABASE_URL set")
		process.exit(1)
	}

	const client = new pg.Client({ connectionString: dbUri })
	await client.connect()
	console.log("[backfill-rewards] Connected to database")

	// Set a generous statement timeout (30 minutes)
	await client.query("SET statement_timeout = '30min'")

	try {
		console.log("[backfill-rewards] Step 1/4: Truncating validator_rewards...")
		await client.query("TRUNCATE api.validator_rewards")
		console.log("[backfill-rewards] Truncated.")

		console.log("[backfill-rewards] Step 2/4: Re-extracting from finalize_block_events (this may take several minutes)...")
		const insertResult = await client.query(`
			INSERT INTO api.validator_rewards (height, validator_address, rewards, commission)
			SELECT
				f.height,
				f.attributes->>'validator' as validator_address,
				SUM(CASE WHEN f.event_type = 'rewards' THEN
					COALESCE(
						NULLIF(regexp_replace(f.attributes->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
						0
					)
				ELSE 0 END) as rewards,
				SUM(CASE WHEN f.event_type = 'commission' THEN
					COALESCE(
						NULLIF(regexp_replace(f.attributes->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
						0
					)
				ELSE 0 END) as commission
			FROM api.finalize_block_events f
			WHERE f.event_type IN ('rewards', 'commission')
				AND f.attributes->>'validator' IS NOT NULL
				AND f.attributes->>'validator' != ''
			GROUP BY f.height, f.attributes->>'validator'
		`)
		console.log(`[backfill-rewards] Inserted ${insertResult.rowCount} rows into validator_rewards.`)

		console.log("[backfill-rewards] Step 3/4: Rebuilding rt_hourly_rewards...")
		await client.query("TRUNCATE api.rt_hourly_rewards")
		const hourlyResult = await client.query(`
			INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
			SELECT
				date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz) AS hour,
				SUM(COALESCE(vr.rewards, 0)) AS rewards,
				SUM(COALESCE(vr.commission, 0)) AS commission
			FROM api.validator_rewards vr
			JOIN api.blocks_raw b ON b.id = vr.height
			WHERE (b.data->'block'->'header'->>'time')::timestamptz > NOW() - INTERVAL '48 hours'
			GROUP BY date_trunc('hour', (b.data->'block'->'header'->>'time')::timestamptz)
			ON CONFLICT (hour) DO UPDATE SET
				rewards = EXCLUDED.rewards,
				commission = EXCLUDED.commission
		`)
		console.log(`[backfill-rewards] Inserted ${hourlyResult.rowCount} rows into rt_hourly_rewards.`)

		console.log("[backfill-rewards] Step 4/4: Refreshing materialized views...")
		const mvChecks = [
			{ name: "mv_daily_rewards", schema: "api" },
			{ name: "mv_validator_leaderboard", schema: "api" },
		]
		for (const mv of mvChecks) {
			const { rowCount } = await client.query(
				`SELECT 1 FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2`,
				[mv.schema, mv.name]
			)
			if (rowCount && rowCount > 0) {
				console.log(`[backfill-rewards] Refreshing ${mv.schema}.${mv.name}...`)
				await client.query(`REFRESH MATERIALIZED VIEW ${mv.schema}.${mv.name}`)
				console.log(`[backfill-rewards] Refreshed ${mv.schema}.${mv.name}.`)
			}
		}

		console.log("[backfill-rewards] Done.")
	} catch (err) {
		console.error("[backfill-rewards] Error:", err)
		process.exit(1)
	} finally {
		await client.end()
	}
}

run()
