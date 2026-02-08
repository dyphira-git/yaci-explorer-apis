#!/usr/bin/env npx tsx
/**
 * Validator Refresh Script
 *
 * Periodically fetches the full validator set from the chain via the
 * chain-query-service and upserts into the api.validators table.
 *
 * Handles:
 * - Genesis validators not seen in indexed transactions
 * - Accurate current token totals and status
 * - Jailed state changes
 * - Commission rate changes
 *
 * Also refreshes analytics materialized views on a separate cadence
 * (default: every 4 cycles = ~1 hour).
 *
 * Runs on a configurable interval (default: 15 minutes).
 *
 * Environment:
 *   DATABASE_URL         - PostgreSQL connection string
 *   CHAIN_QUERY_URL      - chain-query-service base URL (default: http://localhost:3001)
 *   REFRESH_INTERVAL_MS  - Refresh interval in milliseconds (default: 900000 = 15 min)
 *   MV_REFRESH_EVERY_N   - Refresh materialized views every N cycles (default: 4 = ~1 hour)
 */

import pg from 'pg'
const { Pool } = pg

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:bOqwmcryOQcdmrO@localhost:15432/postgres?sslmode=disable'
const CHAIN_QUERY_URL = process.env.CHAIN_QUERY_URL || 'https://yaci-explorer-apis.fly.dev'
const REFRESH_INTERVAL_MS = parseInt(process.env.REFRESH_INTERVAL_MS || '900000', 10)
const MV_REFRESH_EVERY_N = parseInt(process.env.MV_REFRESH_EVERY_N || '1', 10)

/** Maps gRPC status enum to DB status string */
function mapStatus(status: number | string): string {
	const statusMap: Record<string, string> = {
		'0': 'BOND_STATUS_UNSPECIFIED',
		'1': 'BOND_STATUS_UNBONDED',
		'2': 'BOND_STATUS_UNBONDING',
		'3': 'BOND_STATUS_BONDED',
		'BOND_STATUS_UNSPECIFIED': 'BOND_STATUS_UNSPECIFIED',
		'BOND_STATUS_UNBONDED': 'BOND_STATUS_UNBONDED',
		'BOND_STATUS_UNBONDING': 'BOND_STATUS_UNBONDING',
		'BOND_STATUS_BONDED': 'BOND_STATUS_BONDED',
	}
	return statusMap[String(status)] || 'BOND_STATUS_UNSPECIFIED'
}

/** Fetches validators from chain-query-service */
async function fetchValidators(): Promise<any[]> {
	const controller = new AbortController()
	const timeoutId = setTimeout(() => controller.abort(), 30000)

	try {
		const response = await fetch(`${CHAIN_QUERY_URL}/chain/staking/validators`, {
			signal: controller.signal,
		})
		clearTimeout(timeoutId)

		if (!response.ok) {
			throw new Error(`Failed to fetch validators: ${response.status} ${response.statusText}`)
		}

		const data = await response.json()
		return data.validators || []
	} catch (err) {
		clearTimeout(timeoutId)
		throw err
	}
}

/** Fetches staking pool from chain-query-service */
async function fetchStakingPool(): Promise<{ bondedTokens: string; notBondedTokens: string }> {
	const controller = new AbortController()
	const timeoutId = setTimeout(() => controller.abort(), 10000)

	try {
		const response = await fetch(`${CHAIN_QUERY_URL}/chain/staking/pool`, {
			signal: controller.signal,
		})
		clearTimeout(timeoutId)

		if (!response.ok) {
			throw new Error(`Failed to fetch staking pool: ${response.status} ${response.statusText}`)
		}

		const data = await response.json()
		return data.pool || { bondedTokens: '0', notBondedTokens: '0' }
	} catch (err) {
		clearTimeout(timeoutId)
		throw err
	}
}

/** Upserts a single validator into the DB */
async function upsertValidator(client: pg.PoolClient, validator: any): Promise<void> {
	const operatorAddress = validator.operatorAddress || ''
	if (!operatorAddress) return

	const status = mapStatus(validator.status)
	const desc = validator.description || {}
	const rates = validator.commission?.commissionRates || {}

	// Parse numeric fields, defaulting to null on failure
	const parseNum = (val: string | undefined): string | null => {
		if (!val || val === '0') return val === '0' ? '0' : null
		return val
	}

	/** Converts Cosmos SDK Dec format (10^18) commission rates to decimal */
	const parseRate = (val: string | undefined): string | null => {
		if (!val) return null
		const n = Number(val)
		if (n === 0) return '0'
		if (n > 1) return (n / 1e18).toString()
		return val
	}

	await client.query(
		`INSERT INTO api.validators (
			operator_address, moniker, identity, website, details,
			commission_rate, commission_max_rate, commission_max_change_rate,
			min_self_delegation, tokens, delegator_shares,
			status, jailed
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (operator_address) DO UPDATE SET
			moniker = COALESCE(EXCLUDED.moniker, api.validators.moniker),
			identity = COALESCE(EXCLUDED.identity, api.validators.identity),
			website = COALESCE(EXCLUDED.website, api.validators.website),
			details = COALESCE(EXCLUDED.details, api.validators.details),
			commission_rate = COALESCE(EXCLUDED.commission_rate, api.validators.commission_rate),
			commission_max_rate = COALESCE(EXCLUDED.commission_max_rate, api.validators.commission_max_rate),
			commission_max_change_rate = COALESCE(EXCLUDED.commission_max_change_rate, api.validators.commission_max_change_rate),
			min_self_delegation = COALESCE(EXCLUDED.min_self_delegation, api.validators.min_self_delegation),
			tokens = EXCLUDED.tokens,
			delegator_shares = EXCLUDED.delegator_shares,
			status = EXCLUDED.status,
			jailed = EXCLUDED.jailed,
			updated_at = NOW()`,
		[
			operatorAddress,
			desc.moniker || null,
			desc.identity || null,
			desc.website || null,
			desc.details || null,
			parseRate(rates.rate),
			parseRate(rates.maxRate),
			parseRate(rates.maxChangeRate),
			parseNum(validator.minSelfDelegation),
			parseNum(validator.tokens),
			parseNum(validator.delegatorShares),
			status,
			validator.jailed || false,
		]
	)
}

/** Runs one refresh cycle */
async function refreshValidators(pool: pg.Pool): Promise<void> {
	const startTime = Date.now()
	console.log(`[ValidatorRefresh] Starting refresh...`)

	const [validators, pool_info] = await Promise.all([
		fetchValidators(),
		fetchStakingPool().catch(err => {
			console.warn(`[ValidatorRefresh] Staking pool fetch failed (non-fatal): ${err.message}`)
			return { bondedTokens: '0', notBondedTokens: '0' }
		}),
	])

	console.log(`[ValidatorRefresh] Fetched ${validators.length} validators from chain`)
	console.log(`[ValidatorRefresh] Pool: bonded=${pool_info.bondedTokens}, not_bonded=${pool_info.notBondedTokens}`)

	const client = await pool.connect()
	let updated = 0
	let errors = 0

	try {
		await client.query('BEGIN')

		// Build set of operator addresses from chain response
		const chainAddresses = new Set(validators.map((v: any) => v.operatorAddress || '').filter(Boolean))

		for (const validator of validators) {
			try {
				await upsertValidator(client, validator)
				updated++
			} catch (err: any) {
				errors++
				console.error(`[ValidatorRefresh] Error upserting ${validator.operatorAddress}: ${err.message}`)
			}
		}

		// Mark validators in DB that are no longer on chain as UNBONDED
		// These are "ghost" validators that were removed from chain state
		const { rowCount: ghostCount } = await client.query(
			`UPDATE api.validators
			 SET status = 'BOND_STATUS_UNBONDED', jailed = true, updated_at = NOW()
			 WHERE status = 'BOND_STATUS_BONDED'
			   AND operator_address NOT IN (SELECT unnest($1::text[]))`,
			[Array.from(chainAddresses)]
		)
		if (ghostCount && ghostCount > 0) {
			console.log(`[ValidatorRefresh] Marked ${ghostCount} ghost validators as unbonded`)
		}

		await client.query('COMMIT')
	} catch (err) {
		await client.query('ROLLBACK')
		throw err
	} finally {
		client.release()
	}

	const elapsed = Date.now() - startTime
	console.log(`[ValidatorRefresh] Done in ${elapsed}ms: ${updated} updated, ${errors} errors`)
}

/** Refreshes analytics materialized views concurrently */
async function refreshMaterializedViews(pool: pg.Pool): Promise<void> {
	const startTime = Date.now()
	console.log(`[MVRefresh] Refreshing analytics views...`)

	const views = [
		'api.mv_daily_tx_stats',
		'api.mv_hourly_tx_stats',
		'api.mv_message_type_stats',
		'api.mv_validator_delegator_counts',
		'api.mv_daily_rewards',
		'api.mv_validator_leaderboard',
		'api.mv_chain_stats',
		'api.mv_network_overview',
		'api.mv_hourly_rewards',
	]

	const client = await pool.connect()
	try {
		for (const view of views) {
			try {
				await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`)
			} catch (err: any) {
				// View may not exist yet if migration hasn't run
				console.warn(`[MVRefresh] Skipping ${view}: ${err.message}`)
			}
		}
	} finally {
		client.release()
	}

	const elapsed = Date.now() - startTime
	console.log(`[MVRefresh] Done in ${elapsed}ms (${views.length} views)`)
}

/** Main loop */
async function main() {
	const pool = new Pool({ connectionString: DATABASE_URL })

	console.log(`[ValidatorRefresh] Starting validator refresh service`)
	console.log(`[ValidatorRefresh] Chain query URL: ${CHAIN_QUERY_URL}`)
	console.log(`[ValidatorRefresh] Refresh interval: ${REFRESH_INTERVAL_MS}ms (${REFRESH_INTERVAL_MS / 60000} min)`)
	console.log(`[MVRefresh] Materialized view refresh every ${MV_REFRESH_EVERY_N} cycles`)

	let cycle = 0

	// Run immediately on startup
	try {
		await refreshValidators(pool)
	} catch (err: any) {
		console.error(`[ValidatorRefresh] Initial refresh failed: ${err.message}`)
	}

	try {
		await refreshMaterializedViews(pool)
	} catch (err: any) {
		console.error(`[MVRefresh] Initial refresh failed: ${err.message}`)
	}

	// Then run on interval
	while (true) {
		await new Promise(resolve => setTimeout(resolve, REFRESH_INTERVAL_MS))
		cycle++

		try {
			await refreshValidators(pool)
		} catch (err: any) {
			console.error(`[ValidatorRefresh] Refresh failed: ${err.message}`)
		}

		if (cycle % MV_REFRESH_EVERY_N === 0) {
			try {
				await refreshMaterializedViews(pool)
			} catch (err: any) {
				console.error(`[MVRefresh] Refresh failed: ${err.message}`)
			}
		}
	}
}

main().catch(err => {
	console.error('[ValidatorRefresh] Fatal error:', err)
	process.exit(1)
})
