#!/usr/bin/env npx tsx
/**
 * Chain Query Service
 *
 * HTTP proxy for direct chain gRPC queries.
 * Extensible design to add more query types as needed.
 *
 * Current endpoints:
 * - GET /chain/balances/:address - Get account balances
 * - GET /chain/spendable/:address - Get spendable balances
 * - GET /chain/supply/:denom - Get supply of a denom
 * - GET /chain/staking/validators - Get all validators
 * - GET /chain/staking/pool - Get staking pool info
 * - GET /chain/health - Health check
 */

import * as http from 'http'
import * as grpc from '@grpc/grpc-js'
import protobuf from 'protobufjs'

const GRPC_ENDPOINT = process.env.CHAIN_GRPC_ENDPOINT || 'localhost:9090'
const PORT = parseInt(process.env.CHAIN_QUERY_PORT || '3001', 10)
// Default to TLS unless explicitly set to insecure
const INSECURE = process.env.YACI_INSECURE === 'true'

// Proto definitions for chain queries
const PROTOS = {
	auth: `
		syntax = "proto3";
		package cosmos.auth.v1beta1;

		service Query {
			rpc Account(QueryAccountRequest) returns (QueryAccountResponse);
		}

		message QueryAccountRequest {
			string address = 1;
		}

		message QueryAccountResponse {
			Any account = 1;
		}

		message Any {
			string type_url = 1;
			bytes value = 2;
		}
	`,
	tx: `
		syntax = "proto3";
		package cosmos.tx.v1beta1;

		service Service {
			rpc BroadcastTx(BroadcastTxRequest) returns (BroadcastTxResponse);
		}

		message BroadcastTxRequest {
			bytes tx_bytes = 1;
			int32 mode = 2;
		}

		message BroadcastTxResponse {
			TxResponse tx_response = 1;
		}

		message TxResponse {
			int64 height = 1;
			string txhash = 2;
			string codespace = 3;
			uint32 code = 4;
			string data = 5;
			string raw_log = 6;
			repeated AbciMessageLog logs = 7;
			string info = 8;
			int64 gas_wanted = 9;
			int64 gas_used = 10;
			Any tx = 11;
			string timestamp = 12;
			repeated Event events = 13;
		}

		message AbciMessageLog {
			uint32 msg_index = 1;
			string log = 2;
			repeated StringEvent events = 3;
		}

		message StringEvent {
			string type = 1;
			repeated Attribute attributes = 2;
		}

		message Attribute {
			string key = 1;
			string value = 2;
		}

		message Event {
			string type = 1;
			repeated EventAttribute attributes = 2;
		}

		message EventAttribute {
			bytes key = 1;
			bytes value = 2;
			bool index = 3;
		}

		message Any {
			string type_url = 1;
			bytes value = 2;
		}
	`,
	bank: `
		syntax = "proto3";
		package cosmos.bank.v1beta1;

		service Query {
			rpc AllBalances(QueryAllBalancesRequest) returns (QueryAllBalancesResponse);
			rpc SpendableBalances(QuerySpendableBalancesRequest) returns (QuerySpendableBalancesResponse);
			rpc TotalSupply(QueryTotalSupplyRequest) returns (QueryTotalSupplyResponse);
			rpc SupplyOf(QuerySupplyOfRequest) returns (QuerySupplyOfResponse);
		}

		message QueryAllBalancesRequest {
			string address = 1;
			PageRequest pagination = 2;
		}

		message QueryAllBalancesResponse {
			repeated Coin balances = 1;
			PageResponse pagination = 2;
		}

		message QuerySpendableBalancesRequest {
			string address = 1;
			PageRequest pagination = 2;
		}

		message QuerySpendableBalancesResponse {
			repeated Coin balances = 1;
			PageResponse pagination = 2;
		}

		message QueryTotalSupplyRequest {
			PageRequest pagination = 1;
		}

		message QueryTotalSupplyResponse {
			repeated Coin supply = 1;
			PageResponse pagination = 2;
		}

		message QuerySupplyOfRequest {
			string denom = 1;
		}

		message QuerySupplyOfResponse {
			Coin amount = 1;
		}

		message Coin {
			string denom = 1;
			string amount = 2;
		}

		message PageRequest {
			bytes key = 1;
			uint64 offset = 2;
			uint64 limit = 3;
			bool count_total = 4;
			bool reverse = 5;
		}

		message PageResponse {
			bytes next_key = 1;
			uint64 total = 2;
		}
	`,
	staking: `
		syntax = "proto3";
		package cosmos.staking.v1beta1;

		service Query {
			rpc Validators(QueryValidatorsRequest) returns (QueryValidatorsResponse);
			rpc Validator(QueryValidatorRequest) returns (QueryValidatorResponse);
			rpc Pool(QueryPoolRequest) returns (QueryPoolResponse);
		}

		message QueryValidatorsRequest {
			string status = 1;
			PageRequest pagination = 2;
		}

		message QueryValidatorsResponse {
			repeated Validator validators = 1;
			PageResponse pagination = 2;
		}

		message QueryValidatorRequest {
			string validator_addr = 1;
		}

		message QueryValidatorResponse {
			Validator validator = 1;
		}

		message QueryPoolRequest {}

		message QueryPoolResponse {
			Pool pool = 1;
		}

		message Validator {
			string operator_address = 1;
			bytes consensus_pubkey = 2;
			bool jailed = 3;
			int32 status = 4;
			string tokens = 5;
			string delegator_shares = 6;
			Description description = 7;
			int64 unbonding_height = 8;
			string unbonding_time = 9;
			Commission commission = 10;
			string min_self_delegation = 11;
		}

		message Description {
			string moniker = 1;
			string identity = 2;
			string website = 3;
			string security_contact = 4;
			string details = 5;
		}

		message Commission {
			CommissionRates commission_rates = 1;
			string update_time = 2;
		}

		message CommissionRates {
			string rate = 1;
			string max_rate = 2;
			string max_change_rate = 3;
		}

		message Pool {
			string not_bonded_tokens = 1;
			string bonded_tokens = 2;
		}

		message PageRequest {
			bytes key = 1;
			uint64 offset = 2;
			uint64 limit = 3;
			bool count_total = 4;
			bool reverse = 5;
		}

		message PageResponse {
			bytes next_key = 1;
			uint64 total = 2;
		}
	`,
}

// Generic gRPC query client
class ChainQueryClient {
	private endpoint: string
	private credentials: grpc.ChannelCredentials
	private roots: Map<string, protobuf.Root> = new Map()
	private stubs: Map<string, any> = new Map()

	constructor(endpoint: string, insecure: boolean) {
		this.endpoint = endpoint
		this.credentials = insecure
			? grpc.credentials.createInsecure()
			: grpc.credentials.createSsl()

		console.log(`[ChainQuery] Connecting to ${endpoint} (insecure: ${insecure})`)

		// Parse all proto definitions
		for (const [name, proto] of Object.entries(PROTOS)) {
			const root = protobuf.Root.fromJSON(protobuf.parse(proto).root.toJSON())
			this.roots.set(name, root)
		}
	}

	// Create a stub for a specific service
	private getStub(service: string, methods: Record<string, { path: string; requestType: string; responseType: string }>): any {
		const cacheKey = service
		if (this.stubs.has(cacheKey)) {
			return this.stubs.get(cacheKey)
		}

		// Extract module name from service (e.g., "cosmos.bank.v1beta1.Query" -> "bank")
		const parts = service.split('.')
		const moduleName = parts.length >= 2 ? parts[1] : parts[0]
		const root = this.roots.get(moduleName)
		if (!root) {
			throw new Error(`Unknown module: ${moduleName} (from service: ${service})`)
		}

		const methodDefs: Record<string, any> = {}

		for (const [methodName, config] of Object.entries(methods)) {
			const requestType = root.lookupType(config.requestType)
			const responseType = root.lookupType(config.responseType)

			methodDefs[methodName] = {
				path: config.path,
				requestStream: false,
				responseStream: false,
				requestSerialize: (value: any) => Buffer.from(requestType.encode(requestType.create(value)).finish()),
				requestDeserialize: (buffer: Buffer) => requestType.decode(buffer),
				responseSerialize: (value: any) => Buffer.from(responseType.encode(value).finish()),
				responseDeserialize: (buffer: Buffer) => responseType.decode(buffer),
			}
		}

		const ClientClass = grpc.makeGenericClientConstructor(methodDefs, service, {})
		const stub = new ClientClass(this.endpoint, this.credentials)
		this.stubs.set(cacheKey, stub)
		return stub
	}

	// Bank queries
	async getAllBalances(address: string): Promise<any> {
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
			AllBalances: {
				path: '/cosmos.bank.v1beta1.Query/AllBalances',
				requestType: 'cosmos.bank.v1beta1.QueryAllBalancesRequest',
				responseType: 'cosmos.bank.v1beta1.QueryAllBalancesResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.AllBalances({ address, pagination: { limit: 100 } }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					balances: (response.balances || []).map((b: any) => ({
						denom: b.denom,
						amount: b.amount,
					})),
				})
			})
		})
	}

	async getSpendableBalances(address: string): Promise<any> {
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
			SpendableBalances: {
				path: '/cosmos.bank.v1beta1.Query/SpendableBalances',
				requestType: 'cosmos.bank.v1beta1.QuerySpendableBalancesRequest',
				responseType: 'cosmos.bank.v1beta1.QuerySpendableBalancesResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.SpendableBalances({ address, pagination: { limit: 100 } }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					balances: (response.balances || []).map((b: any) => ({
						denom: b.denom,
						amount: b.amount,
					})),
				})
			})
		})
	}

	async getSupplyOf(denom: string): Promise<any> {
		const stub = this.getStub('cosmos.bank.v1beta1.Query', {
			SupplyOf: {
				path: '/cosmos.bank.v1beta1.Query/SupplyOf',
				requestType: 'cosmos.bank.v1beta1.QuerySupplyOfRequest',
				responseType: 'cosmos.bank.v1beta1.QuerySupplyOfResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.SupplyOf({ denom }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					amount: response.amount ? {
						denom: response.amount.denom,
						amount: response.amount.amount,
					} : null,
				})
			})
		})
	}

	// Staking queries

	/** Returns all staking gRPC method definitions (shared across staking methods to avoid stub cache conflicts) */
	private stakingMethods() {
		return {
			Validators: {
				path: '/cosmos.staking.v1beta1.Query/Validators',
				requestType: 'cosmos.staking.v1beta1.QueryValidatorsRequest',
				responseType: 'cosmos.staking.v1beta1.QueryValidatorsResponse',
			},
			Pool: {
				path: '/cosmos.staking.v1beta1.Query/Pool',
				requestType: 'cosmos.staking.v1beta1.QueryPoolRequest',
				responseType: 'cosmos.staking.v1beta1.QueryPoolResponse',
			},
		}
	}

	/** Fetches validators from chain, optionally filtered by status */
	async getValidators(status?: string): Promise<any> {
		const stub = this.getStub('cosmos.staking.v1beta1.Query', this.stakingMethods())

		const statusMap: Record<string, number> = {
			'BOND_STATUS_UNSPECIFIED': 0,
			'BOND_STATUS_UNBONDED': 1,
			'BOND_STATUS_UNBONDING': 2,
			'BOND_STATUS_BONDED': 3,
		}

		return new Promise((resolve, reject) => {
			stub.Validators(
				{ status: status || '', pagination: { limit: 500, countTotal: true } },
				(err: Error | null, response: any) => {
					if (err) {
						reject(err)
						return
					}
					const validators = (response.validators || []).map((v: any) => ({
						operatorAddress: v.operatorAddress || v.operator_address || '',
						jailed: v.jailed || false,
						status: v.status,
						tokens: v.tokens || '0',
						delegatorShares: v.delegatorShares || v.delegator_shares || '0',
						description: {
							moniker: v.description?.moniker || '',
							identity: v.description?.identity || '',
							website: v.description?.website || '',
							securityContact: v.description?.securityContact || v.description?.security_contact || '',
							details: v.description?.details || '',
						},
						commission: {
							commissionRates: {
								rate: v.commission?.commissionRates?.rate || v.commission?.commission_rates?.rate || '0',
								maxRate: v.commission?.commissionRates?.maxRate || v.commission?.commission_rates?.max_rate || '0',
								maxChangeRate: v.commission?.commissionRates?.maxChangeRate || v.commission?.commission_rates?.max_change_rate || '0',
							},
						},
						minSelfDelegation: v.minSelfDelegation || v.min_self_delegation || '0',
						unbondingHeight: v.unbondingHeight || v.unbonding_height || '0',
					}))
					resolve({ validators })
				}
			)
		})
	}

	/** Fetches the staking pool (bonded/not-bonded totals) */
	async getStakingPool(): Promise<any> {
		const stub = this.getStub('cosmos.staking.v1beta1.Query', this.stakingMethods())

		return new Promise((resolve, reject) => {
			stub.Pool({}, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}
				resolve({
					pool: {
						notBondedTokens: response.pool?.notBondedTokens || response.pool?.not_bonded_tokens || '0',
						bondedTokens: response.pool?.bondedTokens || response.pool?.bonded_tokens || '0',
					},
				})
			})
		})
	}

	// Auth queries

	/** Fetches account info (account number, sequence) for signing transactions */
	async getAccount(address: string): Promise<any> {
		const stub = this.getStub('cosmos.auth.v1beta1.Query', {
			Account: {
				path: '/cosmos.auth.v1beta1.Query/Account',
				requestType: 'cosmos.auth.v1beta1.QueryAccountRequest',
				responseType: 'cosmos.auth.v1beta1.QueryAccountResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.Account({ address }, (err: Error | null, response: any) => {
				if (err) {
					// Check if account not found (new account)
					if (err.message?.includes('NotFound') || err.message?.includes('not found')) {
						resolve({
							account: null,
							accountNumber: 0,
							sequence: 0,
						})
						return
					}
					reject(err)
					return
				}

				// The account comes as Any type with encoded value
				// For EthAccount, the structure is different from BaseAccount
				const account = response.account
				if (!account) {
					resolve({ account: null, accountNumber: 0, sequence: 0 })
					return
				}

				// Return raw account data - let the client parse it
				// We return the type_url so client knows how to decode
				resolve({
					account: {
						type_url: account.type_url || account.typeUrl || '',
						// Value is bytes - encode as base64 for JSON transport
						value: account.value ? Buffer.from(account.value).toString('base64') : null,
					},
				})
			})
		})
	}

	// Transaction broadcast

	/** Broadcasts a signed transaction to the chain */
	async broadcastTx(txBytes: Buffer, mode: number = 2): Promise<any> {
		const stub = this.getStub('cosmos.tx.v1beta1.Service', {
			BroadcastTx: {
				path: '/cosmos.tx.v1beta1.Service/BroadcastTx',
				requestType: 'cosmos.tx.v1beta1.BroadcastTxRequest',
				responseType: 'cosmos.tx.v1beta1.BroadcastTxResponse',
			},
		})

		return new Promise((resolve, reject) => {
			stub.BroadcastTx({ tx_bytes: txBytes, mode }, (err: Error | null, response: any) => {
				if (err) {
					reject(err)
					return
				}

				const txResponse = response.tx_response || response.txResponse || {}
				resolve({
					tx_response: {
						height: txResponse.height?.toString() || '0',
						txhash: txResponse.txhash || txResponse.txHash || '',
						codespace: txResponse.codespace || '',
						code: txResponse.code || 0,
						data: txResponse.data || '',
						raw_log: txResponse.raw_log || txResponse.rawLog || '',
						info: txResponse.info || '',
						gas_wanted: txResponse.gas_wanted?.toString() || txResponse.gasWanted?.toString() || '0',
						gas_used: txResponse.gas_used?.toString() || txResponse.gasUsed?.toString() || '0',
					},
				})
			})
		})
	}
}

// Initialize client
let client: ChainQueryClient

function initClient() {
	client = new ChainQueryClient(GRPC_ENDPOINT, INSECURE)
}

// Route handlers
type RouteHandler = (params: Record<string, string>, query: URLSearchParams) => Promise<any>

const routes: Array<{ pattern: RegExp; handler: RouteHandler }> = [
	// GET /chain/balances/:address
	{
		pattern: /^\/chain\/balances\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getAllBalances(params.address),
	},
	// GET /chain/spendable/:address
	{
		pattern: /^\/chain\/spendable\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getSpendableBalances(params.address),
	},
	// GET /chain/supply/:denom
	{
		pattern: /^\/chain\/supply\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getSupplyOf(params.denom),
	},
	// GET /chain/staking/validators?status=BOND_STATUS_BONDED
	{
		pattern: /^\/chain\/staking\/validators$/,
		handler: async (_params, query) => client.getValidators(query.get('status') || undefined),
	},
	// GET /chain/staking/pool
	{
		pattern: /^\/chain\/staking\/pool$/,
		handler: async () => client.getStakingPool(),
	},
	// GET /chain/auth/account/:address - Get account info for signing
	{
		pattern: /^\/chain\/auth\/account\/([a-zA-Z0-9]+)$/,
		handler: async (params) => client.getAccount(params.address),
	},
]

// Helper to read request body
async function readBody(req: http.IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let body = ''
		req.on('data', (chunk) => { body += chunk.toString() })
		req.on('end', () => resolve(body))
		req.on('error', reject)
	})
}

// HTTP server
const server = http.createServer(async (req, res) => {
	// CORS headers
	res.setHeader('Access-Control-Allow-Origin', '*')
	res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
	res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
	res.setHeader('Content-Type', 'application/json')

	if (req.method === 'OPTIONS') {
		res.writeHead(204)
		res.end()
		return
	}

	const url = new URL(req.url || '/', `http://localhost:${PORT}`)

	// Health check
	if (url.pathname === '/chain/health') {
		res.writeHead(200)
		res.end(JSON.stringify({ status: 'ok', endpoint: GRPC_ENDPOINT }))
		return
	}

	// POST /chain/tx/broadcast - Broadcast signed transaction
	if (url.pathname === '/chain/tx/broadcast' && req.method === 'POST') {
		try {
			const body = await readBody(req)
			const data = JSON.parse(body)

			// Expect tx_bytes as base64 encoded string
			if (!data.tx_bytes) {
				res.writeHead(400)
				res.end(JSON.stringify({ error: 'Missing tx_bytes in request body' }))
				return
			}

			const txBytes = Buffer.from(data.tx_bytes, 'base64')
			// mode: 1 = BROADCAST_MODE_BLOCK, 2 = BROADCAST_MODE_SYNC, 3 = BROADCAST_MODE_ASYNC
			const mode = data.mode || 2

			const result = await client.broadcastTx(txBytes, mode)
			res.writeHead(200)
			res.end(JSON.stringify(result))
		} catch (err: any) {
			console.error(`[ChainQuery] Broadcast error:`, err.message)
			res.writeHead(500)
			res.end(JSON.stringify({ error: err.message }))
		}
		return
	}

	// Match GET routes
	for (const route of routes) {
		const match = url.pathname.match(route.pattern)
		if (match && req.method === 'GET') {
			const params: Record<string, string> = {}

			// Extract named params based on pattern groups
			if (url.pathname.includes('/balances/')) params.address = match[1]
			else if (url.pathname.includes('/spendable/')) params.address = match[1]
			else if (url.pathname.includes('/supply/')) params.denom = match[1]
			else if (url.pathname.includes('/auth/account/')) params.address = match[1]

			try {
				const result = await route.handler(params, url.searchParams)
				res.writeHead(200)
				res.end(JSON.stringify(result))
			} catch (err: any) {
				console.error(`[ChainQuery] Error:`, err.message)
				res.writeHead(500)
				res.end(JSON.stringify({ error: err.message }))
			}
			return
		}
	}

	// 404
	res.writeHead(404)
	res.end(JSON.stringify({ error: 'Not found', endpoints: [
		'GET /chain/health',
		'GET /chain/balances/:address',
		'GET /chain/spendable/:address',
		'GET /chain/supply/:denom',
		'GET /chain/staking/validators?status=BOND_STATUS_BONDED',
		'GET /chain/staking/pool',
		'GET /chain/auth/account/:address',
		'POST /chain/tx/broadcast',
	]}))
})

// Start server
initClient()
server.listen(PORT, () => {
	console.log(`[ChainQuery] Running on port ${PORT}`)
	console.log(`[ChainQuery] gRPC endpoint: ${GRPC_ENDPOINT}`)
	console.log(`[ChainQuery] Endpoints:`)
	console.log(`  GET  /chain/health`)
	console.log(`  GET  /chain/balances/:address`)
	console.log(`  GET  /chain/spendable/:address`)
	console.log(`  GET  /chain/supply/:denom`)
	console.log(`  GET  /chain/staking/validators`)
	console.log(`  GET  /chain/staking/pool`)
	console.log(`  GET  /chain/auth/account/:address`)
	console.log(`  POST /chain/tx/broadcast`)
})
