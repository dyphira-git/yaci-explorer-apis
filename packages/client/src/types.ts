/**
 * Type definitions for YACI Explorer API
 */

// Pagination

export interface Pagination {
	total: number
	limit: number
	offset: number
	has_next: boolean
	has_prev: boolean
}

export interface PaginatedResponse<T> {
	data: T[]
	pagination: Pagination
}

// Transactions

export interface Transaction {
	id: string
	fee: TransactionFee
	memo: string | null
	error: string | null
	height: number
	timestamp: string
	proposal_ids: number[] | null
	messages: Message[]
	events: Event[]
	ingest_error: IngestError | null
}

export interface TransactionDetail extends Transaction {
	evm_data: EvmData | null
	raw_data: unknown
}

export interface TransactionFee {
	amount: Array<{ denom: string; amount: string }>
	gasLimit: string
}

export interface IngestError {
	message: string
	reason: string
	hash: string
}

// Messages

export interface Message {
	id: string
	message_index: number
	type: string
	sender: string | null
	mentions: string[]
	metadata: Record<string, unknown>
}

// Events

export interface Event {
	id: string
	event_index: number
	attr_index: number
	event_type: string
	attr_key: string
	attr_value: string
	msg_index: number | null
}

// EVM

export interface EvmData {
	ethereum_tx_hash: string | null
	recipient: string | null
	gas_used: number | null
	tx_type: number | null
}

// Address

export interface AddressStats {
	address: string
	transaction_count: number
	first_seen: string | null
	last_seen: string | null
	total_sent: number
	total_received: number
}

// Chain Stats

export interface ChainStats {
	latest_block: number
	total_transactions: number
	unique_addresses: number
	avg_block_time: number
	min_block_time: number
	max_block_time: number
	active_validators: number
}

// Search

export interface SearchResult {
	type: 'block' | 'transaction' | 'evm_transaction' | 'address' | 'evm_address'
	value: unknown
	score: number
}

// Blocks

export interface BlockRaw {
	id: number
	data: {
		block: {
			header: {
				height: string
				time: string
				chain_id: string
				proposer_address: string
			}
			data: {
				txs: string[]
			}
			last_commit?: {
				signatures: Array<{
					validator_address: string
					signature: string
				}>
			}
		}
	}
}

// Governance

export interface GovernanceProposal {
	proposal_id: number
	title: string | null
	summary: string | null
	status: string
	submit_time: string
	deposit_end_time: string | null
	voting_start_time: string | null
	voting_end_time: string | null
	proposer: string | null
	tally: {
		yes: string | null
		no: string | null
		abstain: string | null
		no_with_veto: string | null
	}
	last_updated: string
}

export interface ProposalSnapshot {
	proposal_id: number
	status: string
	yes_count: string
	no_count: string
	abstain_count: string
	no_with_veto_count: string
	snapshot_time: string
}

// Delegation Events

export interface DelegationEvent {
	id: string
	tx_hash: string
	event_type: 'DELEGATE' | 'UNDELEGATE' | 'REDELEGATE' | 'CREATE_VALIDATOR'
	delegator_address: string
	validator_address: string
	src_validator_address: string | null
	amount: string | null
	denom: string | null
	timestamp: string | null
	block_height: number | null
	validator_moniker?: string | null
}

export interface DelegatorDelegation {
	validator_address: string
	validator_moniker: string | null
	commission_rate: string | null
	validator_status: string | null
	validator_jailed: boolean | null
	denom: string | null
	total_delegated: string
}

export interface DelegatorDelegationsResponse {
	delegations: DelegatorDelegation[]
	total_staked: string
	validator_count: number
}

export interface DelegatorStats {
	total_delegations: number
	total_undelegations: number
	total_redelegations: number
	first_delegation: string | null
	last_activity: string | null
	unique_validators: number
}
