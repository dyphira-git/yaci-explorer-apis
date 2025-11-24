-- Migration: 003_evm_views.sql
-- Description: EVM-specific views for transaction and address lookups
-- These views support EVM hash searches in universal_search

-- =============================================================================
-- 1. EVM Transaction Map View
-- Maps Ethereum tx hashes to Cosmos tx hashes
-- =============================================================================

CREATE OR REPLACE VIEW api.evm_tx_map AS
SELECT
  e.id AS tx_id,
  MAX(CASE WHEN e.attr_key = 'ethereumTxHash' THEN e.attr_value END) AS ethereum_tx_hash,
  t.height,
  t.timestamp
FROM api.events_main e
JOIN api.transactions_main t ON e.id = t.id
WHERE e.event_type = 'ethereum_tx'
GROUP BY e.id, t.height, t.timestamp;

GRANT SELECT ON api.evm_tx_map TO web_anon;

-- =============================================================================
-- 2. EVM Address Activity View
-- Tracks EVM addresses and their transaction counts
-- =============================================================================

CREATE OR REPLACE VIEW api.evm_address_activity AS
WITH evm_addresses AS (
  SELECT
    e.id,
    t.timestamp,
    MAX(CASE WHEN e.attr_key = 'recipient' THEN e.attr_value END) AS recipient
  FROM api.events_main e
  JOIN api.transactions_main t ON e.id = t.id
  WHERE e.event_type = 'ethereum_tx'
  GROUP BY e.id, t.timestamp
)
SELECT
  recipient AS address,
  COUNT(*) AS tx_count,
  MIN(timestamp) AS first_seen,
  MAX(timestamp) AS last_seen
FROM evm_addresses
WHERE recipient IS NOT NULL AND recipient LIKE '0x%'
GROUP BY recipient;

GRANT SELECT ON api.evm_address_activity TO web_anon;
