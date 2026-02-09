-- Migration 057: Fix double-division in rewards extraction
--
-- Problem: extract_rewards_from_events() divides by 1e18 before storing,
-- converting raw arai to display RAI. The frontend then divides by 10^decimals
-- again (using the chain config for 'arai'), resulting in near-zero display values.
--
-- Solution: Store raw base units (like validators.tokens does) and let the
-- frontend handle the single conversion to display units.
--
-- Steps:
--   1. Fix the trigger function (remove / 1e18)
--   2. Truncate + re-backfill validator_rewards from finalize_block_events
--   3. Truncate + rebuild rt_hourly_rewards from corrected data
--   4. Refresh mv_daily_rewards materialized view

BEGIN;

-- ============================================================================
-- 1. Fix extract_rewards_from_events() - remove / 1e18
-- ============================================================================

CREATE OR REPLACE FUNCTION api.extract_rewards_from_events()
RETURNS TRIGGER AS $$
DECLARE
  events JSONB;
  event_item JSONB;
  event_type TEXT;
  attrs JSONB;
  attr_item JSONB;
  validator_addr TEXT;
  reward_amount NUMERIC;
  commission_amount NUMERIC;
BEGIN
  -- Get finalize_block_events array
  events := COALESCE(
    NEW.data->'finalizeBlockEvents',
    NEW.data->'finalize_block_events',
    '[]'::JSONB
  );

  IF jsonb_array_length(events) = 0 THEN
    RETURN NEW;
  END IF;

  FOR event_item IN SELECT * FROM jsonb_array_elements(events)
  LOOP
    event_type := event_item->>'type';

    -- Build attributes as key-value object
    attrs := '{}';
    FOR attr_item IN SELECT * FROM jsonb_array_elements(COALESCE(event_item->'attributes', '[]'::JSONB))
    LOOP
      attrs := attrs || jsonb_build_object(
        COALESCE(attr_item->>'key', ''),
        COALESCE(attr_item->>'value', '')
      );
    END LOOP;

    -- Handle rewards events (store raw base units, no division)
    IF event_type = 'rewards' THEN
      validator_addr := COALESCE(attrs->>'validator', '');
      reward_amount := COALESCE(
        NULLIF(regexp_replace(attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
        0
      );

      IF validator_addr != '' THEN
        INSERT INTO api.validator_rewards (height, validator_address, rewards)
        VALUES (NEW.height, validator_addr, reward_amount)
        ON CONFLICT (height, validator_address)
        DO UPDATE SET rewards = EXCLUDED.rewards + api.validator_rewards.rewards;
      END IF;
    END IF;

    -- Handle commission events (store raw base units, no division)
    IF event_type = 'commission' THEN
      validator_addr := COALESCE(attrs->>'validator', '');
      commission_amount := COALESCE(
        NULLIF(regexp_replace(attrs->>'amount', '[^0-9.]', '', 'g'), '')::NUMERIC,
        0
      );

      IF validator_addr != '' THEN
        INSERT INTO api.validator_rewards (height, validator_address, commission)
        VALUES (NEW.height, validator_addr, commission_amount)
        ON CONFLICT (height, validator_address)
        DO UPDATE SET commission = EXCLUDED.commission + api.validator_rewards.commission;
      END IF;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 2. Truncate + re-backfill validator_rewards from finalize_block_events
-- ============================================================================

TRUNCATE api.validator_rewards;

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
GROUP BY f.height, f.attributes->>'validator';

-- ============================================================================
-- 3. Truncate + rebuild rt_hourly_rewards from corrected validator_rewards
-- ============================================================================

TRUNCATE api.rt_hourly_rewards;

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
  commission = EXCLUDED.commission;

-- ============================================================================
-- 4. Refresh mv_daily_rewards materialized view
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_daily_rewards') THEN
    REFRESH MATERIALIZED VIEW api.mv_daily_rewards;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'api' AND matviewname = 'mv_validator_leaderboard') THEN
    REFRESH MATERIALIZED VIEW api.mv_validator_leaderboard;
  END IF;
END $$;

COMMIT;
