BEGIN;

-- Migration 068: Stored generated column for block timestamp on blocks_raw
--
-- Extracts (data->'block'->'header'->>'time')::timestamptz into a proper column
-- so queries can use a B-tree index instead of JSONB extraction per row.
--
-- Affected functions:
--   get_blocks_paginated  -- date filtering in WHERE clause
--   trg_rt_hourly_rewards -- block_time lookup per validator_rewards INSERT
--   trg_rt_daily_rewards  -- same

-- ============================================================================
-- 1. Immutable helper for generated column (::timestamptz is STABLE, not
--    IMMUTABLE, because it depends on the TimeZone GUC. Cosmos block
--    timestamps are always RFC3339/UTC so we can safely mark this IMMUTABLE.)
-- ============================================================================

CREATE OR REPLACE FUNCTION api.parse_block_time(data jsonb)
RETURNS timestamptz
LANGUAGE sql IMMUTABLE STRICT
AS $$
  SELECT (data->'block'->'header'->>'time')::timestamptz;
$$;

-- ============================================================================
-- 2. Add stored generated column + descending index
-- ============================================================================

ALTER TABLE api.blocks_raw
  ADD COLUMN IF NOT EXISTS block_time TIMESTAMPTZ
  GENERATED ALWAYS AS (api.parse_block_time(data)) STORED;

CREATE INDEX IF NOT EXISTS idx_blocks_raw_block_time
  ON api.blocks_raw(block_time DESC);

-- ============================================================================
-- 2. Update get_blocks_paginated to use the new column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.get_blocks_paginated(
  _limit int DEFAULT 20,
  _offset int DEFAULT 0,
  _min_tx_count int DEFAULT NULL,
  _from_date timestamp DEFAULT NULL,
  _to_date timestamp DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE
AS $$
  WITH filtered_blocks AS (
    SELECT b.id, b.data, b.tx_count
    FROM api.blocks_raw b
    WHERE
      (_min_tx_count IS NULL OR b.tx_count >= _min_tx_count)
      AND (_from_date IS NULL OR b.block_time >= _from_date)
      AND (_to_date IS NULL OR b.block_time <= _to_date)
    ORDER BY b.id DESC
  ),
  total AS (
    SELECT COUNT(*) AS count FROM filtered_blocks
  ),
  paginated AS (
    SELECT * FROM filtered_blocks
    LIMIT _limit OFFSET _offset
  )
  SELECT jsonb_build_object(
    'data', COALESCE(jsonb_agg(
      jsonb_build_object(
        'id', p.id,
        'data', p.data,
        'tx_count', COALESCE(p.tx_count, 0)
      ) ORDER BY p.id DESC
    ), '[]'::jsonb),
    'pagination', jsonb_build_object(
      'total', (SELECT count FROM total),
      'limit', _limit,
      'offset', _offset,
      'has_next', _offset + _limit < (SELECT count FROM total),
      'has_prev', _offset > 0
    )
  )
  FROM paginated p;
$$;

-- ============================================================================
-- 3. Update trg_rt_hourly_rewards to use block_time column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_rt_hourly_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_hour TIMESTAMPTZ;
BEGIN
  SELECT b.block_time INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_hour := date_trunc('hour', block_time);

  INSERT INTO api.rt_hourly_rewards (hour, rewards, commission)
  VALUES (reward_hour, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (hour) DO UPDATE SET
    rewards = api.rt_hourly_rewards.rewards + COALESCE(NEW.rewards, 0),
    commission = api.rt_hourly_rewards.commission + COALESCE(NEW.commission, 0);

  -- Prune moved to periodic refresh (was causing deadlocks)
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. Update trg_rt_daily_rewards to use block_time column
-- ============================================================================

CREATE OR REPLACE FUNCTION api.trg_rt_daily_rewards()
RETURNS TRIGGER AS $$
DECLARE
  block_time TIMESTAMPTZ;
  reward_date DATE;
BEGIN
  SELECT b.block_time INTO block_time
  FROM api.blocks_raw b
  WHERE b.id = NEW.height;

  IF block_time IS NULL THEN
    block_time := NOW();
  END IF;

  reward_date := block_time::DATE;

  INSERT INTO api.rt_daily_rewards (date, total_rewards, total_commission)
  VALUES (reward_date, COALESCE(NEW.rewards, 0), COALESCE(NEW.commission, 0))
  ON CONFLICT (date) DO UPDATE SET
    total_rewards = api.rt_daily_rewards.total_rewards + COALESCE(NEW.rewards, 0),
    total_commission = api.rt_daily_rewards.total_commission + COALESCE(NEW.commission, 0);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
