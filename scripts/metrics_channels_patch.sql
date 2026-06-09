-- metrics_channels_patch.sql
-- A.T.L.A.S. LIVE / MINED channel split (CHANNEL_REMAP_PLAN.md sec 3).
-- READ-ONLY: this is a SELECT-only classification view. It adds NO columns,
-- NO constraints, NO migration -- it derives `channel` at metrics-time from
-- existing data, so it cannot harm the 1.27M corpus and is fully reversible
-- (DROP VIEW). The metrics emitter reads this view to populate
-- metrics.channels.{live,mined} in atlas-metrics.json.
--
-- LIVE = a row whose defining freshness signal is genuinely same-day-NEW
--        (rolling 72h). MINED = everything else (batch/periodic/backfill).
-- The bright line: a source's CADENCE does not make it LIVE -- only a
-- per-row same-day signal date does (e.g. Chicago licenses update daily but
-- only rows with date_issued within 72h count LIVE).
--
-- NOTE on column names: ATLAS source_record/business column names are
-- introspected on the box. The view below uses the documented columns
-- (source_code, first_seen). If the live schema names the freshness date
-- differently (e.g. signal_date / date_issued surfaced into source_record),
-- adjust the COALESCE in the freshness expression on the box before applying.
-- This file is the reference; confirm column names with \d atlas.source_record.

CREATE OR REPLACE VIEW atlas.v_channel_classification AS
SELECT
  sr.business_id,
  sr.source_code,
  sr.first_seen,
  CASE
    -- pure-LIVE sources: always live (their existence == a fresh signal)
    WHEN sr.source_code IN ('ct_new_ssl', 'mx_qualify', 'nrd', 'nrd_whoisds', 'nrd_czds')
      THEN 'live'
    -- split sources: LIVE only when the row's signal is within the rolling 72h window
    WHEN sr.source_code IN ('sos_new_business', 'edgar_formd')
         OR sr.source_code LIKE 'socrata_%_license'
         OR sr.source_code LIKE 'socrata_license_%'
      THEN CASE
             WHEN sr.first_seen >= (now() - interval '72 hours') THEN 'live'
             ELSE 'mined'
           END
    -- everything else (Chicago/NYC bulk, EDGAR seed, nonprofits, Overture, OSM, ...)
    ELSE 'mined'
  END AS channel
FROM atlas.source_record sr;

-- Convenience rollup the metrics emitter can SELECT directly.
CREATE OR REPLACE VIEW atlas.v_channel_metrics AS
SELECT
  channel,
  source_code,
  count(*)                                                        AS rows_total,
  count(*) FILTER (WHERE first_seen >= current_date)              AS rows_today,
  count(*) FILTER (WHERE first_seen >= now() - interval '72 hours') AS rows_rolling_72h
FROM atlas.v_channel_classification
GROUP BY channel, source_code;

-- The emitter should produce (pseudocode against these views):
--   metrics.channels.live.today      = SUM(rows_today)        WHERE channel='live'
--   metrics.channels.live.rolling_72h= SUM(rows_rolling_72h)  WHERE channel='live'
--   metrics.channels.live.sources    = { source_code: rows_today } WHERE channel='live'
--   metrics.channels.mined.total     = SUM(rows_total)        WHERE channel='mined'
--   metrics.channels.mined.sources   = { source_code: rows_total } WHERE channel='mined'
--   metrics.channels.live.status     = 'live' if today>0 else 'connecting'
-- business_total is UNCHANGED (still LIVE union MINED).

-- Reversal (if ever needed):
--   DROP VIEW IF EXISTS atlas.v_channel_metrics;
--   DROP VIEW IF EXISTS atlas.v_channel_classification;
