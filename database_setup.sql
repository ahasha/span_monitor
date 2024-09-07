-- Requires timescaledb extension to be installed.
-- See https://supabase.com/docs/guides/database/extensions/timescaledb
DROP TABLE IF EXISTS main_energy;
DROP TABLE IF EXISTS branch_energy;
DROP TYPE relay_state_type;
CREATE TYPE relay_state_type AS ENUM ('CLOSED', 'OPEN');
CREATE TABLE main_energy (
    time TIMESTAMPTZ NOT NULL,
    relay_state relay_state_type,
    main_meter_produced_energy_wh DOUBLE PRECISION,
    main_meter_consumed_energy_wh DOUBLE PRECISION,
    instant_grid_power_w DOUBLE PRECISION,
    feed_through_power_w DOUBLE PRECISION,
    feed_through_produced_energy_wh DOUBLE PRECISION,
    feed_through_consumed_energy_wh DOUBLE PRECISION,
    grid_sample_start_ms BIGINT,
    grid_sample_end_ms BIGINT,
    dsm_grid_state TEXT,
    dsm_state TEXT,
    current_run_config TEXT
);
select create_hypertable('main_energy', 'time');
CREATE TABLE branch_energy (
    time TIMESTAMPTZ NOT NULL,
    branch_id INT NOT NULL,
    relay_state relay_state_type,
    instant_power_w DOUBLE PRECISION,
    imported_active_energy_wh DOUBLE PRECISION,
    exported_active_energy_wh DOUBLE PRECISION,
    measure_start_ts_ms BIGINT,
    measure_duration_ms BIGINT,
    is_measure_valid BOOLEAN
);
select create_hypertable('branch_energy', 'time');
-- metadata tables
CREATE TABLE IF NOT EXISTS branch_to_circuit (
    branch_id INT PRIMARY KEY,
    circuit_id TEXT,
    name TEXT
);
-- set up retention and aggregation policy
DROP VIEW IF EXISTS branch_energy_hourly;
CREATE MATERIALIZED VIEW branch_energy_hourly (
    time
    , branch_id
    , num_measurements
    , avg_instant_power_w
    , imported_active_energy_wh
    , exported_active_energy_wh
    , measure_start_ts_ms
    , measure_duration_ms
)
WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 hour', b.time) as time
    , b.branch_id
    , count(*) as num_measurements
    , avg(b.instant_power_w) as avg_instant_power_w
    , max(b.imported_active_energy_wh) as imported_active_energy_wh
    , max(b.exported_active_energy_wh) as exported_active_energy_wh
    , min(b.measure_start_ts_ms) as measure_start_ts_ms
    , sum(b.measure_duration_ms) as measure_duration_ms
  FROM branch_energy b
  GROUP BY (1, 2)
WITH NO DATA;
CALL refresh_continuous_aggregate('branch_energy_hourly', NULL, localtimestamp - INTERVAL '1 hour');
SELECT add_continuous_aggregate_policy('branch_energy_hourly',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');
DROP VIEW IF EXISTS main_energy_hourly;
CREATE MATERIALIZED VIEW main_energy_hourly (
    time
    , num_measurements
    , main_meter_produced_energy_wh
    , main_meter_consumed_energy_wh
    , avg_instant_grid_power_w
    , avg_feed_through_power_w
    , feed_through_produced_energy_wh
    , feed_through_consumed_energy_wh
    , min_grid_sample_start_ms
    , max_grid_sample_end_ms
)
WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 hour', m.time) as time
    , count(*) as num_measurements
    , max(m.main_meter_produced_energy_wh) as main_meter_produced_energy_wh
    , max(m.main_meter_consumed_energy_wh) as main_meter_consumed_energy_wh
    , avg(m.instant_grid_power_w) as avg_instant_grid_power_w
    , avg(m.feed_through_power_w) as avg_feed_through_power_w
    , max(m.feed_through_produced_energy_wh) as feed_through_produced_energy_wh
    , max(m.feed_through_consumed_energy_wh) as feed_through_consumed_energy_wh
    , min(m.grid_sample_start_ms) as min_grid_sample_start_ms
    , max(m.grid_sample_end_ms) as max_grid_sample_end_ms
  FROM main_energy m
  GROUP BY 1
WITH NO DATA;
CALL refresh_continuous_aggregate('main_energy_hourly', NULL, localtimestamp - INTERVAL '1 hour');
SELECT add_continuous_aggregate_policy('main_energy_hourly',
  start_offset => INTERVAL '1 day',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

-- retention policies
SELECT add_retention_policy('main_energy', INTERVAL '1 week');
SELECT add_retention_policy('branch_energy', INTERVAL '1 week');