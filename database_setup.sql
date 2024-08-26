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