

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE EXTENSION IF NOT EXISTS "timescaledb" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgsodium" WITH SCHEMA "pgsodium";






COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE EXTENSION IF NOT EXISTS "pg_graphql" WITH SCHEMA "graphql";






CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgjwt" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE TYPE "public"."relay_state_type" AS ENUM (
    'CLOSED',
    'OPEN'
);


ALTER TYPE "public"."relay_state_type" OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."branch_energy" (
    "time" timestamp with time zone NOT NULL,
    "branch_id" integer NOT NULL,
    "relay_state" "public"."relay_state_type",
    "instant_power_w" double precision,
    "imported_active_energy_wh" double precision,
    "exported_active_energy_wh" double precision,
    "measure_start_ts_ms" bigint,
    "measure_duration_ms" bigint,
    "is_measure_valid" boolean
);


ALTER TABLE "public"."branch_energy" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."main_energy" (
    "time" timestamp with time zone NOT NULL,
    "relay_state" "public"."relay_state_type",
    "main_meter_produced_energy_wh" double precision,
    "main_meter_consumed_energy_wh" double precision,
    "instant_grid_power_w" double precision,
    "feed_through_power_w" double precision,
    "feed_through_produced_energy_wh" double precision,
    "feed_through_consumed_energy_wh" double precision,
    "grid_sample_start_ms" bigint,
    "grid_sample_end_ms" bigint,
    "dsm_grid_state" "text",
    "dsm_state" "text",
    "current_run_config" "text"
);


ALTER TABLE "public"."main_energy" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."branch_energy_hourly" AS
 SELECT "b"."time",
    "b"."branch_id",
    "b"."num_measurements",
    "b"."avg_instant_power_w",
    "b"."imported_active_energy_wh",
    "b"."exported_active_energy_wh",
    "b"."measure_start_ts_ms",
    "b"."measure_duration_ms"
   FROM "_timescaledb_internal"."_materialized_hypertable_5" "b"
  WHERE ("b"."time" < COALESCE("_timescaledb_internal"."to_timestamp"("_timescaledb_internal"."cagg_watermark"(5)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT "extensions"."time_bucket"('01:00:00'::interval, "b"."time") AS "time",
    "b"."branch_id",
    "count"(*) AS "num_measurements",
    "avg"("b"."instant_power_w") AS "avg_instant_power_w",
    "max"("b"."imported_active_energy_wh") AS "imported_active_energy_wh",
    "max"("b"."exported_active_energy_wh") AS "exported_active_energy_wh",
    "min"("b"."measure_start_ts_ms") AS "measure_start_ts_ms",
    "sum"("b"."measure_duration_ms") AS "measure_duration_ms"
   FROM "public"."branch_energy" "b"
  WHERE ("b"."time" >= COALESCE("_timescaledb_internal"."to_timestamp"("_timescaledb_internal"."cagg_watermark"(5)), '-infinity'::timestamp with time zone))
  GROUP BY ("extensions"."time_bucket"('01:00:00'::interval, "b"."time")), "b"."branch_id";


ALTER TABLE "public"."branch_energy_hourly" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."branch_to_circuit" (
    "branch_id" integer NOT NULL,
    "circuit_id" "text",
    "name" "text"
);


ALTER TABLE "public"."branch_to_circuit" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."main_energy_hourly" AS
 SELECT "m"."time",
    "m"."num_measurements",
    "m"."main_meter_produced_energy_wh",
    "m"."main_meter_consumed_energy_wh",
    "m"."avg_instant_grid_power_w",
    "m"."avg_feed_through_power_w",
    "m"."feed_through_produced_energy_wh",
    "m"."feed_through_consumed_energy_wh",
    "m"."min_grid_sample_start_ms",
    "m"."max_grid_sample_end_ms"
   FROM "_timescaledb_internal"."_materialized_hypertable_6" "m"
  WHERE ("m"."time" < COALESCE("_timescaledb_internal"."to_timestamp"("_timescaledb_internal"."cagg_watermark"(6)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT "extensions"."time_bucket"('01:00:00'::interval, "m"."time") AS "time",
    "count"(*) AS "num_measurements",
    "max"("m"."main_meter_produced_energy_wh") AS "main_meter_produced_energy_wh",
    "max"("m"."main_meter_consumed_energy_wh") AS "main_meter_consumed_energy_wh",
    "avg"("m"."instant_grid_power_w") AS "avg_instant_grid_power_w",
    "avg"("m"."feed_through_power_w") AS "avg_feed_through_power_w",
    "max"("m"."feed_through_produced_energy_wh") AS "feed_through_produced_energy_wh",
    "max"("m"."feed_through_consumed_energy_wh") AS "feed_through_consumed_energy_wh",
    "min"("m"."grid_sample_start_ms") AS "min_grid_sample_start_ms",
    "max"("m"."grid_sample_end_ms") AS "max_grid_sample_end_ms"
   FROM "public"."main_energy" "m"
  WHERE ("m"."time" >= COALESCE("_timescaledb_internal"."to_timestamp"("_timescaledb_internal"."cagg_watermark"(6)), '-infinity'::timestamp with time zone))
  GROUP BY ("extensions"."time_bucket"('01:00:00'::interval, "m"."time"));


ALTER TABLE "public"."main_energy_hourly" OWNER TO "postgres";


ALTER TABLE ONLY "public"."branch_to_circuit"
    ADD CONSTRAINT "branch_to_circuit_pkey" PRIMARY KEY ("branch_id");



CREATE INDEX "branch_energy_time_idx" ON "public"."branch_energy" USING "btree" ("time" DESC);



CREATE INDEX "main_energy_time_idx" ON "public"."main_energy" USING "btree" ("time" DESC);



CREATE OR REPLACE TRIGGER "ts_cagg_invalidation_trigger" AFTER INSERT OR DELETE OR UPDATE ON "public"."branch_energy" FOR EACH ROW EXECUTE FUNCTION "_timescaledb_internal"."continuous_agg_invalidation_trigger"('2');



CREATE OR REPLACE TRIGGER "ts_cagg_invalidation_trigger" AFTER INSERT OR DELETE OR UPDATE ON "public"."main_energy" FOR EACH ROW EXECUTE FUNCTION "_timescaledb_internal"."continuous_agg_invalidation_trigger"('1');



CREATE OR REPLACE TRIGGER "ts_insert_blocker" BEFORE INSERT ON "public"."branch_energy" FOR EACH ROW EXECUTE FUNCTION "_timescaledb_internal"."insert_blocker"();



CREATE OR REPLACE TRIGGER "ts_insert_blocker" BEFORE INSERT ON "public"."main_energy" FOR EACH ROW EXECUTE FUNCTION "_timescaledb_internal"."insert_blocker"();





ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";


GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";




























































































































































































































































































































































































































































GRANT ALL ON TABLE "public"."branch_energy" TO "anon";
GRANT ALL ON TABLE "public"."branch_energy" TO "authenticated";
GRANT ALL ON TABLE "public"."branch_energy" TO "service_role";



GRANT ALL ON TABLE "public"."main_energy" TO "anon";
GRANT ALL ON TABLE "public"."main_energy" TO "authenticated";
GRANT ALL ON TABLE "public"."main_energy" TO "service_role";





















GRANT ALL ON TABLE "public"."branch_energy_hourly" TO "anon";
GRANT ALL ON TABLE "public"."branch_energy_hourly" TO "authenticated";
GRANT ALL ON TABLE "public"."branch_energy_hourly" TO "service_role";



GRANT ALL ON TABLE "public"."branch_to_circuit" TO "anon";
GRANT ALL ON TABLE "public"."branch_to_circuit" TO "authenticated";
GRANT ALL ON TABLE "public"."branch_to_circuit" TO "service_role";



GRANT ALL ON TABLE "public"."main_energy_hourly" TO "anon";
GRANT ALL ON TABLE "public"."main_energy_hourly" TO "authenticated";
GRANT ALL ON TABLE "public"."main_energy_hourly" TO "service_role";



ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "service_role";






























RESET ALL;
