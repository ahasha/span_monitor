-- Close the Supabase REST API to anonymous and logged-in app users.
--
-- The 2024 schema dump granted ALL on every public table and view to anon and
-- authenticated, with RLS disabled, so anyone holding the project URL and the
-- (public) anon key could read, edit, or delete all energy data.
--
-- Everything that legitimately touches this data bypasses both RLS and these
-- grants: the monitor writes with a secret (service_role) key, and the scripts
-- and notebooks connect directly as postgres.

-- RLS with no policies: nothing is visible to anon/authenticated.
-- main_energy and branch_energy are compressed hypertables, and TimescaleDB
-- rejects ENABLE ROW LEVEL SECURITY on those ("operation not supported on
-- hypertables that have compression enabled"). The REVOKE below is what closes
-- them; with no anon/authenticated grants the advisor stops flagging them too.
ALTER TABLE public.branch_to_circuit ENABLE ROW LEVEL SECURITY;

-- Without table privileges Postgres denies access before RLS is consulted, so
-- this protects the hypertables and the hourly continuous aggregates (views
-- owned by postgres, which RLS on the raw tables would not cover anyway).
REVOKE ALL ON TABLE
    public.main_energy,
    public.branch_energy,
    public.branch_to_circuit,
    public.main_energy_hourly,
    public.branch_energy_hourly
FROM anon, authenticated;

-- Stop new objects in public from being exposed by default.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON TABLES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON SEQUENCES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM anon, authenticated;

-- Attempted for the security_definer_view lint. On 2026-10-23 TimescaleDB
-- rejected it for both continuous aggregates (the NOTICE path); the lint cleared
-- anyway once the REVOKE above removed anon/authenticated access.
DO $$
DECLARE
    v text;
BEGIN
    FOREACH v IN ARRAY ARRAY['public.main_energy_hourly', 'public.branch_energy_hourly'] LOOP
        BEGIN
            EXECUTE format('ALTER VIEW %s SET (security_invoker = true)', v);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'security_invoker not set on %: %', v, SQLERRM;
        END;
    END LOOP;
END $$;
