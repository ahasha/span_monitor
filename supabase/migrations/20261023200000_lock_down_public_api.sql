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
ALTER TABLE public.main_energy ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.branch_energy ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.branch_to_circuit ENABLE ROW LEVEL SECURITY;

-- The hourly continuous aggregates are views owned by postgres, so RLS on the
-- raw tables does not protect them. Revoking the grants is what closes them.
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

-- Clears the security_definer_view lint where TimescaleDB allows it. Continuous
-- aggregates may reject this; the REVOKE above already blocks access either way.
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
