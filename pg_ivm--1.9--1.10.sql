DO $$
    DECLARE
        shared_pl text;
        session_pl text;
    BEGIN
        SELECT setting INTO shared_pl FROM pg_catalog.pg_settings
        WHERE name = 'shared_preload_libraries';

        SELECT setting INTO session_pl FROM pg_catalog.pg_settings
        WHERE name = 'session_preload_libraries';

        IF shared_pl !~ '\mpg_ivm\M' AND session_pl !~ '\mpg_ivm\M' THEN
            RAISE EXCEPTION 'pg_ivm is not loaded in shared_preload_libraries '
                'or session_preload_libraries'
            USING HINT = 'Add pg_ivm to session_preload_libraries and '
                'restart the session. Or, add pg_ivm to '
                'shared_preload_libraries and restart Postgres.';
        END IF;
    END
$$;

-- create a new schema pgivm and change the objects' schema to it
CREATE SCHEMA pgivm;

ALTER TABLE pg_ivm_immv SET SCHEMA pgivm;
ALTER FUNCTION create_immv(text, text) SET SCHEMA pgivm;
ALTER FUNCTION refresh_immv(text, bool) SET SCHEMA pgivm;
ALTER FUNCTION get_immv_def(regclass) SET SCHEMA pgivm;
ALTER FUNCTION ivm_visible_in_prestate(oid, tid, oid) SET SCHEMA pgivm;
ALTER FUNCTION "IVM_immediate_before"() SET SCHEMA pgivm;
ALTER FUNCTION "IVM_immediate_maintenance"() SET SCHEMA pgivm;
ALTER FUNCTION "IVM_prevent_immv_change"() SET SCHEMA pgivm;

GRANT USAGE ON SCHEMA pgivm TO PUBLIC;

ALTER TABLE pgivm.pg_ivm_immv ADD COLUMN lastivmupdate xid8;

-- drop a garbage
DROP SCHEMA __pg_ivm__;
