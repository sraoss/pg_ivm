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

CREATE SCHEMA pgivm;

-- catalog

CREATE TABLE pgivm.pg_ivm_immv(
  immvrelid regclass NOT NULL,
  viewdef text NOT NULL,
  ispopulated bool NOT NULL,
  lastivmupdate xid8,

  CONSTRAINT pg_ivm_immv_pkey PRIMARY KEY (immvrelid)
);

SELECT pg_catalog.pg_extension_config_dump('pgivm.pg_ivm_immv', '');

-- functions

CREATE FUNCTION pgivm.create_immv(text, text)
RETURNS bigint 
STRICT
AS 'MODULE_PATHNAME', 'create_immv'
LANGUAGE C;

CREATE FUNCTION pgivm.refresh_immv(text, bool)
RETURNS bigint
STRICT
AS 'MODULE_PATHNAME', 'refresh_immv'
LANGUAGE C;

CREATE FUNCTION pgivm.get_immv_def(IN immvrelid regclass)
RETURNS text
STRICT
AS 'MODULE_PATHNAME', 'get_immv_def'
LANGUAGE C;

CREATE FUNCTION pgivm.ivm_visible_in_prestate(oid, tid, oid)
RETURNS bool
STABLE
AS 'MODULE_PATHNAME', 'ivm_visible_in_prestate'
LANGUAGE C;

-- trigger functions

CREATE FUNCTION pgivm."IVM_immediate_before"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_immediate_before'
LANGUAGE C;

CREATE FUNCTION pgivm."IVM_immediate_maintenance"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_immediate_maintenance'
LANGUAGE C;

CREATE FUNCTION pgivm."IVM_prevent_immv_change"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_prevent_immv_change'
LANGUAGE C;

GRANT SELECT ON TABLE pgivm.pg_ivm_immv TO PUBLIC;
GRANT USAGE ON SCHEMA pgivm TO PUBLIC;
