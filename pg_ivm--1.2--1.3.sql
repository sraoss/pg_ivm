-- functions

CREATE FUNCTION ivm_visible_in_prestate(oid, tid, oid)
RETURNS bool
STABLE
AS 'MODULE_PATHNAME', 'ivm_visible_in_prestate'
LANGUAGE C;

CREATE FUNCTION get_immv_def(IN immvrelid regclass)
RETURNS text
STRICT
AS 'MODULE_PATHNAME', 'get_immv_def'
LANGUAGE C;

-- event trigger

DROP EVENT TRIGGER pg_ivm_sql_drop_trigger;
DROP FUNCTION pg_ivm_sql_drop_trigger_func;
