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

-- drop a garbage
DROP SCHEMA __pg_ivm__;
