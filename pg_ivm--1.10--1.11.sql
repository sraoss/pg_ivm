ALTER TABLE pgivm.pg_ivm_immv ADD COLUMN querystring text;
ALTER TABLE pgivm.pg_ivm_immv ADD COLUMN immvuuid uuid;

UPDATE pgivm.pg_ivm_immv SET querystring = pgivm.get_immv_def(immvrelid);
UPDATE pgivm.pg_ivm_immv SET immvuuid = gen_random_uuid();

ALTER TABLE pgivm.pg_ivm_immv ADD CONSTRAINT pg_ivm_immv_uuid UNIQUE (immvuuid);
ALTER TABLE pgivm.pg_ivm_immv ALTER COLUMN querystring SET NOT NULL;
ALTER TABLE pgivm.pg_ivm_immv ALTER COLUMN immvuuid SET NOT NULL;
ALTER TABLE pgivm.pg_ivm_immv DROP COLUMN viewdef;

CREATE FUNCTION pgivm.recreate_all_immvs() RETURNS VOID LANGUAGE PLPGSQL AS
$$
BEGIN
	PERFORM pgivm.refresh_immv(n.nspname || '.' || c.relname, false)
		FROM pgivm.pg_ivm_immv as ivm
		JOIN pg_catalog.pg_class as c
		ON c.oid = ivm.immvrelid
		JOIN pg_catalog.pg_namespace as n
		ON c.relnamespace = n.oid;

	PERFORM pgivm.refresh_immv(n.nspname || '.' || c.relname, true)
		FROM pgivm.pg_ivm_immv as ivm
		JOIN pg_catalog.pg_class as c
		ON c.oid = ivm.immvrelid
		JOIN pg_catalog.pg_namespace as n
		ON c.relnamespace = n.oid;
END
$$;

CREATE FUNCTION pgivm.save_query_strings() RETURNS event_trigger
AS 'MODULE_PATHNAME', 'save_query_strings' LANGUAGE C;

CREATE FUNCTION pgivm.restore_query_strings() RETURNS event_trigger
AS 'MODULE_PATHNAME', 'restore_query_strings' LANGUAGE C;

CREATE EVENT TRIGGER save_query_strings
ON ddl_command_start
EXECUTE FUNCTION pgivm.save_query_strings();

CREATE EVENT TRIGGER restore_query_strings
ON ddl_command_end
EXECUTE FUNCTION pgivm.restore_query_strings();
