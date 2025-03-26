ALTER TABLE pgivm.pg_ivm_immv ADD COLUMN querystring text NOT NULL;

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

CREATE FUNCTION pgivm.refresh_query_strings()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS
$$
DECLARE
	old_search_path text;
BEGIN
	-- Empty search path so that get_immv_def returns a fully-qualified query.
	SELECT setting INTO old_search_path FROM pg_catalog.pg_settings
		WHERE name = 'search_path';
	SET search_path = '';

	UPDATE pgivm.pg_ivm_immv SET querystring = pgivm.get_immv_def(immvrelid);

	-- Reset search path to the original value.
	IF old_search_path != '' AND old_search_path != '""' THEN
		EXECUTE format('SET search_path = %s', old_search_path);
	END IF;
END
$$;

CREATE EVENT TRIGGER refresh_query_strings
ON ddl_command_end
EXECUTE FUNCTION pgivm.refresh_query_strings();
