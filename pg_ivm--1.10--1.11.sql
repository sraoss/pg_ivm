ALTER TABLE pgivm.pg_ivm_immv ADD COLUMN querystring text NOT NULL;

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
	EXECUTE format('SET search_path = %s', old_search_path);
END
$$;

CREATE EVENT TRIGGER refresh_query_strings
ON ddl_command_end
EXECUTE FUNCTION pgivm.refresh_query_strings();
