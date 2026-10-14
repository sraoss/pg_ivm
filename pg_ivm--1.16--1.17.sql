DROP FUNCTION pgivm.restore_immv(text, text, boolean);
CREATE FUNCTION pgivm.restore_immv(
	immv text,
	view_definition text,
	populate boolean DEFAULT false,
	replace boolean DEFAULT false
)
RETURNS void
STRICT
AS 'MODULE_PATHNAME', 'restore_immv'
LANGUAGE C;

DROP FUNCTION pgivm.get_restore_immv_commands();
CREATE FUNCTION pgivm.get_restore_immv_commands(
	replace boolean DEFAULT false
)
RETURNS SETOF text
LANGUAGE sql
SET search_path = ''
AS $$
SELECT format(
    'SELECT pgivm.restore_immv(' || E'\n' ||
	'    immv => %L,' || E'\n' ||
	'    view_definition => %L,' || E'\n' ||
	'    replace => %s' || E'\n' ||
	');',
    format('%I.%I', n.nspname, c.relname),
    pgivm.get_immv_def(c.oid),
	CASE WHEN replace THEN 'true' ELSE 'false' END
)
FROM pgivm.pg_ivm_immv i
JOIN pg_catalog.pg_class c
  ON c.oid = i.immvrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = c.relnamespace
ORDER BY n.nspname, c.relname;
$$;
