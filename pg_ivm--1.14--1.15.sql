-- pg_ivm_immv contents must not be dumped during pg_dump
ALTER EXTENSION pg_ivm DROP TABLE pgivm.pg_ivm_immv;
ALTER EXTENSION pg_ivm ADD TABLE pgivm.pg_ivm_immv;

CREATE OR REPLACE FUNCTION pgivm.create_immv(
	immv text,
	view_definition text
)
RETURNS bigint
STRICT
AS 'MODULE_PATHNAME', 'create_immv'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pgivm.restore_immv(
	immv text,
	view_definition text,
	populate boolean DEFAULT false
)
RETURNS void
STRICT
AS 'MODULE_PATHNAME', 'restore_immv'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pgivm.refresh_immv(
	immv text,
	with_data boolean DEFAULT true
)
RETURNS bigint
STRICT
AS 'MODULE_PATHNAME', 'refresh_immv'
LANGUAGE C;

CREATE FUNCTION pgivm.get_create_immv_commands()
RETURNS SETOF text
LANGUAGE sql
SET search_path = ''
AS $$
SELECT format(
    'SELECT pgivm.create_immv(' || E'\n' ||
	'    immv => %L,' || E'\n' ||
	'    view_definition => %L' || E'\n' ||
	');',
    format('%I.%I', n.nspname, c.relname),
    pgivm.get_immv_def(c.oid)
)
FROM pgivm.pg_ivm_immv i
JOIN pg_catalog.pg_class c
  ON c.oid = i.immvrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = c.relnamespace
ORDER BY n.nspname, c.relname;
$$;

CREATE FUNCTION pgivm.get_restore_immv_commands()
RETURNS SETOF text
LANGUAGE sql
SET search_path = ''
AS $$
SELECT format(
    'SELECT pgivm.restore_immv(' || E'\n' ||
	'    immv => %L,' || E'\n' ||
	'    view_definition => %L' || E'\n' ||
	');',
    format('%I.%I', n.nspname, c.relname),
    pgivm.get_immv_def(c.oid)
)
FROM pgivm.pg_ivm_immv i
JOIN pg_catalog.pg_class c
  ON c.oid = i.immvrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = c.relnamespace
ORDER BY n.nspname, c.relname;
$$;
