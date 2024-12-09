CREATE SCHEMA pgivm;

-- catalog

CREATE TABLE pgivm.pg_ivm_immv(
  immvrelid regclass NOT NULL,
  viewdef text NOT NULL,
  ispopulated bool NOT NULL,

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
