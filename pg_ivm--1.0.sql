-- catalog

CREATE SCHEMA __pg_ivm__;

CREATE TABLE __pg_ivm__.pg_ivm_immv(
  immvrelid regclass NOT NULL,
  viewdef text NOT NULL,

  CONSTRAINT pg_ivm_immv_pkey PRIMARY KEY (immvrelid)
);

ALTER TABLE __pg_ivm__.pg_ivm_immv SET SCHEMA pg_catalog;

--CREATE UNIQUE INDEX ON pg_catalog.pg_ivm_immv USING btree (immvrelid);

SELECT pg_catalog.pg_extension_config_dump('pg_catalog.pg_ivm_immv', '');

-- functions

CREATE FUNCTION create_immv(text, text)
RETURNS bigint 
AS 'MODULE_PATHNAME', 'create_immv'
LANGUAGE C;

-- trigger functions

CREATE FUNCTION "IVM_immediate_before"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_immediate_before'
LANGUAGE C;

CREATE FUNCTION "IVM_immediate_maintenance"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_immediate_maintenance'
LANGUAGE C;

CREATE FUNCTION "IVM_prevent_immv_change"()
RETURNS trigger 
AS 'MODULE_PATHNAME', 'IVM_prevent_immv_change'
LANGUAGE C;
