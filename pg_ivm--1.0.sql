-- catalog

CREATE SCHEMA __pg_ivm__;

CREATE TABLE __pg_ivm__.pg_ivm_immv(
  immvrelid regclass NOT NULL,
  viewdef text NOT NULL,

  CONSTRAINT pg_ivm_immv_pkey PRIMARY KEY (immvrelid)
);

ALTER TABLE __pg_ivm__.pg_ivm_immv SET SCHEMA pg_catalog;

SELECT pg_catalog.pg_extension_config_dump('pg_catalog.pg_ivm_immv', '');

-- functions

CREATE FUNCTION create_immv(text, text)
RETURNS bigint 
STRICT
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


/*
 * DDL trigger that removes entry from pg_ivm_immv
 */
CREATE FUNCTION pg_catalog.pg_ivm_sql_drop_trigger_func()
RETURNS event_trigger AS $$
DECLARE
	pg_class_oid	OID;
	relids			REGCLASS[];

BEGIN
	pg_class_oid = 'pg_catalog.pg_class'::regclass;

	/* Find relids to remove */
	DELETE FROM pg_catalog.pg_ivm_immv
	USING pg_catalog.pg_event_trigger_dropped_objects() AS events
	WHERE immvrelid = events.objid AND
	      events.classid = pg_class_oid AND events.objsubid = 0;
END
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER pg_ivm_sql_drop_trigger
ON sql_drop
EXECUTE PROCEDURE pg_catalog.pg_ivm_sql_drop_trigger_func();
