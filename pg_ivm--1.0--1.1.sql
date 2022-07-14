-- catalog

ALTER TABLE pg_catalog.pg_ivm_immv ADD COLUMN ispopulated bool NOT NULL;

-- functions

CREATE FUNCTION refresh_immv(text, bool)
RETURNS bigint
STRICT
AS 'MODULE_PATHNAME', 'refresh_immv'
LANGUAGE C;
