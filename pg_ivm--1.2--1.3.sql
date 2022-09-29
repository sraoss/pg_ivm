-- functions

CREATE FUNCTION ivm_visible_in_prestate(oid, tid, oid)
RETURNS bool
STABLE
AS 'MODULE_PATHNAME', 'ivm_visible_in_prestate'
LANGUAGE C;
