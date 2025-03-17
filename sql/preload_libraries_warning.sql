-- SQL tests to validate that the session_preload_libraries warning is emitted
-- whenever pg_ivm is not in session_preload_libraries.

DROP EXTENSION IF EXISTS pg_ivm CASCADE;

-- Validate that pg_ivm 1.10 cannot be created if pg_ivm is not in
-- session_preload_libraries.
ALTER SYSTEM RESET session_preload_libraries;
SELECT pg_reload_conf();
\c -

CREATE EXTENSION pg_ivm VERSION '1.9';  -- expect success
ALTER EXTENSION pg_ivm UPDATE;          -- expect failure
DROP EXTENSION pg_ivm;
CREATE EXTENSION pg_ivm;                -- expect failure

-- Validate that pg_ivm 1.10 can be created if pg_ivm is in
-- session_preload_libraries.
ALTER SYSTEM SET session_preload_libraries = pg_ivm;
SELECT pg_reload_conf();
\c -

CREATE EXTENSION pg_ivm VERSION '1.9';  -- expect success
ALTER EXTENSION pg_ivm UPDATE;          -- expect success
DROP EXTENSION pg_ivm;
CREATE EXTENSION pg_ivm;                -- expect success

DROP EXTENSION pg_ivm CASCADE;
