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

-- Verify that the warning is emitted when each SQL function is called.
ALTER SYSTEM RESET session_preload_libraries;
SELECT pg_reload_conf();
\c -

CREATE TABLE mytab (i int primary key, a text);
SELECT pgivm.create_immv('myview', 'SELECT i, reverse(a) FROM mytab');
SELECT pgivm.refresh_immv('myview', true);
SELECT pgivm.get_immv_def('myview');

-- Verify that the warning is emitted by the maintenance triggers.
INSERT INTO mytab VALUES (1, 'asdf');

-- Verify that the warning is not emitted after pg_ivm is added to
-- session_preload_libraries.
ALTER SYSTEM SET session_preload_libraries = pg_ivm;
SELECT pg_reload_conf();
\c -

SELECT pgivm.create_immv('myview2', 'SELECT i, reverse(a) FROM mytab');
SELECT pgivm.refresh_immv('myview2', true);
SELECT pgivm.get_immv_def('myview2');
INSERT INTO mytab VALUES (2, 'qwer');

