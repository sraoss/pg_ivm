CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 5);

SELECT create_immv('mv', 'SELECT * FROM t');
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;

-- Refresh IMMV with data
SELECT refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;

INSERT INTO t VALUES(6);
SELECT i FROM mv ORDER BY 1;

-- Make IMMV unpopulated
SELECT refresh_immv('mv', false);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- Immediate maintenance is disabled. IMMV can be scannable and is empty.
INSERT INTO t VALUES(7);
SELECT i FROM mv ORDER BY 1;

-- Refresh the IMMV and make it populated.
SELECT refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- Immediate maintenance is enabled.
INSERT INTO t VALUES(8);
SELECT i FROM mv ORDER BY 1;

-- Use qualified name
SELECT refresh_immv('public.mv', true);

-- Use not existing IMMV
SELECT refresh_immv('mv_not_existing', true);

-- Try to refresh a normal table -- error
SELECT refresh_immv('t', true);
