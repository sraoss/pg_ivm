CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 5);

SELECT pgivm.create_immv('mv', 'SELECT * FROM t');
SELECT immvrelid, ispopulated FROM pgivm.pg_ivm_immv ORDER BY 1;

-- Refresh IMMV with data
SELECT pgivm.refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pgivm.pg_ivm_immv ORDER BY 1;

INSERT INTO t VALUES(6);
SELECT i FROM mv ORDER BY 1;

-- Make IMMV unpopulated
SELECT pgivm.refresh_immv('mv', false);
SELECT immvrelid, ispopulated FROM pgivm.pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- Immediate maintenance is disabled. IMMV can be scannable and is empty.
INSERT INTO t VALUES(7);
SELECT i FROM mv ORDER BY 1;

-- Refresh the IMMV and make it populated.
SELECT pgivm.refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pgivm.pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- Immediate maintenance is enabled.
INSERT INTO t VALUES(8);
SELECT i FROM mv ORDER BY 1;

-- Use qualified name
SELECT pgivm.refresh_immv('public.mv', true);

-- Use not existing IMMV
SELECT pgivm.refresh_immv('mv_not_existing', true);

-- Try to refresh a normal table -- error
SELECT pgivm.refresh_immv('t', true);
