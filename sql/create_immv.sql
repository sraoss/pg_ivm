CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 100);

SELECT pgivm.create_immv('mv', 'SELECT * FROM t');
SELECT pgivm.create_immv(' mv2 ( x  ) ', 'SELECT * FROM t WHERE i%2 = 0');

SELECT pgivm.create_immv('mv3', 'WITH d AS (DELETE FROM t RETURNING NULL) SELECT * FROM t');

SELECT immvrelid, pgivm.get_immv_def(immvrelid) FROM pgivm.pg_ivm_immv ORDER BY 1;

-- contain immv
SELECT pgivm.create_immv('mv_in_immv01', 'SELECT i FROM mv');
SELECT pgivm.create_immv('mv_in_immv02', 'SELECT t.i FROM t INNER JOIN mv2 ON t.i = mv2.x');

-- SQL other than SELECT
SELECT pgivm.create_immv('mv_in_create', 'CREATE TABLE in_create(i int)');
SELECT pgivm.create_immv('mv_in_insert', 'INSERT INTO t VALUES(10)');
SELECT pgivm.create_immv('mv_in_update', 'UPDATE t SET i = 10');
SELECT pgivm.create_immv('mv_in_delete', 'DELETE FROM t');
SELECT pgivm.create_immv('mv_in_drop', 'DROP TABLE t');

DROP TABLE t;

DROP TABLE mv;
SELECT immvrelid, pgivm.get_immv_def(immvrelid) FROM pgivm.pg_ivm_immv ORDER BY 1;

DROP TABLE mv2;
SELECT immvrelid, pgivm.get_immv_def(immvrelid) FROM pgivm.pg_ivm_immv ORDER BY 1;

DROP TABLE t;
