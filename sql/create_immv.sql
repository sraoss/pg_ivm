CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 100);

SELECT create_immv('mv', 'SELECT * FROM t');
SELECT create_immv(' mv2 ( x  ) ', 'SELECT * FROM t WHERE i%2 = 0');

SELECT create_immv('mv3', 'WITH d AS (DELETE FROM t RETURNING NULL) SELECT * FROM t');

SELECT immvrelid, get_immv_def(immvrelid) FROM pg_ivm_immv ORDER BY 1;

-- contain immv
SELECT create_immv('mv_in_immv01', 'SELECT i FROM mv');
SELECT create_immv('mv_in_immv02', 'SELECT t.i FROM t INNER JOIN mv2 ON t.i = mv2.x');

DROP TABLE t;

DROP TABLE mv;
SELECT immvrelid, get_immv_def(immvrelid) FROM pg_ivm_immv ORDER BY 1;

DROP TABLE mv2;
SELECT immvrelid, get_immv_def(immvrelid) FROM pg_ivm_immv ORDER BY 1;

DROP TABLE t;
