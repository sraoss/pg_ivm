CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 100);

SELECT create_immv('mv', 'SELECT * FROM t');
SELECT create_immv(' mv2 ( x  ) ', 'SELECT * FROM t WHERE i%2 = 0');

SELECT create_immv('mv3', 'WITH d AS (DELETE FROM t RETURNING NULL) SELECT * FROM t');

SELECT immvrelid FROM pg_ivm_immv ORDER BY 1;

DROP TABLE t;

DROP TABLE mv;
SELECT immvrelid FROM pg_ivm_immv ORDER BY 1;

DROP TABLE mv2;
SELECT immvrelid FROM pg_ivm_immv ORDER BY 1;
