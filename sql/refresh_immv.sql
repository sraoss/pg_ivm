CREATE TABLE t (i int PRIMARY KEY);
INSERT INTO t SELECT generate_series(1, 5);

SELECT create_immv('mv', 'SELECT * FROM t');
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;

-- refresh immv without changing the ispopulated flag
SELECT refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;

INSERT INTO t VALUES(6);
SELECT i FROM mv ORDER BY 1;

-- change ispopulated to False
SELECT refresh_immv('mv', false);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- immv remains empty
INSERT INTO t VALUES(7);
SELECT i FROM mv ORDER BY 1;

-- chaneg ispopulated to True, immv is updated
SELECT refresh_immv('mv', true);
SELECT immvrelid, ispopulated FROM pg_ivm_immv ORDER BY 1;
SELECT i FROM mv ORDER BY 1;

-- immediate maintenance
INSERT INTO t VALUES(8);
SELECT i FROM mv ORDER BY 1;

