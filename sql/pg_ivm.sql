CREATE EXTENSION pg_ivm;
GRANT ALL ON SCHEMA public TO public;

-- create a table to use as a basis for views and materialized views in various combinations
CREATE TABLE mv_base_a (i int, j int);
INSERT INTO mv_base_a VALUES
  (1,10),
  (2,20),
  (3,30),
  (4,40),
  (5,50);
CREATE TABLE mv_base_b (i int, k int);
INSERT INTO mv_base_b VALUES
  (1,101),
  (2,102),
  (3,103),
  (4,104);

SELECT create_immv('mv_ivm_1', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i)');
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;

-- immediate maintenance
BEGIN;
INSERT INTO mv_base_b VALUES(5,105);
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
UPDATE mv_base_a SET j = 0 WHERE i = 1;
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
DELETE FROM mv_base_b WHERE (i,k) = (5,105);
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;
ROLLBACK;
SELECT * FROM mv_ivm_1 ORDER BY 1,2,3;

-- TRUNCATE a base table in join views
BEGIN;
TRUNCATE mv_base_a;
SELECT * FROM mv_ivm_1;
ROLLBACK;

BEGIN;
TRUNCATE mv_base_b;
SELECT * FROM mv_ivm_1;
ROLLBACK;

-- some query syntax
BEGIN;
CREATE FUNCTION ivm_func() RETURNS int LANGUAGE 'sql'
       AS 'SELECT 1' IMMUTABLE;
SELECT create_immv('mv_ivm_func', 'SELECT * FROM ivm_func()');
SELECT create_immv('mv_ivm_no_tbl', 'SELECT 1');
ROLLBACK;

-- result of materialized view have DISTINCT clause or the duplicate result.
BEGIN;
SELECT create_immv('mv_ivm_duplicate', 'SELECT j FROM mv_base_a');
SELECT create_immv('mv_ivm_distinct', 'SELECT DISTINCT j FROM mv_base_a');
INSERT INTO mv_base_a VALUES(6,20);
SELECT * FROM mv_ivm_duplicate ORDER BY 1;
SELECT * FROM mv_ivm_distinct ORDER BY 1;
DELETE FROM mv_base_a WHERE (i,j) = (2,20);
SELECT * FROM mv_ivm_duplicate ORDER BY 1;
SELECT * FROM mv_ivm_distinct ORDER BY 1;
ROLLBACK;

-- support SUM(), COUNT() and AVG() aggregate functions
BEGIN;
SELECT create_immv('mv_ivm_agg', 'SELECT i, SUM(j), COUNT(i), AVG(j) FROM mv_base_a GROUP BY i');
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
UPDATE mv_base_a SET j = 200 WHERE (i,j) = (2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
DELETE FROM mv_base_a WHERE (i,j) = (2,200);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3,4;
ROLLBACK;

-- support COUNT(*) aggregate function
BEGIN;
SELECT create_immv('mv_ivm_agg', 'SELECT i, SUM(j), COUNT(*) FROM mv_base_a GROUP BY i');
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES(2,100);
SELECT * FROM mv_ivm_agg ORDER BY 1,2,3;
ROLLBACK;

-- TRUNCATE a base table in aggregate views
BEGIN;
SELECT create_immv('mv_ivm_agg', 'SELECT i, SUM(j), COUNT(*) FROM mv_base_a GROUP BY i');
TRUNCATE mv_base_a;
SELECT sum, count FROM mv_ivm_agg;
SELECT i, SUM(j), COUNT(*) FROM mv_base_a GROUP BY i;
ROLLBACK;

-- support aggregate functions without GROUP clause
BEGIN;
SELECT create_immv('mv_ivm_group',  'SELECT SUM(j), COUNT(j), AVG(j) FROM mv_base_a');
SELECT * FROM mv_ivm_group ORDER BY 1;
INSERT INTO mv_base_a VALUES(6,60);
SELECT * FROM mv_ivm_group ORDER BY 1;
DELETE FROM mv_base_a;
SELECT * FROM mv_ivm_group ORDER BY 1;
ROLLBACK;

-- TRUNCATE a base table in aggregate views without GROUP clause
BEGIN;
SELECT create_immv('mv_ivm_group', 'SELECT SUM(j), COUNT(j), AVG(j) FROM mv_base_a');
TRUNCATE mv_base_a;
SELECT sum, count, avg FROM mv_ivm_group;
SELECT SUM(j), COUNT(j), AVG(j) FROM mv_base_a;
ROLLBACK;

-- resolved issue: When use AVG() function and values is indivisible, result of AVG() is incorrect.
BEGIN;
SELECT create_immv('mv_ivm_avg_bug', 'SELECT i, SUM(j), COUNT(j), AVG(j) FROM mv_base_A GROUP BY i');
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES
  (1,0),
  (1,0),
  (2,30),
  (2,30);
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
DELETE FROM mv_base_a WHERE (i,j) = (1,0);
DELETE FROM mv_base_a WHERE (i,j) = (2,30);
SELECT * FROM mv_ivm_avg_bug ORDER BY 1,2,3;
ROLLBACK;

-- support MIN(), MAX() aggregate functions
BEGIN;
SELECT create_immv('mv_ivm_min_max(i, min_j, max_j)', 'SELECT i, MIN(j), MAX(j)  FROM mv_base_a GROUP BY i');
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
INSERT INTO mv_base_a VALUES
  (1,11), (1,12),
  (2,21), (2,22),
  (3,31), (3,32),
  (4,41), (4,42),
  (5,51), (5,52);
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
DELETE FROM mv_base_a WHERE (i,j) IN ((1,10), (2,21), (3,32));
SELECT * FROM mv_ivm_min_max ORDER BY 1,2,3;
ROLLBACK;

-- support MIN(), MAX() aggregate functions without GROUP clause
BEGIN;
SELECT create_immv('mv_ivm_min_max(min_j, max_j)', 'SELECT MIN(j), MAX(j)  FROM mv_base_a');
SELECT * FROM mv_ivm_min_max;
INSERT INTO mv_base_a VALUES
  (0,0), (6,60), (7,70);
SELECT * FROM mv_ivm_min_max;
DELETE FROM mv_base_a WHERE (i,j) IN ((0,0), (7,70));
SELECT * FROM mv_ivm_min_max;
DELETE FROM mv_base_a;
SELECT * FROM mv_ivm_min_max;
ROLLBACK;

-- Test MIN/MAX after search_path change
BEGIN;
SELECT create_immv('mv_ivm_min', 'SELECT MIN(j) FROM mv_base_a');
SELECT * FROM mv_ivm_min ORDER BY 1,2,3;

CREATE SCHEMA myschema;
GRANT ALL ON SCHEMA myschema TO public;
CREATE TABLE myschema.mv_base_a (j int);
INSERT INTO myschema.mv_base_a VALUES (1);

DELETE FROM mv_base_a WHERE (i,j) = (1,10);
SELECT * FROM mv_ivm_min ORDER BY 1,2,3;

SET search_path TO myschema,public,pg_catalog;
DELETE FROM public.mv_base_a WHERE (i,j) = (2,20);
SELECT * FROM mv_ivm_min ORDER BY 1,2,3;
ROLLBACK;

-- aggregate views with column names specified
BEGIN;
SELECT create_immv('mv_ivm_agg(a)', 'SELECT i, SUM(j) FROM mv_base_a GROUP BY i');
INSERT INTO mv_base_a VALUES (1,100), (2,200), (3,300);
UPDATE mv_base_a SET j = 2000 WHERE (i,j) = (2,20);
DELETE FROM mv_base_a WHERE (i,j) = (3,30);
SELECT * FROM mv_ivm_agg ORDER BY 1,2;
ROLLBACK;
BEGIN;
SELECT create_immv('mv_ivm_agg(a,b)', 'SELECT i, SUM(j) FROM mv_base_a GROUP BY i');
INSERT INTO mv_base_a VALUES (1,100), (2,200), (3,300);
UPDATE mv_base_a SET j = 2000 WHERE (i,j) = (2,20);
DELETE FROM mv_base_a WHERE (i,j) = (3,30);
SELECT * FROM mv_ivm_agg ORDER BY 1,2;
ROLLBACK;
BEGIN;
SELECT create_immv('mv_ivm_agg(a,b,c)', 'SELECT i, SUM(j) FROM mv_base_a GROUP BY i');
ROLLBACK;

-- support self join view and multiple change on the same table
BEGIN;
CREATE TABLE base_t (i int, v int);
INSERT INTO base_t VALUES (1, 10), (2, 20), (3, 30);
SELECT create_immv('mv_self(v1, v2)',
 'SELECT t1.v, t2.v FROM base_t AS t1 JOIN base_t AS t2 ON t1.i = t2.i');
SELECT * FROM mv_self ORDER BY v1;
INSERT INTO base_t VALUES (4,40);
DELETE FROM base_t WHERE i = 1;
UPDATE base_t SET v = v*10 WHERE i=2;
SELECT * FROM mv_self ORDER BY v1;
WITH
 ins_t1 AS (INSERT INTO base_t VALUES (5,50) RETURNING 1),
 ins_t2 AS (INSERT INTO base_t VALUES (6,60) RETURNING 1),
 upd_t AS (UPDATE base_t SET v = v + 100  RETURNING 1),
 dlt_t AS (DELETE FROM base_t WHERE i IN (4,5)  RETURNING 1)
SELECT NULL;
SELECT * FROM mv_self ORDER BY v1;

--- with sub-transactions
SAVEPOINT p1;
INSERT INTO base_t VALUES (7,70);
RELEASE SAVEPOINT p1;
INSERT INTO base_t VALUES (7,77);
SELECT * FROM mv_self ORDER BY v1, v2;

ROLLBACK;

-- support simultaneous table changes
BEGIN;
CREATE TABLE base_r (i int, v int);
CREATE TABLE base_s (i int, v int);
INSERT INTO base_r VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO base_s VALUES (1, 100), (2, 200), (3, 300);
SELECT create_immv('mv(v1, v2)', 'SELECT r.v, s.v FROM base_r AS r JOIN base_s AS s USING(i)');;
SELECT * FROM mv ORDER BY v1;
WITH
 ins_r AS (INSERT INTO base_r VALUES (1,11) RETURNING 1),
 ins_r2 AS (INSERT INTO base_r VALUES (3,33) RETURNING 1),
 ins_s AS (INSERT INTO base_s VALUES (2,222) RETURNING 1),
 upd_r AS (UPDATE base_r SET v = v + 1000 WHERE i = 2 RETURNING 1),
 dlt_s AS (DELETE FROM base_s WHERE i = 3 RETURNING 1)
SELECT NULL;
SELECT * FROM mv ORDER BY v1;

-- support foreign reference constraints
BEGIN;
CREATE TABLE ri1 (i int PRIMARY KEY);
CREATE TABLE ri2 (i int PRIMARY KEY REFERENCES ri1(i) ON UPDATE CASCADE ON DELETE CASCADE, v int);
INSERT INTO ri1 VALUES (1),(2),(3);
INSERT INTO ri2 VALUES (1),(2),(3);
SELECT create_immv('mv_ri(i1, i2)', 'SELECT ri1.i, ri2.i FROM ri1 JOIN ri2 USING(i)');
SELECT * FROM mv_ri ORDER BY i1;
UPDATE ri1 SET i=10 where i=1;
DELETE FROM ri1 WHERE i=2;
SELECT * FROM mv_ri ORDER BY i2;
ROLLBACK;

-- support subquery for using EXISTS()
BEGIN;
SELECT create_immv('mv_ivm_exists_subquery', 'SELECT a.i, a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i)');
SELECT create_immv('mv_ivm_exists_subquery2', 'SELECT a.i,a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) AND a.i > 2');
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery2 ORDER BY i, j;
INSERT INTO mv_base_a VALUES(1,10),(6,60),(3,30),(3,300);
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery2 ORDER BY i, j;
INSERT INTO mv_base_b VALUES(1,101);
INSERT INTO mv_base_b VALUES(1,111);
INSERT INTO mv_base_b VALUES(2,102);
INSERT INTO mv_base_b VALUES(6,106);
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery2 ORDER BY i, j;
UPDATE mv_base_a SET i = 1 WHERE j =60;
UPDATE mv_base_b SET i = 10  WHERE k = 101;
UPDATE mv_base_b SET k = 1002 WHERE k = 102;
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery2 ORDER BY i, j;
DELETE FROM mv_base_a WHERE (i,j) = (1,60);
DELETE FROM mv_base_b WHERE i = 2;
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery2 ORDER BY i, j;
--- EXISTS subquery with tuple duplication and DISTINCT
SELECT create_immv('mv_ivm_exists_subquery_distinct', 'SELECT DISTINCT a.i, a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i)');
DELETE FROM mv_base_b WHERE i = 1 or i = 3;
INSERT INTO mv_base_b VALUES (1,100), (3,300);
SELECT * FROM mv_ivm_exists_subquery ORDER BY i, j;
SELECT * FROM mv_ivm_exists_subquery_distinct ORDER BY i, j;
ROLLBACK;

-- support simple subquery in FROM clause
BEGIN;
SELECT create_immv('mv_ivm_subquery', 'SELECT a.i,a.j FROM mv_base_a a,( SELECT * FROM mv_base_b) b WHERE a.i = b.i');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_ivm_subquery ORDER BY i,j;
ROLLBACK;

-- disallow non-simple subqueries
SELECT create_immv('mv_ivm_subquery', 'SELECT a.i,a.j FROM mv_base_a a, (SELECT i, COUNT(*) FROM mv_base_b GROUP BY i) b WHERE a.i = b.i');
SELECT create_immv('mv_ivm_subquery', 'SELECT a.i,a.j FROM mv_base_a a, (SELECT DISTINCT i FROM mv_base_b) b WHERE a.i = b.i');
SELECT create_immv('mv_ivm_subquery', 'SELECT i,j FROM mv_base_a WHERE i IN (SELECT i FROM mv_base_b WHERE k < 103 )');
SELECT create_immv('mv_ivm_subquery', 'SELECT i,j, (SELECT k FROM mv_base_b LIMIT 1) FROM mv_base_a a');
SELECT create_immv('mv_ivm_subquery', 'SELECT i,j, (SELECT k FROM mv_base_b LIMIT 1) + 1 FROM mv_base_a a');
SELECT create_immv('mv_ivm_subquery', 'SELECT * FROM generate_series(1, (SELECT k FROM mv_base_b LIMIT 1)) AS v');
SELECT create_immv('mv_ivm_subquery', 'SELECT a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i)');
SELECT create_immv('mv_ivm_subquery', 'SELECT a.i,a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) OR a.i > 2');
SELECT create_immv('mv_ivm_subquery', 'SELECT a.j FROM mv_base_a a WHERE EXISTS(SELECT 1 FROM mv_base_a a2 WHERE EXISTS(SELECT 1 FROM mv_base_b b WHERE a2.i = b.i))');
SELECT create_immv('mv_ivm_subquery', 'SELECT EXISTS(SELECT 1 from mv_base_b) FROM mv_base_a a');
SELECT create_immv('mv_ivm_subquery', 'SELECT false OR EXISTS(SELECT 1 FROM mv_base_a) FROM mv_base_b');
SELECT create_immv('mv_ivm_subquery', 'SELECT * FROM generate_series(1, CASE EXISTS(SELECT 1 FROM mv_base_a) WHEN true THEN 100 ELSE 10 END), mv_base_b');
SELECT create_immv('mv_ivm_subquery', 'SELECT * FROM mv_base_a a WHERE CASE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) WHEN true THEN false ELSE true END');
SELECT create_immv('mv_ivm_subquery', 'SELECT * FROM mv_base_a a WHERE true and CASE EXISTS(SELECT 1 FROM mv_base_b b WHERE a.i = b.i) WHEN true THEN false ELSE true END');

-- support join subquery in FROM clause
BEGIN;
SELECT create_immv('mv_ivm_join_subquery', 'SELECT i, j, k FROM ( SELECT i, a.j, b.k FROM mv_base_b b INNER JOIN mv_base_a a USING(i)) tmp');
WITH
 ai AS (INSERT INTO mv_base_a VALUES (1,11),(2,22) RETURNING 0),
 bi AS (INSERT INTO mv_base_b VALUES (1,111),(3,133) RETURNING 0),
 bd AS (DELETE FROM mv_base_b WHERE i = 4 RETURNING 0)
SELECT;
SELECT * FROM mv_ivm_join_subquery ORDER BY i,j,k;
ROLLBACK;
BEGIN;

-- nested subquery
SELECT create_immv('mv_ivm_join_subquery', 'SELECT i, j, k FROM ( SELECT i, a.j, b.k FROM mv_base_b b INNER JOIN (SELECT * FROM mv_base_a) a USING(i)) tmp');
WITH
 ai AS (INSERT INTO mv_base_a VALUES (1,11),(2,22) RETURNING 0),
 bi AS (INSERT INTO mv_base_b VALUES (1,111),(3,133) RETURNING 0),
 bd AS (DELETE FROM mv_base_b WHERE i = 4 RETURNING 0)
SELECT;
SELECT * FROM mv_ivm_join_subquery ORDER BY i,j,k;
ROLLBACK;

-- support simple CTE
BEGIN;
SELECT create_immv('mv_cte',
    'WITH b AS ( SELECT * FROM mv_base_b) SELECT a.i,a.j FROM mv_base_a a, b WHERE a.i = b.i');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_cte ORDER BY i,j;
ROLLBACK;

BEGIN;
SELECT create_immv('mv_cte',
    'WITH a AS (SELECT * FROM mv_base_a), b AS ( SELECT * FROM mv_base_b) SELECT a.i,a.j FROM a, b WHERE a.i = b.i');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_cte ORDER BY i,j;
ROLLBACK;

BEGIN;
SELECT create_immv('mv_cte',
    'WITH b AS ( SELECT * FROM mv_base_b) SELECT v.i,v.j FROM (WITH a AS (SELECT * FROM mv_base_a) SELECT a.i,a.j FROM a, b WHERE a.i = b.i) v');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_cte ORDER BY i,j;
ROLLBACK;

BEGIN;
SELECT create_immv('mv_cte',
    'SELECT * FROM (WITH a AS (SELECT * FROM mv_base_a), b AS ( SELECT * FROM mv_base_b) SELECT a.i,a.j FROM a, b WHERE a.i = b.i) v');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_cte ORDER BY i,j;
ROLLBACK;

BEGIN;
SELECT create_immv('mv_cte',
    'WITH b AS ( SELECT * FROM (SELECT * FROM mv_base_b) b2) SELECT v.i,v.j FROM (WITH a AS (SELECT * FROM mv_base_a) SELECT a.i,a.j FROM a, b WHERE a.i = b.i) v');
INSERT INTO mv_base_a VALUES(2,20);
INSERT INTO mv_base_b VALUES(3,300);
SELECT * FROM mv_cte ORDER BY i,j;
ROLLBACK;

BEGIN;
SELECT create_immv('mv_cte',
    'WITH x AS ( SELECT i, a.j, b.k FROM mv_base_b b INNER JOIN mv_base_a a USING(i)) SELECT * FROM x');
WITH
 ai AS (INSERT INTO mv_base_a VALUES (1,11),(2,22) RETURNING 0),
 bi AS (INSERT INTO mv_base_b VALUES (1,111),(3,133) RETURNING 0),
 bd AS (DELETE FROM mv_base_b WHERE i = 4 RETURNING 0)
SELECT;
SELECT * FROM mv_cte ORDER BY i,j,k;
ROLLBACK;

-- nested CTE
BEGIN;
SELECT create_immv('mv_ivm_nested_cte', 'WITH v AS ( WITH a AS (SELECT * FROM mv_base_a) SELECT i, a.j, b.k FROM mv_base_b b INNER JOIN a USING(i)) SELECT * FROM v');
WITH
 ai AS (INSERT INTO mv_base_a VALUES (1,11),(2,22) RETURNING 0),
 bi AS (INSERT INTO mv_base_b VALUES (1,111),(3,133) RETURNING 0),
 bd AS (DELETE FROM mv_base_b WHERE i = 4 RETURNING 0)
SELECT;
SELECT * FROM mv_ivm_nested_cte ORDER BY i,j,k;
ROLLBACK;

-- Multiply-referenced CTE
BEGIN;
CREATE TABLE base_t (i int, v int);
INSERT INTO base_t VALUES (1, 10), (2, 20), (3, 30);
SELECT create_immv('mv_cte_multi(v1, v2)',
 'WITH t AS (SELECT * FROM base_t) SELECT t1.v, t2.v FROM t AS t1 JOIN t AS t2 ON t1.i = t2.i');
SELECT * FROM mv_cte_multi ORDER BY v1;
INSERT INTO base_t VALUES (4,40);
DELETE FROM base_t WHERE i = 1;
UPDATE base_t SET v = v*10 WHERE i=2;
SELECT * FROM mv_cte_multi ORDER BY v1;
WITH
 ins_t1 AS (INSERT INTO base_t VALUES (5,50) RETURNING 1),
 ins_t2 AS (INSERT INTO base_t VALUES (6,60) RETURNING 1),
 upd_t AS (UPDATE base_t SET v = v + 100  RETURNING 1),
 dlt_t AS (DELETE FROM base_t WHERE i IN (4,5)  RETURNING 1)
SELECT NULL;
SELECT * FROM mv_cte_multi ORDER BY v1;
ROLLBACK;

--- disallow not-simple CTE
SELECT create_immv('mv_cte_fail', 'WITH b AS (SELECT i, COUNT(*) FROM mv_base_b GROUP BY i) SELECT a.i,a.j FROM mv_base_a a, b WHERE a.i = b.i');
SELECT create_immv('mv_cte_fail', 'WITH b AS (SELECT DISTINCT i FROM mv_base_b) SELECT a.i,a.j FROM mv_base_a a, b WHERE a.i = b.i');
SELECT create_immv('mv_cte_fail', 'WITH a AS (SELECT i, j FROM mv_base_a) SELECT a.i,a.j FROM a WHERE EXISTS(WITH b AS (SELECT i FROM mv_base_b) SELECT 1 FROM b WHERE a.i = b.i)');

-- unreferenced CTE
SELECT create_immv('mv_cte_fail', 'WITH b AS (SELECT * FROM mv_base_b) SELECT * FROM mv_base_a a');

-- views including NULL
BEGIN;
CREATE TABLE base_t (i int, v int);
INSERT INTO base_t VALUES (1,10),(2, NULL);
SELECT create_immv('mv', 'SELECT * FROM base_t');
SELECT * FROM mv ORDER BY i;
UPDATE base_t SET v = 20 WHERE i = 2;
SELECT * FROM mv ORDER BY i;
ROLLBACK;

BEGIN;
CREATE TABLE base_t (i int);
SELECT create_immv('mv', 'SELECT * FROM base_t');
SELECT * FROM mv ORDER BY i;
INSERT INTO base_t VALUES (1),(NULL);
SELECT * FROM mv ORDER BY i;
ROLLBACK;

BEGIN;
CREATE TABLE base_t (i int, v int);
INSERT INTO base_t VALUES (NULL, 1), (NULL, 2), (1, 10), (1, 20);
SELECT create_immv('mv', 'SELECT i, sum(v) FROM base_t GROUP BY i');
SELECT * FROM mv ORDER BY i;
UPDATE base_t SET v = v * 10;
SELECT * FROM mv ORDER BY i;
ROLLBACK;

BEGIN;
CREATE TABLE base_t (i int, v int);
INSERT INTO base_t VALUES (NULL, 1), (NULL, 2), (NULL, 3), (NULL, 4), (NULL, 5);
SELECT create_immv('mv', 'SELECT i, min(v), max(v) FROM base_t GROUP BY i');
SELECT * FROM mv ORDER BY i;
DELETE FROM base_t WHERE v = 1;
SELECT * FROM mv ORDER BY i;
DELETE FROM base_t WHERE v = 3;
SELECT * FROM mv ORDER BY i;
DELETE FROM base_t WHERE v = 5;
SELECT * FROM mv ORDER BY i;
ROLLBACK;

-- IMMV containing user defined type
BEGIN;

CREATE TYPE mytype;
CREATE FUNCTION mytype_in(cstring)
 RETURNS mytype AS 'int4in'
 LANGUAGE INTERNAL STRICT IMMUTABLE;
CREATE FUNCTION mytype_out(mytype)
 RETURNS cstring AS 'int4out'
 LANGUAGE INTERNAL STRICT IMMUTABLE;
CREATE TYPE mytype (
 LIKE = int4,
 INPUT = mytype_in,
 OUTPUT = mytype_out
);

CREATE FUNCTION mytype_eq(mytype, mytype)
 RETURNS bool AS 'int4eq'
 LANGUAGE INTERNAL STRICT IMMUTABLE;
CREATE FUNCTION mytype_lt(mytype, mytype)
 RETURNS bool AS 'int4lt'
 LANGUAGE INTERNAL STRICT IMMUTABLE;
CREATE FUNCTION mytype_cmp(mytype, mytype)
 RETURNS integer AS 'btint4cmp'
 LANGUAGE INTERNAL STRICT IMMUTABLE;

CREATE OPERATOR = (
 leftarg = mytype, rightarg = mytype,
 procedure = mytype_eq);
CREATE OPERATOR < (
 leftarg = mytype, rightarg = mytype,
 procedure = mytype_lt);

CREATE OPERATOR CLASS mytype_ops
 DEFAULT FOR TYPE mytype USING btree AS
 OPERATOR        1       <,
 OPERATOR        3       = ,
 FUNCTION		1		mytype_cmp(mytype,mytype);

CREATE TABLE t_mytype (x mytype);
SELECT create_immv('mv_mytype',
 'SELECT * FROM t_mytype');
INSERT INTO t_mytype VALUES ('1'::mytype);
SELECT * FROM mv_mytype;

ROLLBACK;

-- outer join is not supported
SELECT create_immv('mv(a,b)',
    'SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i');

-- contain system column
SELECT create_immv('mv_ivm01', 'SELECT i,j,xmin FROM mv_base_a');
SELECT create_immv('mv_ivm02', 'SELECT i,j FROM mv_base_a WHERE xmin = ''610''');
SELECT create_immv('mv_ivm03', 'SELECT i,j,xmin::text AS x_min FROM mv_base_a');
SELECT create_immv('mv_ivm04', 'SELECT i,j,xidsend(xmin) AS x_min FROM mv_base_a');

-- contain ORDER BY
SELECT create_immv('mv_ivm07', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) ORDER BY i,j,k');
-- contain HAVING
SELECT create_immv('mv_ivm08', 'SELECT i,j,k FROM mv_base_a a INNER JOIN mv_base_b b USING(i) GROUP BY i,j,k HAVING SUM(i) > 5');
-- contain GROUP BY without aggregate
SELECT create_immv('mv_ivm08', 'SELECT i,j FROM mv_base_a GROUP BY i,j');

-- contain view or materialized view
CREATE VIEW b_view AS SELECT i,k FROM mv_base_b;
CREATE MATERIALIZED VIEW b_mview AS SELECT i,k FROM mv_base_b;
SELECT create_immv('mv_ivm07', 'SELECT a.i,a.j FROM mv_base_a a,b_view b WHERE a.i = b.i');
SELECT create_immv('mv_ivm08', 'SELECT a.i,a.j FROM mv_base_a a,b_mview b WHERE a.i = b.i');

-- contain mutable functions
SELECT create_immv('mv_ivm12', 'SELECT i,j FROM mv_base_a WHERE i = random()::int');

-- LIMIT/OFFSET is not supported
SELECT create_immv('mv_ivm13', 'SELECT i,j FROM mv_base_a LIMIT 10 OFFSET 5');

-- DISTINCT ON is not supported
SELECT create_immv('mv_ivm14', 'SELECT DISTINCT ON(i) i, j FROM mv_base_a');

-- TABLESAMPLE clause is not supported
SELECT create_immv('mv_ivm15', 'SELECT i, j FROM mv_base_a TABLESAMPLE SYSTEM(50)');

-- window functions are not supported
SELECT create_immv('mv_ivm16', 'SELECT *, cume_dist() OVER (ORDER BY i) AS rank FROM mv_base_a');

-- aggregate function with some options is not supported
SELECT create_immv('mv_ivm17', 'SELECT COUNT(*) FILTER(WHERE i < 3) FROM mv_base_a');
SELECT create_immv('mv_ivm18', 'SELECT COUNT(DISTINCT i)  FROM mv_base_a');
SELECT create_immv('mv_ivm19', 'SELECT array_agg(j ORDER BY i DESC) FROM mv_base_a');
SELECT create_immv('mv_ivm20', 'SELECT i,SUM(j) FROM mv_base_a GROUP BY GROUPING SETS((i),())');

-- inheritance parent is not supported
BEGIN;
CREATE TABLE parent (i int, v int);
CREATE TABLE child_a(options text) INHERITS(parent);
SELECT create_immv('mv_ivm21', 'SELECT * FROM parent');
ROLLBACK;

-- UNION statement is not supported
SELECT create_immv('mv_ivm22', 'SELECT i,j FROM mv_base_a UNION ALL SELECT i,k FROM mv_base_b');

-- DISTINCT clause in nested query are not supported
SELECT create_immv('mv_ivm23', 'SELECT * FROM (SELECT DISTINCT i,j FROM mv_base_a) AS tmp');;

-- empty target list is not allowed with IVM
SELECT create_immv('mv_ivm25', 'SELECT FROM mv_base_a');

-- FOR UPDATE/SHARE is not supported
SELECT create_immv('mv_ivm26', 'SELECT i,j FROM mv_base_a FOR UPDATE');
SELECT create_immv('mv_ivm27', 'SELECT * FROM (SELECT i,j FROM mv_base_a FOR UPDATE) AS tmp;');

-- tartget list cannot contain ivm column that start with '__ivm'
SELECT create_immv('mv_ivm28', 'SELECT i AS "__ivm_count__" FROM mv_base_a');

-- expressions specified in GROUP BY must appear in the target list.
SELECT create_immv('mv_ivm29', 'SELECT COUNT(i) FROM mv_base_a GROUP BY i;');

-- experssions containing an aggregate is not supported
SELECT create_immv('mv_ivm30', 'SELECT sum(i)*0.5 FROM mv_base_a');
SELECT create_immv('mv_ivm31', 'SELECT sum(i)/sum(j) FROM mv_base_a');

-- VALUES is not supported
SELECT create_immv('mv_ivm_only_values1', 'values(1)');
SELECT create_immv('mv_ivm_only_values2',  'SELECT * FROM (values(1)) AS tmp');


-- views containing base tables with Row Level Security
DROP USER IF EXISTS ivm_admin;
DROP USER IF EXISTS ivm_user;
CREATE USER ivm_admin;
CREATE USER ivm_user;

--- create a table with RLS
SET SESSION AUTHORIZATION ivm_admin;
CREATE TABLE rls_tbl(id int, data text, owner name);
INSERT INTO rls_tbl VALUES
  (1,'foo','ivm_user'),
  (2,'bar','postgres');
CREATE TABLE num_tbl(id int, num text);
INSERT INTO num_tbl VALUES
  (1,'one'),
  (2,'two'),
  (3,'three'),
  (4,'four'),
  (5,'five'),
  (6,'six');

--- Users can access only their own rows
CREATE POLICY rls_tbl_policy ON rls_tbl FOR SELECT TO PUBLIC USING(owner = current_user);
ALTER TABLE rls_tbl ENABLE ROW LEVEL SECURITY;
GRANT ALL on rls_tbl TO PUBLIC;
GRANT ALL on num_tbl TO PUBLIC;

--- create a view owned by ivm_user
SET SESSION AUTHORIZATION ivm_user;
SELECT create_immv('ivm_rls', 'SELECT * FROM rls_tbl');
SELECT id, data, owner FROM ivm_rls ORDER BY 1,2,3;
RESET SESSION AUTHORIZATION;

--- inserts rows owned by different users
INSERT INTO rls_tbl VALUES
  (3,'baz','ivm_user'),
  (4,'qux','postgres');
SELECT id, data, owner FROM ivm_rls ORDER BY 1,2,3;

--- combination of diffent kinds of commands
WITH
 i AS (INSERT INTO rls_tbl VALUES(5,'quux','postgres'), (6,'corge','ivm_user')),
 u AS (UPDATE rls_tbl SET owner = 'postgres' WHERE id = 1),
 u2 AS (UPDATE rls_tbl SET owner = 'ivm_user' WHERE id = 2)
SELECT;
SELECT id, data, owner FROM ivm_rls ORDER BY 1,2,3;

---
SET SESSION AUTHORIZATION ivm_user;
SELECT create_immv('ivm_rls2', 'SELECT * FROM rls_tbl JOIN num_tbl USING(id)');
RESET SESSION AUTHORIZATION;

WITH
 x AS (UPDATE rls_tbl SET data = data || '_2' where id in (3,4)),
 y AS (UPDATE num_tbl SET num = num || '_2' where id in (3,4))
SELECT;
SELECT * FROM ivm_rls2 ORDER BY 1,2,3;

DROP TABLE rls_tbl CASCADE;
DROP TABLE num_tbl CASCADE;

DROP USER ivm_user;
DROP USER ivm_admin;

-- automatic index creation
BEGIN;
CREATE TABLE base_a (i int primary key, j int);
CREATE TABLE base_b (i int primary key, j int);

--- group by: create an index
SELECT create_immv('mv_idx1', 'SELECT i, sum(j) FROM base_a GROUP BY i');

--- distinct: create an index
SELECT create_immv('mv_idx2', 'SELECT DISTINCT j FROM base_a');

--- with all pkey columns: create an index
SELECT create_immv('mv_idx3(i_a, i_b)', 'SELECT a.i, b.i FROM base_a a, base_b b');

--- missing some pkey columns: no index
SELECT create_immv('mv_idx4', 'SELECT j FROM base_a');
SELECT create_immv('mv_idx5', 'SELECT a.i, b.j FROM base_a a, base_b b');

--- subqueries: create an index
SELECT create_immv('mv_idx6(i_a, i_b)', 'SELECT a.i, b.i FROM (SELECT * FROM base_a) a, (SELECT * FROM base_b) b');

--- with set-returning function: no index
SELECT create_immv('mv_idx7', 'SELECT i FROM base_a, generate_series(1,10)');

ROLLBACK;

-- type that doesn't have default operator class for access method btree
BEGIN;
CREATE TABLE table_json (j json);
SELECT create_immv('mv_json', 'SELECT * from table_json');
ROLLBACK;

-- prevent IMMV chanages
INSERT INTO mv_ivm_1 VALUES(1,1,1);
UPDATE  mv_ivm_1 SET k = 1 WHERE i = 1;
DELETE FROM mv_ivm_1;
TRUNCATE mv_ivm_1;

-- get_immv_def function
SELECT immvrelid, get_immv_def(immvrelid) FROM pg_ivm_immv ORDER BY 1;
-- mv_base_b is not immv
SELECT 'mv_base_b'::regclass, get_immv_def('mv_base_b');

DROP TABLE mv_base_b CASCADE;
DROP TABLE mv_base_a CASCADE;
