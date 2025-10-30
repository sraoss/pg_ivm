GRANT ALL ON SCHEMA public TO public;

-- support outer joins
BEGIN;
CREATE TABLE base_r(i int);
CREATE TABLE base_s (i int, j int);
CREATE TABLE base_t (j int);
INSERT INTO base_r VALUES (1), (2), (3), (3);
INSERT INTO base_s VALUES (2,1), (2,2), (3,1), (4,1), (4,2);
INSERT INTO base_t VALUES (2), (3), (3);

\set tl '*'

CREATE FUNCTION is_match(tl text) RETURNS text AS $$
DECLARE
x text;
BEGIN
 EXECUTE
 'SELECT CASE WHEN count(*) = 0 THEN ''OK'' ELSE ''NG'' END FROM (
	SELECT * FROM (SELECT ' || tl || ' FROM mv EXCEPT ALL SELECT * FROM v) v1
	UNION ALL
 SELECT * FROM (SELECT * FROM v EXCEPT ALL SELECT ' || tl || ' FROM mv) v2
 ) v' INTO x;
 RETURN x;
END;
$$ LANGUAGE plpgsql;

-- 3-way outer join (full & full)
SELECT pgivm.create_immv('mv(r,si,sj,t)', 
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j;
SELECT * FROM mv ORDER BY r, si, sj, t;
SAVEPOINT p1;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

-- TRUNCATE a base table in views with outer join
TRUNCATE base_r;
SELECT is_match(:'tl');
ROLLBACK TO p1;

TRUNCATE base_s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

TRUNCATE base_t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;


-- 3-way outer join (full & full) with DISTINCT
\set tl 'r, si, sj, t'
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT DISTINCT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT DISTINCT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j;
SELECT * FROM mv ORDER BY r, si, sj, t;
SAVEPOINT p1;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (full & left)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (full & right)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (full & inner)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

select * from mv;
DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
select * from mv;
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

select * from mv;
DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (left & full)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (left & left)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (left & right)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (left & inner)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r LEFT JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (right & full)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (right & left)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (right & right)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (right & inner)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r RIGHT JOIN base_s AS s ON r.i=s.i INNER JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (inner & full)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i FULL JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (inner & left)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i LEFT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 3-way outer join (inner & right)
SELECT pgivm.create_immv('mv(r, si, sj, t)',
 'SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j');
CREATE VIEW v(r, si, sj, t) AS
 SELECT r.i, s.i, s.j, t.j
   FROM base_r AS r INNER JOIN base_s AS s ON r.i=s.i RIGHT JOIN base_t AS t ON s.j=t.j;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, si, sj, t;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- 4-way outer join (full & full)
\set tl 'r, si, sj, t, ui, uj'
CREATE TABLE base_u (j int, i int);
INSERT INTO base_u VALUES (2,2), (2,3), (3,2), (3,3);
SELECT pgivm.create_immv('mv(r,si,sj,t,ui,uj)', 
 'SELECT r.i, s.i, s.j, t.j, u.i, u.j
   FROM (base_r AS r FULL JOIN base_s AS s ON r.i=s.i) FULL JOIN (base_t AS t FULL JOIN base_u AS u ON t.j=u.j) ON r.i=u.i');
CREATE VIEW v(r, si, sj, t,ui,uj) AS
 SELECT r.i, s.i, s.j, t.j, u.i, u.j
   FROM (base_r AS r FULL JOIN base_s AS s ON r.i=s.i) FULL JOIN (base_t AS t FULL JOIN base_u AS u ON t.j=u.j) ON r.i=u.i;
SELECT * FROM mv ORDER BY r, si, sj, t;
SAVEPOINT p1;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_t VALUES (1),(2);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
INSERT INTO base_t VALUES (3),(4);
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_t WHERE j=2;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
DELETE FROM base_t WHERE j=3;
SELECT * FROM mv ORDER BY r, si, sj, t, ui, uj;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- outer join with WHERE clause
\set tl 'r, s'
SELECT pgivm.create_immv('mv(r, s)',
 'SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i WHERE s.i > 0');
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i WHERE s.i > 0;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

INSERT INTO base_s VALUES (1,3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
INSERT INTO base_s VALUES (2,3);
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_s WHERE i=2;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=3;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
DELETE FROM base_s WHERE i=4;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- self outer join
\set tl 'r1, r2'
SELECT pgivm.create_immv('mv(r1, r2)',
 'SELECT r.i, r2.i
   FROM base_r AS r FULL JOIN base_r as r2 ON r.i=r2.i');
CREATE VIEW v(r1, r2) AS
 SELECT r.i, r2.i
   FROM base_r AS r FULL JOIN base_r as r2 ON r.i=r2.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r1, r2;

INSERT INTO base_r VALUES (1),(2),(3);
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match(:'tl');
INSERT INTO base_r VALUES (4),(5);
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DELETE FROM base_r WHERE i=1;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=2;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match(:'tl');
DELETE FROM base_r WHERE i=3;
SELECT * FROM mv ORDER BY r1, r2;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- support simultaneous table changes on outer join
\set tl 'r, s'
SELECT pgivm.create_immv('mv(r, s)',
 'SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i');
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

WITH
 ri AS (INSERT INTO base_r VALUES (1),(2),(3) RETURNING 0),
 si AS (INSERT INTO base_s VALUES (1,3) RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

WITH
 rd AS (DELETE FROM base_r WHERE i=1 RETURNING 0),
 sd AS (DELETE FROM base_s WHERE i=2 RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

-- multiple change of the same table on outer join
SELECT pgivm.create_immv('mv(r, s)',
 'SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i');
CREATE VIEW v(r, s) AS
 SELECT r.i, s.i
   FROM base_r AS r FULL JOIN base_s AS s ON r.i=s.i;
SAVEPOINT p1;
SELECT * FROM mv ORDER BY r, s;

WITH
 ri1 AS (INSERT INTO base_r VALUES (1),(2),(3) RETURNING 0),
 ri2 AS (INSERT INTO base_r VALUES (4),(5) RETURNING 0),
 rd AS (DELETE FROM base_r WHERE i IN (3,4) RETURNING 0)
SELECT;
SELECT * FROM mv ORDER BY r, s;
SELECT is_match(:'tl');
ROLLBACK TO p1;

DROP TABLE mv;
DROP VIEW v;

ROLLBACK;

CREATE TABLE mv_base_a (x int, i int, y int, j int);
CREATE TABLE mv_base_b (x int, i int, y int, k int);

-- outer join view's targetlist must contain vars in the join conditions
SELECT pgivm.create_immv('mv','SELECT a.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i');
SELECT pgivm.create_immv('mv','SELECT a.i,j,k FROM mv_base_a a LEFT JOIN mv_base_b b USING(i)');

-- outer join view's targetlist cannot contain non strict functions
SELECT pgivm.create_immv('mv','SELECT a.i, b.i, (k > 10 OR k = -1) FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i');
SELECT pgivm.create_immv('mv','SELECT a.i, b.i, CASE WHEN k>0 THEN 1 ELSE 0 END FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i');

-- outer join supports only simple equijoin
SELECT pgivm.create_immv('mv(a,b)','SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i>b.i');
SELECT pgivm.create_immv('mv(a,b,k,j)','SELECT a.i, b.i, k j FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i AND k=j');

-- outer join view's WHERE clause cannot contain non null-rejecting predicates
SELECT pgivm.create_immv('mv(a,b)','SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE k IS NULL');
SELECT pgivm.create_immv('mv(a,b)','SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE (k > 0 OR j > 0)');

-- aggregate is not supported with outer join
SELECT pgivm.create_immv('mv(a,b,v)','SELECT a.i, b.i, sum(k) FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i GROUP BY a.i, b.i');

-- subquery is not supported with outer join
SELECT pgivm.create_immv('mv(a,b)','SELECT a.i, b.i FROM mv_base_a a LEFT JOIN (SELECT * FROM mv_base_b) b ON a.i=b.i');
SELECT pgivm.create_immv('mv(a,b)','SELECT a.i, b.i FROM mv_base_a a LEFT JOIN mv_base_b b ON a.i=b.i WHERE EXISTS (SELECT 1 FROM mv_base_b b2 WHERE a.j = b.k)');

DROP TABLE mv_base_a;
DROP TABLE mv_base_b;
