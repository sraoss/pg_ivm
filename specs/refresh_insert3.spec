# Test interaction between concurrent transactions performing
# refresh_immv and insert in SERIALIZABLE isolation level

setup
{
	CREATE TABLE a (i int);
	INSERT INTO a VALUES (1);
	SELECT pgivm.create_immv('mv(x,y)', 'SELECT * FROM a a1, a a2 WHERE a1.i = a2.i');
	CREATE VIEW v(x,y) AS SELECT * FROM a AS a1, a AS a2 WHERE a1.i = a2.i;
	CREATE FUNCTION check_mv() RETURNS text AS
		$$ SELECT CASE WHEN count(*) = 0 THEN 'ok' ELSE 'ng' END
			FROM ((SELECT * FROM mv EXCEPT ALL SELECT * FROM v) UNION ALL
				  (SELECT * FROM v EXCEPT ALL SELECT * FROM mv)) $$ LANGUAGE sql;
}

teardown
{
	DROP FUNCTION check_mv();
	DROP TABLE mv;
	DROP VIEW v;
	DROP TABLE a;
}

session tx1
setup		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step s1	{ SELECT; }
step refresh { SELECT pgivm.refresh_immv('mv', true); }
step check1 {SELECT check_mv();}
step c1 { COMMIT; }
step mv { SELECT * FROM mv ORDER BY 1,2; SELECT check_mv(); }

session tx2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step s2	{ SELECT; }
step insert { INSERT INTO a VALUES (2); }
step check2 {SELECT check_mv(); }
step c2 { COMMIT; }

permutation s1 refresh s2 insert c1 check2 c2 mv
permutation s1 refresh s2 c1 insert check2 c2 mv
permutation s1 s2 refresh insert c1 check2 c2 mv
permutation s1 s2 insert refresh c2 check1 c1 mv
permutation s1 s2 refresh c1 insert check2 c2 mv
permutation s2 insert s1 refresh c2 check1 c1 mv
permutation s2 insert s1 c2 refresh check1 c1 mv
permutation s2 s1 insert c2 refresh check1 c1 mv
