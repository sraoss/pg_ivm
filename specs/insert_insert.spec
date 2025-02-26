# Test interaction between concurrent transactions performing
# table modifications in READ COMMITTED isolation level

setup
{
	CREATE TABLE a (i int, j int);
	CREATE TABLE b (i int, j int);
	INSERT INTO a VALUES (1,10);
	INSERT INTO b VALUES (1,100);
	SELECT pgivm.create_immv('mv(x,y,z)',
		'SELECT a1.j, a2.j,b.j FROM a AS a1, a AS a2,b WHERE a1.i = a2.i AND a1.i = b.i');
	CREATE VIEW v(x,y,z) AS
		SELECT a1.j, a2.j,b.j FROM a AS a1, a AS a2,b WHERE a1.i = a2.i AND a1.i = b.i;
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
	DROP TABLE b;
}

session tx1
setup	{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s1	{ SELECT; }
step insert1 { INSERT INTO a VALUES (2,20); INSERT INTO b VALUES (2,200); }
step update1 { UPDATE a SET j = 11 WHERE i = 1; }
step check1 { SELECT check_mv();}
step c1 { COMMIT; }
step mv { SELECT * FROM mv ORDER BY 1,2,3; SELECT check_mv(); }

session tx2
setup	{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step s2 { SELECT; }
step insert2 { INSERT INTO a VALUES (1,11), (2,21); INSERT INTO b VALUES (2,201); }
step update2 { UPDATE b SET j = 111 WHERE i = 1; }
step check2 { SELECT check_mv(); }
step c2 { COMMIT; }

permutation s1 update1 s2 update2 c1 check2 c2 mv
permutation s1 update1 s2 c1 update2 check2 c2 mv
permutation s1 s2 update1 update2 c1 check2 c2 mv
permutation s1 s2 update2 update1 c2 check1 c1 mv
permutation s1 s2 update1 c1 update2 check2 c2 mv
permutation s2 update2 s1 update1 c2 check1 c1 mv
permutation s2 update2 s1 c2 update1 check1 c1 mv
permutation s2 s1 update2 c2 update1 check1 c1 mv

permutation s1 insert1 s2 insert2 c1 check2 c2 mv
permutation s1 insert1 s2 c1 insert2 check2 c2 mv
permutation s1 s2 insert1 insert2 c1 check2 c2 mv
permutation s1 s2 insert2 insert1 c2 check1 c1 mv
permutation s1 s2 insert1 c1 insert2 check2 c2 mv
permutation s2 insert2 s1 insert1 c2 check1 c1 mv
permutation s2 insert2 s1 c2 insert1 check1 c1 mv
permutation s2 s1 insert2 c2 insert1 check1 c1 mv
