# Test interaction between concurrent transactions performing
# create_immv and insert in REPEATABLE READ isolation level
#
# Note:
# In this isolation level, it is possible that create_immv could
# create an inconsistent view not including effects of a concurrent
# transaction. So, an warning message is raised to suggest using it
# in READ COMMITTED or executing refresh_immv to make sure to
# make the view contents consistent.

setup
{
	CREATE TABLE a (i int);
	INSERT INTO a VALUES (1);
	CREATE VIEW v(x,y) AS SELECT * FROM a AS a1, a AS a2 WHERE a1.i = a2.i;
}

teardown
{
	DROP FUNCTION check_mv();
	DROP TABLE mv;
	DROP VIEW v;
	DROP TABLE a;
}

session tx1
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s1	{ SELECT; }
step create {
	SELECT pgivm.create_immv('mv(x,y)', 'SELECT * FROM a a1, a a2 WHERE a1.i = a2.i');
	CREATE FUNCTION check_mv() RETURNS text AS
		$$ SELECT CASE WHEN count(*) = 0 THEN 'ok' ELSE 'ng' END
			FROM ((SELECT * FROM mv EXCEPT ALL SELECT * FROM v) UNION ALL
				  (SELECT * FROM v EXCEPT ALL SELECT * FROM mv)) v $$ LANGUAGE sql;
}
step check1 {SELECT check_mv();}
step c1 { COMMIT; }
step mv { SELECT * FROM mv ORDER BY 1,2; SELECT check_mv(); }

session tx2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step s2	{ SELECT; }
step insert { INSERT INTO a VALUES (2); }
step check2 {SELECT check_mv(); }
step c2 { COMMIT; }

permutation s1 create s2 insert c1 check2 c2 mv
permutation s1 create s2 c1 insert check2 c2 mv
permutation s1 s2 create insert c1 check2 c2 mv
permutation s1 s2 insert create c2 check1 c1 mv
permutation s1 s2 create c1 insert check2 c2 mv
permutation s2 insert s1 create c2 check1 c1 mv
permutation s2 insert s1 c2 create check1 c1 mv
permutation s2 s1 insert c2 create check1 c1 mv
