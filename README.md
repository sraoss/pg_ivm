# pg_ivm

The `pg_ivm` module provides Incremental View Maintenance (IVM) feature for PostgreSQL.

The extension is compatible with PostgreSQL 13, 14, 15, 16, and 17.

## Description

**Incremental View Maintenance (IVM)** is a way to make materialized views up-to-date in which only incremental changes are computed and applied on views rather than recomputing the contents from scratch as `REFRESH MATERIALIZED VIEW` does.  IVM can update materialized views more efficiently than recomputation when only small parts of the view are changed.

There are two approaches with regard to timing of view maintenance: immediate and deferred.  In immediate maintenance, views are updated in the same transaction that its base table is modified.  In deferred maintenance, views are updated after the transaction is committed, for example, when the view is accessed, as a response to user command like `REFRESH MATERIALIZED VIEW`, or periodically in background, and so on. `pg_ivm` provides a kind of immediate maintenance, in which materialized views are updated immediately in AFTER triggers when a base table is modified.

We call a materialized view supporting IVM an **Incrementally Maintainable Materialized View (IMMV)**. To create IMMV, you have to call `create_immv` function with a relation name and a view definition query. For example:

```sql
SELECT pgivm.create_immv('myview', 'SELECT * FROM mytab');
```

creates an IMMV with name 'myview' defined as `SELECT * FROM mytab`. This is corresponding to the following command to create a normal materialized view;

```sql
CREATE MATERIALIZED VIEW myview AS SELECT * FROM mytab;
```

When an IMMV is created, some triggers are automatically created so that the view's contents are immediately updated when its base tables are modified.

```sql
postgres=# SELECT pgivm.create_immv('m', 'SELECT * FROM t0');
NOTICE:  could not create an index on immv "m" automatically
DETAIL:  This target list does not have all the primary key columns, or this view does not contain DISTINCT clause.
HINT:  Create an index on the immv for efficient incremental maintenance.
 create_immv 
-------------
           3
(1 row)

postgres=# SELECT * FROM m;
 i
---
 1
 2
 3
(3 rows)

postgres=# INSERT INTO t0 VALUES (4);
INSERT 0 1
postgres=# SELECT * FROM m; -- automatically updated
 i
---
 1
 2
 3
 4
(4 rows)
```

Note that if you use PostgreSQL 17 or later, during automatic maintenance of an IMMV, the `search_path` is temporarily changed to `pg_catalog, pg_temp`.

## Installation

To install `pg_ivm`, execute this in the module's directory:

```shell
make install
```

If you installed PostgreSQL from rpm or deb, you will need the devel package (for example, postgresql14-devel or postgresql-server-dev-14).

> **Important:** Don't forget to set the `PG_CONFIG` variable (`make PG_CONFIG=...`) or the `PATH` to the `pg_config` command in case you want to use `pg_ivm` on a non-default or custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

Once installed, execute `CREATE EXTENSION` comand.

```sql
CREATE EXTENSION pg_ivm;
```

### Configuration

To ensure `pg_ivm` maintains IMMVs correctly, add it to either shared_preload_libraries or session_preload_libraries in postgresql.conf:

```
# Add pg_ivm to preload libraries
shared_preload_libraries = 'pg_ivm'
# OR
session_preload_libraries = 'pg_ivm'
```

After making this change, restart PostgreSQL for the configuration to take effect.

### RPM packages and yum repository

RPM packages of pg_ivm are available from the [PostgreSQL yum repository](https://yum.postgresql.org/). See the [instruction](https://yum.postgresql.org/howto/) for details. Note that we are not the maintainer of this yum repository and RPMs for pg_ivm in it may be not always latest.

## Objects

When `pg_ivm` is installed, the 'pgivm' schema is created, along with the following objects within this schema.

### Functions

#### create_immv

Use `create_immv` function to create IMMV.
```
pgivm.create_immv(immv_name text, view_definition text) RETURNS bigint
```
`create_immv` defines a new IMMV of a query. A table of the name `immv_name` is created and a query specified by `view_definition` is executed and used to populate the IMMV. The query is stored in `pg_ivm_immv`, so that it can be refreshed later upon incremental view maintenance. `create_immv` returns the number of rows in the created IMMV.

When an IMMV is created, some triggers are automatically created so that the view's contents are immediately updated when its base tables are modified. In addition, a unique index is created on the IMMV automatically if possible.  If the view definition query has a GROUP BY clause, a unique index is created on the columns of GROUP BY expressions. Also, if the view has DISTINCT clause, a unique index is created on all columns in the target list. Otherwise, if the IMMV contains all primary key attributes of its base tables in the target list, a unique index is created on these attributes.  In other cases, no index is created.

`create_immv` acquires an `AccessExclusiveLock` on the view. However, even if we are able to acquire the lock, a concurrent transaction may have already incrementally updated and committed the view before we can acquire it. In the `REPEATABLE READ` or `SERIALIZABLE` isolation levels, this could lead to an inconsistent state of the view. Unfortunately, this situation cannot be detected during the creation of the view. As a result, `create_immv` raises a warning in these isolation levels, suggesting that the command be used in `READ COMMITTED` or that `refresh_immv` be executed afterward to make the view's contents remain consistent.

Note that if you use PostgreSQL 17 or later, while `create_immv` is running, the `search_path` is temporarily changed to `pg_catalog, pg_temp`.

#### refresh_immv

Use `refresh_immv` function to refresh IMMV.
```
pgivm.refresh_immv(immv_name text, with_data bool) RETURNS bigint
```

`refresh_immv` completely replaces the contents of an IMMV as `REFRESH MATERIALIZED VIEW` command does for a materialized view. To execute this function you must be the owner of the IMMV (with PostgreSQL 16 or earlier) or have the `MAINTAIN` privilege on the IMMV (with PostgreSQL 17 or later).  The old contents are discarded.

The with_data flag is corresponding to `WITH [NO] DATA` option of REFRESH MATERIALIZED VIEW` command. If with_data is true, the backing query is executed to provide the new data, and if the IMMV is unpopulated, triggers for maintaining the view are created. Also, a unique index is created for IMMV if it is possible and the view doesn't have that yet. If with_data is false, no new data is generated and the IMMV become unpopulated, and the triggers are dropped from the IMMV. Note that unpopulated IMMV is still scannable although the result is empty. This behaviour may be changed in future to raise an error when an unpopulated IMMV is scanned.

`refresh_immv` acquires `AccessExclusiveLock` on the view. However, even if we are able to acquire the lock, a concurrent transaction may have already incrementally updated and committed the view before we can acquire the lock.  In `REPEATABLE READ` or `SERIALIZABLE` isolation level, this could lead to an inconsistent state of the view. Therefore, an error is raised to prevent anomalies when this situation is detected.

Note that if you use PostgreSQL 17 or later, while `refresh_immv` is running, the `search_path` is temporarily changed to `pg_catalog, pg_temp`.

#### get_immv_def

`get_immv_def` reconstructs the underlying SELECT command for an IMMV. (This is a decompiled reconstruction, not the original text of the command.)
```
pgivm.get_immv_def(immv regclass) RETURNS text
```

### IMMV metadata catalog

The catalog `pgivm.pg_ivm_immv` stores IMMV information.

|Name|Type|Description|
|:---|:---|:---|
|immvrelid|regclass|The OID of the IMMV|
|viewdef|text|Query tree (in the form of a nodeToString() representation) for the view definition|
|ispopulated|bool|True if IMMV is currently populated|
|lastivmupdate|xid8|The transaction ID of the most recent incremental update to the view|


## Example

### `CREATE MATERIALIZED VIEW` and `REFRESH MATERIALIZED VIEW`

In general, IMMVs allow faster updates than `REFRESH MATERIALIZED VIEW` at the price of slower updates to their base tables. Update of base tables is slower because triggers will be invoked and the IMMV is updated in triggers per modification statement.

For example, suppose a normal materialized view defined as below:

```sql
test=# CREATE MATERIALIZED VIEW mv_normal AS
        SELECT a.aid, b.bid, a.abalance, b.bbalance
        FROM pgbench_accounts a JOIN pgbench_branches b USING(bid);
SELECT 10000000
```

Updating a tuple in a base table of this materialized view is rapid but the `REFRESH MATERIALIZED VIEW` command on this view takes a long time:

```sql
test=# UPDATE pgbench_accounts SET abalance = 1000 WHERE aid = 1;
UPDATE 1
Time: 9.052 ms

test=# REFRESH MATERIALIZED VIEW mv_normal ;
REFRESH MATERIALIZED VIEW
Time: 20575.721 ms (00:20.576)
```

### Creating an IMMV

On the other hand, after creating IMMV with the same view definition as below:

```
test=# SELECT pgivm.create_immv('immv',
        'SELECT a.aid, b.bid, a.abalance, b.bbalance
         FROM pgbench_accounts a JOIN pgbench_branches b USING(bid)');
NOTICE:  created index "immv_index" on immv "immv"
 create_immv 
-------------
    10000000
(1 row)
```

Updating a tuple in a base table takes more than the normal view, but its content is updated automatically and this is faster than the `REFRESH MATERIALIZED VIEW` command.

```sql
test=# UPDATE pgbench_accounts SET abalance = 1234 WHERE aid = 1;
UPDATE 1
Time: 15.448 ms

test=# SELECT * FROM immv WHERE aid = 1;
 aid | bid | abalance | bbalance 
-----+-----+----------+----------
   1 |   1 |     1234 |        0
(1 row)
```

An appropriate index on IMMV is necessary for efficient IVM because we need to look for tuples to be updated in IMMV.  If there are no indexes, it will take a long time.

Therefore, when an IMMV is created by the `create_immv` function, a unique index is created on it automatically if possible. If the view definition query has a GROUP BY clause, a unique index is created on the columns of GROUP BY expressions. Also, if the view has DISTINCT clause, a unique index is created on all columns in the target list. Otherwise, if the IMMV contains all primary key attributes of its base tables in the target list, a unique index is created on these attributes.  In other cases, no index is created.

In the previous example, a unique index "immv_index" is created on aid and bid columns of "immv", and this enables the rapid update of the view. Dropping this index make updating the view take a loger time.

```sql
test=# DROP INDEX immv_index;
DROP INDEX

test=# UPDATE pgbench_accounts SET abalance = 9876 WHERE aid = 1;
UPDATE 1
Time: 3224.741 ms (00:03.225)
```

### IMMV with Aggregate Functions

You can create an IMMV that includes aggregate functions.

```sql
test=# SELECT pgivm.create_immv('immv_agg',
        'SELECT bid, count(*), sum(abalance), avg(abalance)
         FROM pgbench_accounts JOIN pgbench_branches USING(bid) GROUP BY bid');
NOTICE:  created index "immv_agg_index" on immv "immv_agg"
 create_immv 
-------------
         100
(1 row)

Time: 5772.625 ms (00:05.773)
```

Creating this view takes about five seconds, and the normal refresh operation requires a similar amount of time. The following example demonstrates refreshing the IMMV using `refresh_immv`. The execution time would be approximately the same if you used `REFRESH MATERIALIZED VIEW` on a regular materialized view with the same definition.

```sql
test=# SELECT pgivm.refresh_immv('immv_agg',true);
 refresh_immv
--------------
          100
(1 row)

Time: 5766.292 ms (00:05.766)
```

When a base table is updated, the IMMV is also automatically updated incrementally, as expected.

```sql
test=# SELECT bid, count, sum, avg FROM immv_agg WHERE bid = 42;
 bid | count  |  sum  |          avg
-----+--------+-------+------------------------
  42 | 100000 | 38774 | 0.38774000000000000000
(1 row)

Time: 3.123 ms

test=# UPDATE pgbench_accounts SET abalance = abalance + 1000 WHERE aid = 4112345 AND bid = 42;
UPDATE 1
Time: 16.616 ms

test=# SELECT bid, count, sum, avg FROM immv_agg WHERE bid = 42;
 bid | count  |  sum  |          avg
-----+--------+-------+------------------------
  42 | 100000 | 39774 | 0.39774000000000000000
(1 row)

Time: 1.987 ms
```

After updating a row in the `pgbench_accounts` table, you can see that the aggregated values in `immv_agg` are updated automatically.

### Listing IMMVs and Viewing Their Definitions

You can find all IMMVs in the `pg_ivm_immv` catalog and view their definition queries by executing the `get_immv_def` function."

```sql
test=# SELECT immvrelid AS immv, pgivm.get_immv_def(immvrelid) AS immv_def FROM pgivm.pg_ivm_immv;
   immv   |                  immv_def
----------+--------------------------------------------
 immv_agg |  SELECT pgbench_accounts.bid,             +
          |     count(*) AS count,                    +
          |     sum(pgbench_accounts.abalance) AS sum,+
          |     avg(pgbench_accounts.abalance) AS avg +
          |    FROM (pgbench_accounts                 +
          |      JOIN pgbench_branches USING (bid))   +
          |   GROUP BY pgbench_accounts.bid
 immv     |  SELECT a.aid,                            +
          |     b.bid,                                +
          |     a.abalance,                           +
          |     b.bbalance                            +
          |    FROM (pgbench_accounts a               +
          |      JOIN pgbench_branches b USING (bid))
(2 rows)
```

### Dropping an IMMV

An IMMV can be dropped using the `DROP TABLE` command. Once an IMMV is dropped, its entry is automatically removed from the `pg_ivm_immv` catalog.

```sql
test=# DROP TABLE immv
DROP TABLE

test=# SELECT immvrelid AS immv, pgivm.get_immv_def(immvrelid) AS immv_def FROM pgivm.pg_ivm_immv;
   immv   |                  immv_def
----------+--------------------------------------------
 immv_agg |  SELECT pgbench_accounts.bid,             +
          |     count(*) AS count,                    +
          |     sum(pgbench_accounts.abalance) AS sum,+
          |     avg(pgbench_accounts.abalance) AS avg +
          |    FROM (pgbench_accounts                 +
          |      JOIN pgbench_branches USING (bid))   +
          |   GROUP BY pgbench_accounts.bid
(1 row)
```

### Renaming an IMMV

An IMMV can be renamed using the `ALTER TABLE` command.

```sql
test=# ALTER TABLE immv_agg RENAME TO immv_agg2;
ALTER TABLE

test=# SELECT immvrelid AS immv, pgivm.get_immv_def(immvrelid) AS immv_def FROM pgivm.pg_ivm_immv;
   immv    |                  immv_def
-----------+--------------------------------------------
 immv_agg2 |  SELECT pgbench_accounts.bid,             +
           |     count(*) AS count,                    +
           |     sum(pgbench_accounts.abalance) AS sum,+
           |     avg(pgbench_accounts.abalance) AS avg +
           |    FROM (pgbench_accounts                 +
           |      JOIN pgbench_branches USING (bid))   +
           |   GROUP BY pgbench_accounts.bid
(1 row)
```

## `pg_dump` and `pg_upgrade`

After restoring data from a `pg_dump` backup or upgrading `PostgreSQL` using `pg_upgrade`, all IMMVs must be manually dropped and recreated.

## Supported View Definitions and Restriction

Currently, IMMV's view definition can contain inner joins, DISTINCT clause, some built-in aggregate functions, simple sub-queries in `FROM` clause, EXISTS sub-queries, and simple CTE (`WITH` query). Inner joins including self-join are supported, but outer joins are not supported. Supported aggregate functions are count, sum, avg, min and max. Other aggregates, sub-queries which contain an aggregate or `DISTINCT` clause, sub-queries in other than `FROM` clause, window functions, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `UNION`/`INTERSECT`/`EXCEPT`, `DISTINCT ON`, `TABLESAMPLE`, `VALUES`, and `FOR UPDATE`/`SHARE` can not be used in view definition.

The base tables must be simple tables. Views, materialized views, inheritance parent tables, partitioned tables, partitions, and foreign tables can not be used.

Any system column cannot be included in the view definition query.
The target list cannot columns whose name starts with `__ivm_`.

Data type used in the target list in the view must have default operator class for access method btree. For example, `json`, `xml`, or `point` type cannot be in the target list.

Logical replication is not supported, that is, even when a base table at a publisher node is modified, IMMVs at subscriber nodes defined on these base tables are not updated.

## Notes

### Aggregates

Supported aggregate functions are `count`, `sum`, `avg`, `min`, and `max`. Currently, only built-in aggregate functions are supported and user defined aggregates cannot be used.

When an IMMV including aggregate is created, some extra columns whose name start with `__ivm` are automatically added to the target list. `__ivm_count__` contains the number of tuples aggregated in each group. In addition, more than one extra columns for each column of aggregated value  are added in order to maintain the value. For example, columns named like  `__ivm_count_avg__` and `__ivm_sum_avg__` are added for maintaining an average value. When a base table is modified, the new aggregated values are incrementally calculated using the old aggregated values and values of related extra  columns stored in the IMMV.

Note that for `min` or `max`, the new values could be re-calculated from base tables with regard to the affected groups when a tuple containing the current minimal or maximal values are deleted from a base table. Therefore, it can takes a long time to update an IMMV containing these functions.

Also, note that using `sum` or `avg` on `real` (`float4`) type or `double precision` (`float8`) type in IMMV is unsafe, because aggregated values in IMMV can become different from results calculated from base tables due to the limited precision of these types. To avoid this problem, use the `numeric` type instead.

#### Restrictions on Aggregate

If we have a `GROUP BY` clause, expressions specified in `GROUP BY` must appear in the target list. This is how tuples to be updated in the IMMV are identified. These attributes are used as scan keys for searching tuples in the IMMV, so indexes on them are required for efficient IVM.

Targetlist cannot contain expressions which contain an aggregate in it.

### Subqueries

Simple subqueries in `FROM` clause and EXISTS subqueries in 'WHERE' clause are supported.

#### Restrictions on Subqueries

Subqueries using EXISTS and simple subqueries in FROM clause are supported. EXISTS subqueries with condition other than 'AND' and Subqueries in targetlist are not supported. EXISTS subquery is supported only in WHERE but not in the targetlist.

If EXISTS contains columns that refer to columns in tables in the outer query, such columns must be included in the targetlist.
Subqueries containing an aggregate function or `DISTINCT` are not supported.

### CTE

Simple CTEs (`WITH` queries) are supported.

#### Restrictions on CTEs

`WITH` queries containing an aggregate function or `DISTINCT` are not supported.

Recursive queries (`WITH RECURSIVE`) are not allowed. Unreferenced CTEs are not allowed either, that is, a CTE must be referenced at least once in the view definition query.

### DISTINCT

`DISTINCT` is allowed in IMMV's definition queries. Suppose an IMMV defined with DISTINCT on a base table containing duplicate tuples.  When tuples are deleted from the base table, a tuple in the view is deleted if and only if the multiplicity of the tuple becomes zero.  Moreover, when tuples are inserted into the base table, a tuple is inserted into the view only if the same tuple doesn't already exist in it.

Physically, an IMMV defined with `DISTINCT` contains tuples after eliminating duplicates, and the multiplicity of each tuple is stored in an extra column named `__ivm_count__` that is added when such IMMV is created.

### TRUNCATE

When a base table is truncated, the IMMV is also truncated and the contents become empty if the view definition query does not contain an aggregate without a `GROUP BY` clause. Aggregate views without a `GROUP BY` clause always have one row. Therefore, in such cases, if a base table is truncated, the IMMV is simply refreshed instead of being truncated.


### Concurrent Transactions

Incremental updates of a view are basically performed in sequentially even with concurrent transactions running.

Suppose an IMMV is defined on two base tables and each table is modified in different concurrent transactions simultaneously. In the transaction which was committed first, the IMMV can be updated considering only the change made in that transaction. However, to update the IMMV correctly in the transaction that commits later, we need to account for the changes made in both transactions.

 For this reason, `ExclusiveLock` is held on an IMMV immediately after a base table is modified in `READ COMMITTED` isolation level. This ensures that the IMMV is updated in the latter transaction only after the former transaction has committed.  In `REPEATABLE READ` or `SERIALIZABLE` isolation level, an error is raised immediately if lock acquisition fails, as changes made by other transactions are not visible in these levels,  and the IMMV cannot be updated correctly in such situations.

However, as an exception if the IMMV has only one base table, does not  use DISTINCT or GROUP BY, and is modified by `INSERT`, then the lock held on the IMMV is `RowExclusiveLock`.

Even if we are able to acquire a lock, a concurrent transaction may have already incrementally updated and committed the view before we can acquire the lock.  In `REPEATABLE READ` or `SERIALIZABLE` isolation level, this could lead to an inconsistent state of the view. Therefore, an error is raised to prevent anomalies when this situation is detected.

### Row Level Security

If some base tables have row level security policy, rows that are not visible to the materialized view's owner are excluded from the result.  In addition, such rows are excluded as well when views are incrementally maintained.  However, if a new policy is defined or policies are changed after the materialized view was created, the new policy will not be applied to the view contents.  To apply the new policy, you need to refresh or recreate the IMMV.

### How to Disable or Enable Immediate Maintenance

IVM is effective when we want to keep an IMMV up-to-date and small fraction of a base table is modified infrequently.  Due to the overhead of immediate maintenance, IVM is not effective when a base table is modified frequently.  Also, when a large part of a base table is modified or large data is inserted into a base table, IVM is not effective and the cost of maintenance can be larger than refresh from scratch.

In such situation, we can use `refresh_immv` function with `with_data = false` to disable immediate maintenance before modifying a base table. After a base table modification, call `refresh_immv`with `with_data = true` to refresh the view data and enable immediate maintenance.

## Developer
IVM Development Group
- Original Authors
  - https://github.com/yugo-n
  - https://github.com/thoshiai

## License
[PostgreSQL License](https://github.com/sraoss/pg_ivm/blob/main/LICENSE)

## Copyright
- Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
- Portions Copyright (c) 2022, IVM Development Group

