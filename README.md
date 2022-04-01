# pg_ivm

The `pg_ivm` module provides Incremental View Maintenance (IVM) feature for PostgreSQL. 

The extension is compatible with PostgreSQL 14.

## Description

**Incremental View Maintenance (IVM)** is a way to make materialized views up-to-date in which only incremental changes are computed and applied on views rather than recomputing the contents from scratch as `REFRESH MATERIALIZED VIEW` does.  IVM can update materialized views more efficiently than recomputation when only small parts of the view are changed.

There are two approaches with regard to timing of view maintenance: immediate and deferred.  In immediate maintenance, views are updated in the same transaction that its base table is modified.  In deferred maintenance, views are updated after the transaction is committed, for example, when the view is accessed, as a response to user command like `REFRESH MATERIALIZED VIEW`, or periodically in background, and so on. `pg_ivm` provides a kind of immediate maintenance, in which materialized views are updated immediately in AFTER triggers when a base table is modified.

 We call a materialized view supporting IVM an **Incrementally Maintainable Materialized View (IMMV)**. To create IMMV, you have to call `create_immv` function with a relation name and a view definition query. For example:

```sql
SELECT create_immv('myview', 'SELECT * FROM mytab');
```

creates an IMMV with name 'myview' defined as 'SELECT * FROM mytab'. This is corresponding to the following command to create a normal materialized view;

```sql
CREATE MATERIALIZED VIEW myview AS SELECT * FROM mytab;
```

When an IMMV is created, some triggers are automatically created so that the view's contents are immediately updated when its base tables are modified. 

```sql
postgres=# SELECT create_immv('m', 'SELECT * FROM t0');
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

## Installation
To install `pg_ivm`, execute this in the module's directory:

```shell
make install USE_PGXS=1
```

> **Important:** Don't forget to set the `PG_CONFIG` variable (`make PG_CONFIG=...`) in case you want to test `pg_pathman` on a non-default or custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

## Objects

When `pg_ivm` is installed, the following objects are created.

### IMMV creation function

Use `create_immv` function to creae IMMV.
```
create_immv(immv_name text, view_definition text) RETURNS bigint
```
`create_immv` defined a new IMMV of a query. A table of the name `immv_name` is created and a query specified by `view_definition` is executed and used to populate the IMMV. The query is stored in `pg_ivm_immv`, so that it can be refreshed later upon incremental view maintenance. `create_immv` returns the number of rows in the creaetd IMMV.

When an IMMV is created, some triggers are automatically created so that the view's contents are immediately updated when its base tables are modified. In addition, a unique index is created on the IMMV automatically if possible.  If the IMMV contains all primary key attritubes of its base tables in the target list, a unique index is created on these attritubes. Or, if the view has DISTINCT clause, a unique index is created on all columns in the target list. In other cases, no index is created.

### IMMV metadata catalog

The catalog `pg_ivm_immv` stores IMMV information.

|Name|Type|Description|
|:---|:---|:---|
|immvrelid|regclass|The OID of the IMMV|
|viewdef|text|Query tree (in the form of a nodeToString() representation) for the view definition|


## Example

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

On the other hand, after creating IMMV with the same view definition as below:

```
test=# SELECT create_immv('immv',
        'SELECT a.aid, b.bid, a.abalance, b.bbalance
         FROM pgbench_accounts a JOIN pgbench_branches b USING(bid)');
NOTICE:  created index "immv_index" on immv "immv"
 create_immv 
-------------
    10000000
(1 row)
```

updating a tuple in a base table takes more than the normal view, but its content is updated automatically and this is faster than the `REFRESH MATERIALIZED VIEW` command.

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

An appropriate indexe on IMMV is necessary for efficient IVM because we need to looks for tuples to be updated in IMMV.  If there are no indexes, it will take a long time.

Therefore, when an IMMV is created by the `create_immv` function, a unique index is created on it automatically if possible.  If the IMMV contains all primary key attritubes of its base tables in the target list, a unique index is created on these attritubes. Also, if the view has DISTINCT clause, a unique index is created on all columns in the target list. In other cases, no index is created.

In the previous example, a unique index "immv_index" is created on aid and bid columns of "immv", and this enables the rapid update of the view. Dropping this index make updating the view take a loger time.

```sql
test=# DROP INDEX immv_index;
DROP INDEX

test=# UPDATE pgbench_accounts SET abalance = 9876 WHERE aid = 1;
UPDATE 1
Time: 3224.741 ms (00:03.225)
```

## Supported View Definitions and Restriction

Currently, IMMV's view definition can contain inner joins, and DISTINCT clause. Inner joins including self-join are supported, but outer joins are not supported. Aggregates, sub-quereis, CTEs, window functions, LIMIT/OFFSET, UNION/INTERSECT/EXCEPT, DISTINCT ON, TABLEAMPLE, VALUES, and FOR UPDATE/SHARE can not be used in view definition.

The base tables must be simple. Views, materialized views, inheritance parent tables, partitioned tables, partitions, and foreign tables can not be used.

The targetlist cannot contain system columns, columns whose name starts with `__ivm_`.

Logical replication is not supported, that is, even when a base table at a publisher node is modified, IMMVs at subscriber nodes defined on these base tables are not updated.

When the `TRUNCATE` command is executed on a base table, nothing is changed on the IMMV.

## Notes

### DISTINCT

`DISTINCT` is allowed in IMMV's definition queries. Suppose an IMMV defined with DISTINCT on a base table containing duplicate tuples.  When tuples are deleted from the base table, a tuple in the view is deleted if and only if the multiplicity of the tuple becomes zero.  Moreover, when tuples are inserted into the base table, a tuple is inserted into the view only if the same tuple doesn't already exist in it.

Physically, an IMMV defined with `DISTINCT` contains tuples after eliminating duplicates, and the multiplicity of each tuple is stored in a extra column named `__ivm_count__` that is added when such IMMV is created.

### Concurrent Transactions

Suppose an IMMV is defined on two base tables and each table was modified in different a concurrent transaction simultaneously. In the transaction which was committed first, the IMMV can be updated considering only the change which happened in this transaction. On the other hand, in order to update the IMMV correctly in the transaction which was committed later, we need to know the changes occurred in both transactions.  For this reason, `ExclusiveLock` is held on an IMMV immediately after a base table is modified in `READ COMMITTED` mode to make sure that the IMMV is updated in the latter transaction after the former transaction is committed.  In `REPEATABLE READ` or `SERIALIZABLE` mode, an error is raised immediately if lock acquisition fails because any changes which occurred in other transactions are not be visible in these modes and IMMV cannot be updated correctly in such situations. However, as an exception if the IMMV has only one base table, the lock held on the IMMV is `RowExclusiveLock`.

### Row Level Security

If some base tables have row level security policy, rows that are not visible to the materialized view's owner are excluded from the result.  In addition, such rows are excluded as well when views are incrementally maintained.  However, if a new policy is defined or policies are changed after the materialized view was created, the new policy will not be applied to the view contents.  To apply the new policy, you need to recreate IMMV.

## Authors
IVM Development Group
- https://github.com/yugo-n
- https://github.com/thoshiai

## License
[PostgreSQL License](https://github.com/sraoss/pg_ivm/blob/main/LICENSE)

## Copyright
- Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
- Portions Copyright (c) 2022, IVM Development Group

