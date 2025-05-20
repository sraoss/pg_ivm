#include "postgres.h"
#include "pg_ivm.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "commands/event_trigger.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#define IMMV_INIT_QUERYHASHSIZE 16

typedef struct HashEntry
{
	Oid			immv_relid;
	char	   *query;
}			HashEntry;

/* Stores the Query nodes of each IMMV view query as they were pre-DDL. */
static HTAB *immv_query_cache;

/* Active snapshot at the time of the pre-DDL trigger. */
static Snapshot active_snapshot;

static char *retrieve_query(Oid immv_relid);
static void hash_query(Oid immv_relid, char *query);
static void initialize_query_cache(void);
static void restore_query_strings_internal(void);
static void save_query_strings_internal(void);

PG_FUNCTION_INFO_V1(save_query_strings);
PG_FUNCTION_INFO_V1(restore_query_strings);

Datum
save_query_strings(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		ereport(ERROR, errmsg("not fired by event trigger manager"));

	trigdata = (EventTriggerData *) fcinfo->context;

	if (!IsA(trigdata->parsetree, RenameStmt))
		PG_RETURN_NULL();

	initialize_query_cache();
	active_snapshot = GetActiveSnapshot();
	save_query_strings_internal();

	PG_RETURN_NULL();
}

Datum
restore_query_strings(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		ereport(ERROR, errmsg("not fired by event trigger manager"));

	trigdata = (EventTriggerData *) fcinfo->context;

	if (!IsA(trigdata->parsetree, RenameStmt))
		PG_RETURN_NULL();

	restore_query_strings_internal();

	PG_RETURN_NULL();
}

static void
initialize_query_cache(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(HashEntry);
	immv_query_cache = hash_create("IMMV query cache", IMMV_INIT_QUERYHASHSIZE,
								   &ctl, HASH_ELEM | HASH_BLOBS);
}

static void
hash_query(Oid immv_relid, char *query)
{
	HashEntry  *entry;
	MemoryContext old_cxt;
	bool		found;

	entry = (HashEntry *) hash_search(immv_query_cache,
									  &immv_relid,
									  HASH_ENTER,
									  &found);
	Assert(!found);

	old_cxt = MemoryContextSwitchTo(TopTransactionContext);
	entry->query = pstrdup(query);
	MemoryContextSwitchTo(old_cxt);
}

static char *
retrieve_query(Oid immv_relid)
{
	HashEntry  *entry;
	MemoryContext old_cxt;
	bool		found;
	char	   *query;

	entry = (HashEntry *) hash_search(immv_query_cache,
									  &immv_relid,
									  HASH_FIND,
									  &found);
	Assert(found);

	old_cxt = MemoryContextSwitchTo(TopTransactionContext);
	query = pstrdup(entry->query);
	pfree(entry->query);
	MemoryContextSwitchTo(old_cxt);

	return query;
}

static void
save_query_strings_internal(void)
{
	HeapTuple	tup;
	Relation	pg_ivm_immv_rel;
	SysScanDesc scan;
	TupleDesc	tupdesc;

	pg_ivm_immv_rel = table_open(PgIvmImmvRelationId(), RowExclusiveLock);
	scan = systable_beginscan(pg_ivm_immv_rel, PgIvmImmvPrimaryKeyIndexId(),
							  true, active_snapshot, 0, NULL);
	tupdesc = RelationGetDescr(pg_ivm_immv_rel);

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Oid			matview_oid;
		ParseState *parse_state;
		Query	   *query;
		bool		isnull;
		char	   *matview_relname;
		char	   *query_string;

		query_string = TextDatumGetCString(heap_getattr(tup,
														Anum_pg_ivm_immv_querystring,
														tupdesc, &isnull));
		Assert(!isnull);

		matview_oid = DatumGetObjectId(heap_getattr(tup, Anum_pg_ivm_immv_immvrelid,
													tupdesc, &isnull));
		Assert(!isnull);

		matview_relname = psprintf("%s.%s",
								   get_namespace_name(get_rel_namespace(matview_oid)),
								   get_rel_name(matview_oid));

		/* Parse the existing IMMV query pre-DDL.  Add it to the list. */
		parse_immv_query(matview_relname, query_string, &query, &parse_state);

		/* Store entry for this IMMV. */
		hash_query(matview_oid, nodeToString(query));
	}

	systable_endscan(scan);
	table_close(pg_ivm_immv_rel, NoLock);
}

static void
restore_query_strings_internal(void)
{
	HeapTuple	tup;
	Relation	pg_ivm_immv_rel;
	SysScanDesc scan;
	TupleDesc	tupdesc;
	int			save_nestlevel;

	pg_ivm_immv_rel = table_open(PgIvmImmvRelationId(), RowExclusiveLock);
	scan = systable_beginscan(pg_ivm_immv_rel, PgIvmImmvPrimaryKeyIndexId(),
							  true, active_snapshot, 0, NULL);
	tupdesc = RelationGetDescr(pg_ivm_immv_rel);

	/*
	 * Restrict search_path so that pg_ivm_get_viewdef_internal returns a
	 * fully-qualified query.
	 */
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	while ((tup = systable_getnext(scan)) != NULL)
	{
		Datum		values[Natts_pg_ivm_immv];
		HeapTuple	newtup;
		Oid			matview_oid;
		Query	   *query;
		Relation	matview_rel;
		bool		isnull;
		bool		nulls[Natts_pg_ivm_immv];
		bool		replaces[Natts_pg_ivm_immv];
		char	   *new_query_string;
		char	   *serialized_query;

		matview_oid = DatumGetObjectId(heap_getattr(tup, Anum_pg_ivm_immv_immvrelid,
													tupdesc, &isnull));
		Assert(!isnull);

		serialized_query = retrieve_query(matview_oid);
		query = stringToNode(serialized_query);
		query = (Query *) ((CreateTableAsStmt *) query->utilityStmt)->into->viewQuery;

		matview_rel = table_open(matview_oid, AccessShareLock);
		new_query_string = pg_ivm_get_viewdef_internal(query, matview_rel, true);
		table_close(matview_rel, NoLock);

		memset(values, 0, sizeof(values));
		values[Anum_pg_ivm_immv_querystring - 1] = CStringGetTextDatum(new_query_string);
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(nulls));
		replaces[Anum_pg_ivm_immv_querystring - 1] = true;

		newtup = heap_modify_tuple(tup, tupdesc, values, nulls, replaces);

		CatalogTupleUpdate(pg_ivm_immv_rel, &newtup->t_self, newtup);
		heap_freetuple(newtup);
	}

	systable_endscan(scan);
	table_close(pg_ivm_immv_rel, NoLock);
	hash_destroy(immv_query_cache);

	/* Roll back the search_path change. */
	AtEOXact_GUC(false, save_nestlevel);
}
