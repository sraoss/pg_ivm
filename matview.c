/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  Incremental view maintenance extension
 *    Routines for incremental maintenance of IMMVs
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_depend.h"
#include "catalog/heap.h"
#include "catalog/pg_trigger.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

#include "pg_ivm.h"


#define MV_INIT_QUERYHASHSIZE	16

/* MV query type codes */
#define MV_PLAN_RECALC			1
#define MV_PLAN_SET_VALUE		2

/*
 * MI_QueryKey
 *
 * The key identifying a prepared SPI plan in our query hashtable
 */
typedef struct MV_QueryKey
{
	Oid			matview_id;	/* OID of materialized view */
	int32		query_type;	/* query type ID, see MV_PLAN_XXX above */
} MV_QueryKey;

/*
 * MV_QueryHashEntry
 *
 * Hash entry for cached plans used to maintain materialized views.
 */
typedef struct MV_QueryHashEntry
{
	MV_QueryKey key;
	SPIPlanPtr	plan;
	OverrideSearchPath *search_path;	/* search_path used for parsing
										 * and planning */

} MV_QueryHashEntry;

/*
 * MV_TriggerHashEntry
 *
 * Hash entry for base tables on which IVM trigger is invoked
 */
typedef struct MV_TriggerHashEntry
{
	Oid	matview_id;			/* OID of the materialized view */
	int	before_trig_count;	/* count of before triggers invoked */
	int	after_trig_count;	/* count of after triggers invoked */

	Snapshot	snapshot;	/* Snapshot just before table change */

	List   *tables;		/* List of MV_TriggerTable */
	bool	has_old;	/* tuples are deleted from any table? */
	bool	has_new;	/* tuples are inserted into any table? */
} MV_TriggerHashEntry;

/*
 * MV_TriggerTable
 *
 * IVM related data for tables on which the trigger is invoked.
 */
typedef struct MV_TriggerTable
{
	Oid		table_id;			/* OID of the modified table */
	List   *old_tuplestores;	/* tuplestores for deleted tuples */
	List   *new_tuplestores;	/* tuplestores for inserted tuples */
	List   *old_rtes;			/* RTEs of ENRs for old_tuplestores*/
	List   *new_rtes;			/* RTEs of ENRs for new_tuplestores */

	List   *rte_paths;			/* List of paths to RTE index of the modified table */
	RangeTblEntry *original_rte;	/* the original RTE saved before rewriting query */

	Relation	rel;			/* relation of the modified table */
	TupleTableSlot *slot;		/* for checking visibility in the pre-state table */
} MV_TriggerTable;

static HTAB *mv_query_cache = NULL;
static HTAB *mv_trigger_info = NULL;

static bool in_delta_calculation = false;

/* kind of IVM operation for the view */
typedef enum
{
	IVM_ADD,
	IVM_SUB
} IvmOp;

/* ENR name for materialized view delta */
#define NEW_DELTA_ENRNAME "new_delta"
#define OLD_DELTA_ENRNAME "old_delta"

static int	immv_maintenance_depth = 0;

static uint64 refresh_immv_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString);

static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static void OpenImmvIncrementalMaintenance(void);
static void CloseImmvIncrementalMaintenance(void);

static Query *rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, List *rte_path, Oid matviewid);
static void register_delta_ENRs(ParseState *pstate, Query *query, List *tables);
static char *make_delta_enr_name(const char *prefix, Oid relid, int count);
static RangeTblEntry *get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid);
static RangeTblEntry *union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix,
		   QueryEnvironment *queryEnv);
static Query *rewrite_query_for_distinct_and_aggregates(Query *query, ParseState *pstate);

static void calc_delta(MV_TriggerTable *table, List *rte_path, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv);
static Query *rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, List *rte_path);
static ListCell *getRteListCell(Query *query, List *rte_path);

static void apply_delta(Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname);
static void append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list);
static void append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list);
static void append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype);
static void append_set_clause_for_minmax(const char *resname, StringInfo buf_old,
							 StringInfo buf_new, StringInfo aggs_list,
							 bool is_min);
static char *get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType);
static char *get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col);
static void apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys);
static void apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				List *minmax_list, List *is_min_list,
				const char *count_colname,
				SPITupleTable **tuptable_recalc, uint64 *num_recalc);
static void apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list);
static void apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, StringInfo aggs_set,
				const char* count_colname);
static char *get_matching_condition_string(List *keys);
static char *get_returning_string(List *minmax_list, List *is_min_list, List *keys);
static char *get_minmax_recalc_condition_string(List *minmax_list, List *is_min_list);
static char *get_select_for_recalc_string(List *keys);
static void recalc_and_set_values(SPITupleTable *tuptable_recalc, int64 num_tuples,
					  List *namelist, List *keys, Relation matviewRel);
static SPIPlanPtr get_plan_for_recalc(Relation matviewRel, List *namelist, List *keys, Oid *keyTypes);
static SPIPlanPtr get_plan_for_set_values(Relation matviewRel, List *namelist, Oid *valTypes);
static void generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop);

static void mv_InitHashTables(void);
static SPIPlanPtr mv_FetchPreparedPlan(MV_QueryKey *key);
static void mv_HashPreparedPlan(MV_QueryKey *key, SPIPlanPtr plan);
static void mv_BuildQueryKey(MV_QueryKey *key, Oid matview_id, int32 query_type);
static void clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort);

/* SQL callable functions */
PG_FUNCTION_INFO_V1(IVM_immediate_before);
PG_FUNCTION_INFO_V1(IVM_immediate_maintenance);
PG_FUNCTION_INFO_V1(ivm_visible_in_prestate);

/*
 * ExecRefreshImmv -- execute a refresh_immv() function
 *
 * This imitates PostgreSQL's ExecRefreshMatView().
 */
ObjectAddress
ExecRefreshImmv(const RangeVar *relation, bool skipData,
				const char *queryString, QueryCompletion *qc)
{
	Oid			matviewOid;
	Relation	matviewRel;
	Query	   *dataQuery = NULL; /* initialized to keep compiler happy */
	Query	   *viewQuery;
	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDNewHeap;
	DestReceiver *dest;
	uint64		processed = 0;
	//bool		concurrent;
	LOCKMODE	lockmode;
	char		relpersistence;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	ObjectAddress address;
	bool oldPopulated;

	Relation pgIvmImmv;
	TupleDesc tupdesc;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tup;
	bool isnull;
	Datum datum;

	/* Determine strength of lock needed. */
	//concurrent = stmt->concurrent;
	//lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock;
	lockmode = AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(relation,
										  lockmode, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = table_open(matviewOid, lockmode);
	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also lock down security-restricted operations and arrange to
	 * make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * Get the entry in pg_ivm_immv. If it doesn't exist, the relation
	 * is not IMMV.
	 */
	pgIvmImmv = table_open(PgIvmImmvRelationId(), RowExclusiveLock);
	tupdesc = RelationGetDescr(pgIvmImmv);
	ScanKeyInit(&key,
			    Anum_pg_ivm_immv_immvrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(matviewRel)));
	scan = systable_beginscan(pgIvmImmv, PgIvmImmvPrimaryKeyIndexId(),
								  true, NULL, 1, &key);
	tup = systable_getnext(scan);
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not an IMMV",
						RelationGetRelationName(matviewRel))));

	datum = heap_getattr(tup, Anum_pg_ivm_immv_ispopulated, tupdesc, &isnull);
	Assert(!isnull);
	oldPopulated = DatumGetBool(datum);

	/* Tentatively mark the IMMV as populated or not (this will roll back
	 * if we fail later).
	 */
	if (skipData != (!oldPopulated))
	{
		Datum values[Natts_pg_ivm_immv];
		bool nulls[Natts_pg_ivm_immv];
		bool replaces[Natts_pg_ivm_immv];
		HeapTuple newtup = NULL;

		memset(values, 0, sizeof(values));
		values[Anum_pg_ivm_immv_ispopulated -1 ] = BoolGetDatum(!skipData);
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));
		replaces[Anum_pg_ivm_immv_ispopulated -1 ] = true;

		newtup = heap_modify_tuple(tup, tupdesc, values, nulls, replaces);

		CatalogTupleUpdate(pgIvmImmv, &newtup->t_self, newtup);
		heap_freetuple(newtup);

		/*
		 * Advance command counter to make the updated pg_ivm_immv row locally
		 * visible.
		 */
		CommandCounterIncrement();
	}

	systable_endscan(scan);
	table_close(pgIvmImmv, NoLock);

	viewQuery = get_immv_query(matviewRel);

	/* For IMMV, we need to rewrite matview query */
	if (!skipData)
		dataQuery = rewriteQueryForIMMV(viewQuery,NIL);

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "refresh an IMMV");

	tableSpace = matviewRel->rd_rel->reltablespace;
	relpersistence = matviewRel->rd_rel->relpersistence;

	/* delete IMMV triggers. */
	if (skipData)
	{
		Relation	tgRel;
		Relation	depRel;
		ObjectAddresses *immv_triggers;

		immv_triggers = new_object_addresses();

		tgRel = table_open(TriggerRelationId, RowExclusiveLock);
		depRel = table_open(DependRelationId, RowExclusiveLock);

		/* search triggers that depends on IMMV. */
		ScanKeyInit(&key,
					Anum_pg_depend_refobjid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(matviewOid));
		scan = systable_beginscan(depRel, DependReferenceIndexId, true,
								  NULL, 1, &key);
		while ((tup = systable_getnext(scan)) != NULL)
		{
			ObjectAddress obj;
			Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

			if (foundDep->classid == TriggerRelationId)
			{
				HeapTuple	tgtup;
				ScanKeyData tgkey[1];
				SysScanDesc tgscan;
				Form_pg_trigger tgform;

				/* Find the trigger name. */
				ScanKeyInit(&tgkey[0],
							Anum_pg_trigger_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(foundDep->objid));

				tgscan = systable_beginscan(tgRel, TriggerOidIndexId, true,
											NULL, 1, tgkey);
				tgtup = systable_getnext(tgscan);
				if (!HeapTupleIsValid(tgtup))
					elog(ERROR, "could not find tuple for immv trigger %u", foundDep->objid);

				tgform = (Form_pg_trigger) GETSTRUCT(tgtup);

				/* If trigger is created by IMMV, delete it. */
				if (strncmp(NameStr(tgform->tgname), "IVM_trigger_", 12) == 0)
				{
					obj.classId = foundDep->classid;
					obj.objectId = foundDep->objid;
					obj.objectSubId = foundDep->refobjsubid;
					add_exact_object_address(&obj, immv_triggers);
				}
				systable_endscan(tgscan);
			}
		}
		systable_endscan(scan);

		performMultipleDeletions(immv_triggers, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

		table_close(depRel, RowExclusiveLock);
		table_close(tgRel, RowExclusiveLock);
		free_object_addresses(immv_triggers);
	}

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 150000)
	OIDNewHeap = make_new_heap(matviewOid, tableSpace,
							   matviewRel->rd_rel->relam, relpersistence, ExclusiveLock);
#else
	OIDNewHeap = make_new_heap(matviewOid, tableSpace,
							   relpersistence, ExclusiveLock);
#endif
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap);

	/* Generate the data, if wanted. */
	if (!skipData)
		processed = refresh_immv_datafill(dest, dataQuery, NULL, NULL, queryString);

	/* Make the matview match the newly generated data. */
	refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

	/*
	 * Inform cumulative stats system about our activity: basically, we
	 * truncated the matview and inserted some new data.  (The concurrent
	 * code path above doesn't need to worry about this because the
	 * inserts and deletes it issues get counted by lower-level code.)
	 */
	pgstat_count_truncate(matviewRel);
	if (!skipData)
		pgstat_count_heap_insert(matviewRel, processed);

	if (!skipData && !oldPopulated)
		CreateIvmTriggersOnBaseTables(viewQuery, matviewOid);

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

	/*
	 * Save the rowcount so that pg_stat_statements can track the total number
	 * of rows processed by REFRESH MATERIALIZED VIEW command. Note that we
	 * still don't display the rowcount in the command completion tag output,
	 * i.e., the display_rowcount flag of CMDTAG_REFRESH_MATERIALIZED_VIEW
	 * command tag is left false in cmdtaglist.h. Otherwise, the change of
	 * completion tag output might break applications using it.
	 */
	if (qc)
		SetQueryCompletion(qc, CMDTAG_REFRESH_MATERIALIZED_VIEW, processed);

	return address;
}


/*
 * refresh_immv_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
static uint64
refresh_immv_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, queryEnv ? queryEnv: NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	processed = queryDesc->estate->es_processed;

	if (resultTupleDesc)
		*resultTupleDesc = CreateTupleDescCopy(queryDesc->tupDesc);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	return processed;
}

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
static void
refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence)
{
	finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
					 RecentXmin, ReadNextMultiXactId(), relpersistence);
}

/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify IMMVs.  We only want to
 * allow that for internal code driven by the IMMV definition,
 * not for arbitrary user-supplied code.
 */
bool
ImmvIncrementalMaintenanceIsEnabled(void)
{
	return immv_maintenance_depth > 0;
}

static void
OpenImmvIncrementalMaintenance(void)
{
	immv_maintenance_depth++;
}

static void
CloseImmvIncrementalMaintenance(void)
{
	immv_maintenance_depth--;
	Assert(immv_maintenance_depth >= 0);
}

/*
 * get_immv_query - get the Query of IMMV.
 */
Query *
get_immv_query(Relation matviewRel)
{
	Relation pgIvmImmv = table_open(PgIvmImmvRelationId(), AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(pgIvmImmv);
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple tup;
	bool isnull;
	Datum datum;
	Query *query;

	ScanKeyInit(&key,
			    Anum_pg_ivm_immv_immvrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(matviewRel)));
	scan = systable_beginscan(pgIvmImmv, PgIvmImmvPrimaryKeyIndexId(),
								  true, NULL, 1, &key);

	tup = systable_getnext(scan);

	if (!HeapTupleIsValid(tup))
	{
		systable_endscan(scan);
		table_close(pgIvmImmv, NoLock);
		return NULL;
	}

	datum = heap_getattr(tup, Anum_pg_ivm_immv_viewdef, tupdesc, &isnull);
	Assert(!isnull);
	query = (Query *) stringToNode(TextDatumGetCString(datum));

	systable_endscan(scan);
	table_close(pgIvmImmv, NoLock);

	return query;
}

static Tuplestorestate *
tuplestore_copy(Tuplestorestate *tuplestore, Relation rel)
{
	Tuplestorestate *res = NULL;
	TupleDesc tupdesc = RelationGetDescr(rel);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	tuplestore_rescan(tuplestore);
	res = tuplestore_begin_heap(false, false, work_mem);
	while (tuplestore_gettupleslot(tuplestore, true, false, slot))
		tuplestore_puttupleslot(res, slot);
	ExecDropSingleTupleTableSlot(slot);

	return res;
}


/* ----------------------------------------------------
 *		Incremental View Maintenance routines
 * ---------------------------------------------------
 */

/*
 * IVM_immediate_before
 *
 * IVM trigger function invoked before base table is modified. If this is
 * invoked firstly in the same statement, we save the transaction id and the
 * command id at that time.
 */
Datum
IVM_immediate_before(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	char	   *ex_lock_text = trigdata->tg_trigger->tgargs[1];
	Oid			matviewOid;
	MV_TriggerHashEntry *entry;
	bool	found;
	bool	ex_lock;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));
	ex_lock = DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(ex_lock_text)));

	/* If the view has more than one tables, we have to use an exclusive lock. */
	if (ex_lock)
	{
		/*
		 * Wait for concurrent transactions which update this materialized view at
		 * READ COMMITED. This is needed to see changes committed in other
		 * transactions. No wait and raise an error at REPEATABLE READ or
		 * SERIALIZABLE to prevent update anomalies of matviews.
		 * XXX: dead-lock is possible here.
		 */
		if (!IsolationUsesXactSnapshot())
			LockRelationOid(matviewOid, ExclusiveLock);
		else if (!ConditionalLockRelationOid(matviewOid, ExclusiveLock))
		{
			/* try to throw error by name; relation could be deleted... */
			char	   *relname = get_rel_name(matviewOid);

			if (!relname)
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("could not obtain lock on materialized view during incremental maintenance")));

			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on materialized view \"%s\" during incremental maintenance",
							relname)));
		}
	}
	else
		LockRelationOid(matviewOid, RowExclusiveLock);

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_trigger_info)
		mv_InitHashTables();

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_ENTER, &found);

	/* On the first BEFORE to update the view, initialize trigger data */
	if (!found)
	{
		/*
		 * Get a snapshot just before the table was modified for checking
		 * tuple visibility in the pre-update state of the table.
		 */
		Snapshot snapshot = GetActiveSnapshot();

		entry->matview_id = matviewOid;
		entry->before_trig_count = 0;
		entry->after_trig_count = 0;
		entry->snapshot = RegisterSnapshot(snapshot);
		entry->tables = NIL;
		entry->has_old = false;
		entry->has_new = false;
	}

	entry->before_trig_count++;


	return PointerGetDatum(NULL);
}

/*
 * IVM_immediate_maintenance
 *
 * IVM trigger function invoked after base table is modified.
 * For each table, tuplestores of transition tables are collected.
 * and after the last modification
 */
Datum
IVM_immediate_maintenance(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	Oid			relid;
	Oid			matviewOid;
	Query	   *query;
	Query	   *rewritten = NULL;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	Relation	matviewRel;
	int old_depth = immv_maintenance_depth;

	Oid			relowner;
	Tuplestorestate *old_tuplestore = NULL;
	Tuplestorestate *new_tuplestore = NULL;
	DestReceiver *dest_new = NULL, *dest_old = NULL;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table;
	bool	found;

	ParseState		 *pstate;
	QueryEnvironment *queryEnv = create_queryEnv();
	MemoryContext	oldcxt;
	ListCell   *lc;
	int			i;

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	rel = trigdata->tg_relation;
	relid = rel->rd_id;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_trigger_info)
		mv_InitHashTables();

	/* get the entry for this materialized view */
	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);
	entry->after_trig_count++;

	/* search the entry for the modified table and create new entry if not found */
	found = false;
	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == relid)
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		table = (MV_TriggerTable *) palloc0(sizeof(MV_TriggerTable));
		table->table_id = relid;
		table->old_tuplestores = NIL;
		table->new_tuplestores = NIL;
		table->old_rtes = NIL;
		table->new_rtes = NIL;
		table->rte_paths = NIL;
		table->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), table_slot_callbacks(rel));
		table->rel = table_open(RelationGetRelid(rel), NoLock);
		entry->tables = lappend(entry->tables, table);

		MemoryContextSwitchTo(oldcxt);
	}

	/* Save the transition tables and make a request to not free immediately */
	if (trigdata->tg_oldtable)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		table->old_tuplestores = lappend(table->old_tuplestores, tuplestore_copy(trigdata->tg_oldtable, rel));
		entry->has_old = true;
		MemoryContextSwitchTo(oldcxt);
	}
	if (trigdata->tg_newtable)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		table->new_tuplestores = lappend(table->new_tuplestores, tuplestore_copy(trigdata->tg_newtable, rel));
		entry->has_new = true;
		MemoryContextSwitchTo(oldcxt);
	}

	/* If this is not the last AFTER trigger call, immediately exit. */
	Assert (entry->before_trig_count >= entry->after_trig_count);
	if (entry->before_trig_count != entry->after_trig_count)
		return PointerGetDatum(NULL);

	/*
	 * If this is the last AFTER trigger call, continue and update the view.
	 */

	/*
	 * Advance command counter to make the updated base table row locally
	 * visible.
	 */
	CommandCounterIncrement();

	matviewRel = table_open(matviewOid, NoLock);

	/* Make sure IMMV is a table. */
	Assert(matviewRel->rd_rel->relkind == RELKIND_RELATION);

	/*
	 * Get and push the latast snapshot to see any changes which is committed
	 * during waiting in other transactions at READ COMMITTED level.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "refresh an IMMV incrementally");

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * We will switch modes when we are about to execute user code.
	 */
	relowner = matviewRel->rd_rel->relowner;
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/* get view query*/
	query = get_immv_query(matviewRel);

	/*
	 * When a base table is truncated, the view content will be empty if the
	 * view definition query does not contain an aggregate without a GROUP clause.
	 * Therefore, such views can be truncated.
	 *
	 * Aggregate views without a GROUP clause always have one row. Therefore,
	 * if a base table is truncated, the view will not be empty and will contain
	 * a row with NULL value (or 0 for count()). So, in this case, we refresh the
	 * view instead of truncating it.
	 */
	if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
	{
		if (!(query->hasAggs && query->groupClause == NIL))
		{
			OpenImmvIncrementalMaintenance();
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
			ExecuteTruncateGuts(list_make1(matviewRel), list_make1_oid(matviewOid),
								NIL, DROP_RESTRICT, false, false);
#else
			ExecuteTruncateGuts(list_make1(matviewRel), list_make1_oid(matviewOid),
								NIL, DROP_RESTRICT, false);
#endif
			CloseImmvIncrementalMaintenance();
		}
		else
		{
			Oid			OIDNewHeap;
			DestReceiver *dest;
			uint64		processed = 0;
			Query	   *dataQuery = rewriteQueryForIMMV(query, NIL);
			char		relpersistence = matviewRel->rd_rel->relpersistence;

			/*
			 * Create the transient table that will receive the regenerated data. Lock
			 * it against access by any other process until commit (by which time it
			 * will be gone).
			 */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 150000)
			OIDNewHeap = make_new_heap(matviewOid, matviewRel->rd_rel->reltablespace,
									   matviewRel->rd_rel->relam, relpersistence,  ExclusiveLock);
#else
			OIDNewHeap = make_new_heap(matviewOid, matviewRel->rd_rel->reltablespace,
									   relpersistence,  ExclusiveLock);
#endif
			LockRelationOid(OIDNewHeap, AccessExclusiveLock);
			dest = CreateTransientRelDestReceiver(OIDNewHeap);

			/* Generate the data */
			processed = refresh_immv_datafill(dest, dataQuery, NULL, NULL, "");
			refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

			/* Inform cumulative stats system about our activity */
			pgstat_count_truncate(matviewRel);
			pgstat_count_heap_insert(matviewRel, processed);
		}

		/* Clean up hash entry and delete tuplestores */
		clean_up_IVM_hash_entry(entry, false);

		/* Pop the original snapshot. */
		PopActiveSnapshot();

		table_close(matviewRel, NoLock);

		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		return PointerGetDatum(NULL);
	}

	/*
	 * rewrite query for calculating deltas
	 */

	rewritten = copyObject(query);

	/* Replace resnames in a target list with materialized view's attnames */
	i = 0;
	foreach (lc, rewritten->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		tle->resname = pstrdup(resname);
		i++;
	}

	/* Rewrite for the EXISTS clause */
	if (rewritten->hasSubLinks)
		rewrite_query_for_exists_subquery(rewritten);

	/* Set all tables in the query to pre-update state */
	rewritten = rewrite_query_for_preupdate_state(rewritten, entry->tables,
												  pstate, NIL, matviewOid);
	/* Rewrite for DISTINCT clause and aggregates functions */
	rewritten = rewrite_query_for_distinct_and_aggregates(rewritten, pstate);

	/* Create tuplestores to store view deltas */
	if (entry->has_old)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		old_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_old = CreateDestReceiver(DestTuplestore);
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		SetTuplestoreDestReceiverParams(dest_old,
									old_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);
#else
		SetTuplestoreDestReceiverParams(dest_old,
									old_tuplestore,
									TopTransactionContext,
									false);
#endif
		MemoryContextSwitchTo(oldcxt);
	}
	if (entry->has_new)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		new_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_new = CreateDestReceiver(DestTuplestore);
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		SetTuplestoreDestReceiverParams(dest_new,
									new_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);
#else
		SetTuplestoreDestReceiverParams(dest_new,
									new_tuplestore,
									TopTransactionContext,
									false);
#endif
		MemoryContextSwitchTo(oldcxt);
	}

	/* for all modified tables */
	foreach(lc, entry->tables)
	{
		ListCell *lc2;

		table = (MV_TriggerTable *) lfirst(lc);

		/* loop for self-join */
		foreach(lc2, table->rte_paths)
		{
			List	*rte_path = lfirst(lc2);
			Query *querytree = rewritten;
			RangeTblEntry  *rte;
			TupleDesc		tupdesc_old;
			TupleDesc		tupdesc_new;
			bool	use_count = false;
			char   *count_colname = NULL;

			/* check if the modified table is in EXISTS clause. */
			for (i = 0; i < list_length(rte_path); i++)
			{
				int index =  lfirst_int(list_nth_cell(rte_path, i));
				rte = (RangeTblEntry *) lfirst(list_nth_cell(querytree->rtable, index - 1));

				if (rte != NULL && rte->rtekind == RTE_SUBQUERY)
				{
					querytree = rte->subquery;
					if (rte->lateral)
					{
						int attnum;
						count_colname = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
						if (count_colname)
						{
							use_count = true;
						}
					}
				}
			}

			if (count_colname == NULL && (query->hasAggs || query->distinctClause))
			{
				count_colname = pstrdup("__ivm_count__");
				use_count = true;
			}

			/* calculate delta tables */
			calc_delta(table, rte_path, rewritten, dest_old, dest_new,
					   &tupdesc_old, &tupdesc_new, queryEnv);

			/* Set the table in the query to post-update state */
			rewritten = rewrite_query_for_postupdate_state(rewritten, table, rte_path);

			PG_TRY();
			{
				/* apply the delta tables to the materialized view */
				apply_delta(matviewOid, old_tuplestore, new_tuplestore,
							tupdesc_old, tupdesc_new, query, use_count,
							count_colname);
			}
			PG_CATCH();
			{
				immv_maintenance_depth = old_depth;
				PG_RE_THROW();
			}
			PG_END_TRY();

			/* clear view delta tuplestores */
			if (old_tuplestore)
				tuplestore_clear(old_tuplestore);
			if (new_tuplestore)
				tuplestore_clear(new_tuplestore);
		}
	}

	/* Clean up hash entry and delete tuplestores */
	clean_up_IVM_hash_entry(entry, false);
	if (old_tuplestore)
	{
		dest_old->rDestroy(dest_old);
		tuplestore_end(old_tuplestore);
	}
	if (new_tuplestore)
	{
		dest_new->rDestroy(dest_new);
		tuplestore_end(new_tuplestore);
	}

	/* Pop the original snapshot. */
	PopActiveSnapshot();

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return PointerGetDatum(NULL);
}

/*
 * rewrite_query_for_preupdate_state
 *
 * Rewrite the query so that base tables' RTEs will represent "pre-update"
 * state of tables. This is necessary to calculate view delta after multiple
 * tables are modified.
 */
static Query*
rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, List *rte_path, Oid matviewid)
{
	ListCell *lc;
	int num_rte = list_length(query->rtable);
	int i;

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	/* register delta ENRs only one at first call */
	if (rte_path == NIL)
		register_delta_ENRs(pstate, query, tables);

	/* XXX: Is necessary? Is this right timing? */
	AcquireRewriteLocks(query, true, false);

	/* convert CTEs to subqueries */
	foreach (lc, query->cteList)
	{
		PlannerInfo root;
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (cte->cterefcount == 0)
			continue;

		root.parse = query;
		inline_cte(&root, cte);
	}
	query->cteList = NIL;

	i = 1;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);

		/* if rte contains subquery, search recursively */
		if (r->rtekind == RTE_SUBQUERY)
			rewrite_query_for_preupdate_state(r->subquery, tables, pstate, lappend_int(list_copy(rte_path), i), matviewid);
		else
		{
			ListCell *lc2;
			foreach(lc2, tables)
			{
				MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc2);
				/*
				 * if the modified table is found then replace the original RTE with
				 * "pre-state" RTE and append its index to the list.
				 */
				if (r->relid == table->table_id)
				{
					List *securityQuals;
					List *withCheckOptions;
					bool  hasRowSecurity;
					bool  hasSubLinks;

					RangeTblEntry *rte_pre = get_prestate_rte(r, table, pstate->p_queryEnv, matviewid);

					/*
					 * Set a row security poslicies of the modified table to the subquery RTE which
					 * represents the pre-update state of the table.
					 */
					get_row_security_policies(query, table->original_rte, i,
											  &securityQuals, &withCheckOptions,
											  &hasRowSecurity, &hasSubLinks);
					if (hasRowSecurity)
					{
						query->hasRowSecurity = true;
						rte_pre->security_barrier = true;
					}
					if (hasSubLinks)
						query->hasSubLinks = true;

					rte_pre->securityQuals = securityQuals;
					lfirst(lc) = rte_pre;

					table->rte_paths = lappend(table->rte_paths, lappend_int(list_copy(rte_path), i));
					break;
				}
			}
		}

		/* finish the loop if we processed all RTE included in the original query */
		if (i++ >= num_rte)
			break;
	}

	return query;
}

/*
 * register_delta_ENRs
 *
 * For all modified tables, make ENRs for their transition tables
 * and register them to the queryEnv. ENR's RTEs are also appended
 * into the list in query tree.
 */
static void
register_delta_ENRs(ParseState *pstate, Query *query, List *tables)
{
	QueryEnvironment *queryEnv = pstate->p_queryEnv;
	ListCell *lc;
	RangeTblEntry	*rte;

	foreach(lc, tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;
		int count;

		count = 0;
		foreach(lc2, table->old_tuplestores)
		{
			Tuplestorestate *oldtable = (Tuplestorestate *) lfirst(lc2);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;

			enr->md.name = make_delta_enr_name("old", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(oldtable);
			enr->reldata = oldtable;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;

			query->rtable = lappend(query->rtable, rte);
			table->old_rtes = lappend(table->old_rtes, rte);

			count++;
		}

		count = 0;
		foreach(lc2, table->new_tuplestores)
		{
			Tuplestorestate *newtable = (Tuplestorestate *) lfirst(lc2);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;

			enr->md.name = make_delta_enr_name("new", table->table_id, count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = NULL;
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(newtable);
			enr->reldata = newtable;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;

			query->rtable = lappend(query->rtable, rte);
			table->new_rtes = lappend(table->new_rtes, rte);

			count++;
		}
	}
}

#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))

/*
 * ivm_visible_in_prestate
 *
 * Check visibility of a tuple specified by the tableoid and item pointer
 * using the snapshot taken just before the table was modified.
 */
Datum
ivm_visible_in_prestate(PG_FUNCTION_ARGS)
{
	Oid			tableoid = PG_GETARG_OID(0);
	ItemPointer itemPtr = PG_GETARG_ITEMPOINTER(1);
	Oid			matviewOid = PG_GETARG_OID(2);
	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table = NULL;
	ListCell   *lc;
	bool	found;
	bool	result;

	if (!in_delta_calculation)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ivm_visible_in_prestate can be called only in delta calculation")));

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);

	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == tableoid)
			break;
	}

	Assert (table != NULL);

	result = table_tuple_fetch_row_version(table->rel, itemPtr, entry->snapshot, table->slot);

	PG_RETURN_BOOL(result);
}

/*
 * get_prestate_rte
 *
 * Rewrite RTE of the modified table to a subquery which represents
 * "pre-state" table. The original RTE is saved in table->rte_original.
 */
static RangeTblEntry*
get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid)
{
	StringInfoData str;
	RawStmt *raw;
	Query *subquery;
	Relation rel;
	ParseState *pstate;
	char *relname;
	int i;

	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * We can use NoLock here since AcquireRewriteLocks should
	 * have locked the relation already.
	 */
	rel = table_open(table->table_id, NoLock);
	relname = quote_qualified_identifier(
					get_namespace_name(RelationGetNamespace(rel)),
									   RelationGetRelationName(rel));
	table_close(rel, NoLock);

	/*
	 * Filtering inserted row using the snapshot taken before the table
	 * is modified. ctid is required for maintaining outer join views.
	 */
	initStringInfo(&str);
	appendStringInfo(&str,
		"SELECT t.* FROM %s t"
		" WHERE pg_catalog.ivm_visible_in_prestate(t.tableoid, t.ctid ,%d::pg_catalog.oid)",
			relname, matviewid);

	/*
	 * Append deleted rows contained in old transition tables.
	 */
	for (i = 0; i < list_length(table->old_tuplestores); i++)
	{
		appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT * FROM %s",
			make_delta_enr_name("old", table->table_id, i));
	}

	/* Get a subquery representing pre-state of the table */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
#else
	raw = (RawStmt*)linitial(raw_parser(str.data));
#endif
	subquery = transformStmt(pstate, raw->stmt);

	/* save the original RTE */
	table->original_rte = copyObject(rte);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->security_barrier = false;

	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
	rte->perminfoindex = 0;         /* no permission checking for this RTE */
#else
	rte->requiredPerms = 0;		/* no permission check on subquery itself */
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;
#endif

	return rte;
}

/*
 * make_delta_enr_name
 *
 * Make a name for ENR of a transition table from the base table's oid.
 * prefix will be "new" or "old" depending on its transition table kind..
 */
static char*
make_delta_enr_name(const char *prefix, Oid relid, int count)
{
	char buf[NAMEDATALEN];
	char *name;

	snprintf(buf, NAMEDATALEN, "__ivm_%s_%u_%u", prefix, relid, count);
	name = pstrdup(buf);

	return name;
}

/*
 * union_ENRs
 *
 * Replace RTE of the modified table with a single table delta that combine its
 * all transition tables.
 */
static RangeTblEntry*
union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix,
		   QueryEnvironment *queryEnv)
{
	StringInfoData str;
	ParseState	*pstate;
	RawStmt *raw;
	Query *sub;
	int	i;

	/* the previous RTE must be a subquery which represents "pre-state" table */
	Assert(rte->rtekind == RTE_SUBQUERY);

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	initStringInfo(&str);

	for (i = 0; i < list_length(enr_rtes); i++)
	{
		if (i > 0)
			appendStringInfo(&str, " UNION ALL ");

		appendStringInfo(&str,
			" SELECT * FROM %s",
			make_delta_enr_name(prefix, relid, i));
	}

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
#else
	raw = (RawStmt*)linitial(raw_parser(str.data));
#endif
	sub = transformStmt(pstate, raw->stmt);

	/*
	 * Update the subquery so that it represent the combined transition
	 * table.  Note that we leave the security_barrier and securityQuals
	 * fields so that the subquery relation can be protected by the RLS
	 * policy as same as the modified table.
	 */
	rte->subquery = sub;

	return rte;
}

/*
 * rewrite_query_for_distinct_and_aggregates
 *
 * Rewrite query for counting DISTINCT clause and aggregate functions.
 */
static Query *
rewrite_query_for_distinct_and_aggregates(Query *query, ParseState *pstate)
{
	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;
	int varno = 0;
	ListCell *tbl_lc;

	/* For aggregate views */
	if (query->hasAggs)
	{
		ListCell *lc;
		List *aggs = NIL;
		AttrNumber next_resno = list_length(query->targetList) + 1;

		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (IsA(tle->expr, Aggref))
				makeIvmAggColumn(pstate, (Aggref *)tle->expr, tle->resname, &next_resno, &aggs);
		}
		query->targetList = list_concat(query->targetList, aggs);
	}

	/* Add count(*) used for EXISTS clause */
	foreach(tbl_lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(tbl_lc);
		varno++;
		if (rte->subquery)
		{
			char *columnName;
			int attnum;

			/* search ivm_exists_count_X__ column in RangeTblEntry */
			columnName = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
			if (columnName == NULL)
				continue;

			node = (Node *)makeVar(varno ,attnum,
					INT8OID, -1, InvalidOid, 0);

			if (node == NULL)
				continue;
			tle_count = makeTargetEntry((Expr *) node,
										list_length(query->targetList) + 1,
										pstrdup(columnName),
										false);
			query->targetList = lappend(query->targetList, tle_count);
		}
	}

	/* Add count(*) for counting distinct tuples in views */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
#else
	fn = makeFuncCall(SystemFuncName("count"), NIL, -1);
#endif
	fn->agg_star = true;
	if (!query->groupClause && !query->hasAggs)
		query->groupClause = transformDistinctClause(NULL, &query->targetList, query->sortClause, false);

	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

	tle_count = makeTargetEntry((Expr *) node,
								list_length(query->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
	query->targetList = lappend(query->targetList, tle_count);
	query->hasAggs = true;

	return query;
}

static Query *
rewrite_exists_subquery_walker(Query *query, Node *node, int *count)
{
	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				FromExpr *fromexpr;

				/* get subquery in WHERE clause */
				fromexpr = (FromExpr *) query->jointree;
				if (fromexpr->quals != NULL)
				{
					query = rewrite_exists_subquery_walker(query, fromexpr->quals, count);
					/* drop subquery in WHERE clause */
					if (IsA(fromexpr->quals, SubLink))
						fromexpr->quals = NULL;
				}
				break;
			}
		case T_BoolExpr:
			{
				BoolExprType type;

				type = ((BoolExpr *) node)->boolop;
				switch (type)
				{
					ListCell *lc;
					case AND_EXPR:
						foreach(lc, ((BoolExpr *)node)->args)
						{
							/* If simple EXISTS subquery is used, rewrite LATERAL subquery */
							Node *opnode = (Node *)lfirst(lc);
							query = rewrite_exists_subquery_walker(query, opnode, count);
							/*
							 * overwrite SubLink node to true condition if it is contained in AND_EXPR.
							 * EXISTS clause have already overwritten to LATERAL, so original EXISTS clause
							 * is not necessory.
							 */
							if (IsA(opnode, SubLink))
								lfirst(lc) = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(true), false, true);
						}
						break;
					case OR_EXPR:
					case NOT_EXPR:
						if (checkExprHasSubLink(node))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("OR or NOT conditions and EXISTS condition are not used together")));
						break;
				}
				break;
			}
		case T_SubLink:
			{
				char aliasName[NAMEDATALEN];
				char columnName[NAMEDATALEN];
				Query *subselect;
				ParseState *pstate;
				RangeTblEntry *rte;
				RangeTblRef *rtr;
				Alias *alias;
				Oid opId;
				ParseNamespaceItem *nsitem;

				TargetEntry *tle_count;
				FuncCall *fn;
				Node *fn_node;
				Expr *opexpr;

				SubLink *sublink = (SubLink *)node;
				subselect = (Query *)sublink->subselect;

				pstate = make_parsestate(NULL);
				pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

				/*
				 * convert EXISTS subquery into LATERAL subquery in FROM clause.
				 */

				snprintf(aliasName, sizeof(aliasName), "__ivm_exists_subquery_%d__", *count);
				snprintf(columnName, sizeof(columnName), "__ivm_exists_count_%d__", *count);

				/* add COUNT(*) for counting rows that meet exists condition */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
				fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
#else
				fn = makeFuncCall(SystemFuncName("count"), NIL, -1);
#endif
				fn->agg_star = true;
				fn_node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);
				tle_count = makeTargetEntry((Expr *) fn_node,
											list_length(subselect->targetList) + 1,
											columnName,
											false);
				/* add __ivm_exists_count__ column */
				subselect->targetList = list_concat(subselect->targetList, list_make1(tle_count));
				subselect->hasAggs = true;

				/* add a sub-query whth LATERAL into from clause */
				alias = makeAlias(aliasName, NIL);
				nsitem = addRangeTableEntryForSubquery(pstate, subselect, alias, true, true);
				rte = nsitem->p_rte;
				query->rtable = lappend(query->rtable, rte);

				/* assume the new RTE is at the end */
				rtr = makeNode(RangeTblRef);
				rtr->rtindex = list_length(query->rtable);
				((FromExpr *)query->jointree)->fromlist = lappend(((FromExpr *)query->jointree)->fromlist, rtr);

				/*
				 * EXISTS condition is converted to HAVING count(*) > 0.
				 * We use make_opcllause() to get int84gt( '>' operator). We might be able to use make_op().
				 */
				opId = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString(">")), INT8OID, INT4OID);
				opexpr = make_opclause(opId, BOOLOID, false,
								(Expr *)fn_node,
								(Expr *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), false, true),
								InvalidOid, InvalidOid);
				fix_opfuncids((Node *) opexpr);
				query->hasSubLinks = false;

				subselect->havingQual = (Node *)opexpr;
				(*count)++;
				break;
			}
		default:
			break;
	}
	return query;
}

/*
 * rewrite_query_for_exists_subquery
 *
 * Rewrite EXISTS sublink in WHERE to LATERAL subquery
 * For example, rewrite
 *   SELECT t1.* FROM t1
 *   WHERE EXISTS(SELECT 1 FROM t2 WHERE t1.key = t2.key)
 * to
 *   SELECT t1.*, ex.__ivm_exists_count_0__
 *   FROM t1, LATERAL(
 *     SELECT 1, COUNT(*) AS __ivm_exists_count_0__
 *     FROM t2
 *     WHERE t1.key = t2.key
 *     HAVING __ivm_exists_count_0__ > 0) AS ex
 */
Query *
rewrite_query_for_exists_subquery(Query *query)
{
	int count = 0;
	if (query->hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("this query is not allowed on incrementally maintainable materialized view"),
				 errhint("aggregate function and EXISTS condition are not supported at the same time")));

	return rewrite_exists_subquery_walker(query, (Node *)query, &count);
}

/*
 * calc_delta
 *
 * Calculate view deltas generated under the modification of a table specified
 * by the RTE index.
 */
static void
calc_delta(MV_TriggerTable *table, List *rte_path, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv)
{
	ListCell *lc = getRteListCell(query, rte_path);
	RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

	in_delta_calculation = true;

	/* Generate old delta */
	if (list_length(table->old_rtes) > 0)
	{
		/* Replace the modified table with the old delta table and calculate the old view delta. */
		lfirst(lc) = union_ENRs(rte, table->table_id, table->old_rtes, "old", queryEnv);
		refresh_immv_datafill(dest_old, query, queryEnv, tupdesc_old, "");
	}

	/* Generate new delta */
	if (list_length(table->new_rtes) > 0)
	{
		/* Replace the modified table with the new delta table and calculate the new view delta*/
		lfirst(lc) = union_ENRs(rte, table->table_id, table->new_rtes, "new", queryEnv);
		refresh_immv_datafill(dest_new, query, queryEnv, tupdesc_new, "");
	}

	in_delta_calculation = false;
}

/*
 * rewrite_query_for_postupdate_state
 *
 * Rewrite the query so that the specified base table's RTEs will represent
 * "post-update" state of tables. This is called after the view delta
 * calculation due to changes on this table finishes.
 */
static Query*
rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, List *rte_path)
{
	ListCell *lc = getRteListCell(query, rte_path);

	/* Retore the original RTE */
	lfirst(lc) = table->original_rte;

	return query;
}

/*
 * getRteListCell
 *
 * Get ListCell which contains RTE specified by the given path.
 */
static ListCell*
getRteListCell(Query *query, List *rte_path)
{
	ListCell *lc;
	ListCell *rte_lc = NULL;

	Assert(list_length(rte_path) > 0);

	foreach (lc, rte_path)
	{
		int index = lfirst_int(lc);
		RangeTblEntry	*rte;

		rte_lc = list_nth_cell(query->rtable, index - 1);
		rte = (RangeTblEntry *) lfirst(rte_lc);
		if (rte != NULL && rte->rtekind == RTE_SUBQUERY)
			query = rte->subquery;
	}
	return rte_lc;
}

#define IVM_colname(type, col) makeObjectName("__ivm_" type, col, "_")

/*
 * apply_delta
 *
 * Apply deltas to the materialized view. In outer join cases, this requires
 * the view maintenance graph.
 */
static void
apply_delta(Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname)
{
	StringInfoData querybuf;
	StringInfoData target_list_buf;
	StringInfo	aggs_list_buf = NULL;
	StringInfo	aggs_set_old = NULL;
	StringInfo	aggs_set_new = NULL;
	Relation	matviewRel;
	char	   *matviewname;
	ListCell	*lc;
	int			i;
	List	   *keys = NIL;
	List	   *minmax_list = NIL;
	List	   *is_min_list = NIL;


	/*
	 * get names of the materialized view and delta tables
	 */

	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	/*
	 * Build parts of the maintenance queries
	 */

	initStringInfo(&querybuf);
	initStringInfo(&target_list_buf);

	if (query->hasAggs)
	{
		if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
			aggs_set_old = makeStringInfo();
		if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
			aggs_set_new = makeStringInfo();
		aggs_list_buf = makeStringInfo();
	}

	/* build string of target list */
	for (i = 0; i < matviewRel->rd_att->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char   *resname = NameStr(attr->attname);

		if (i != 0)
			appendStringInfo(&target_list_buf, ", ");
		appendStringInfo(&target_list_buf, "%s", quote_qualified_identifier(NULL, resname));
	}

	i = 0;
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		i++;

		if (tle->resjunk)
			continue;

		/*
		 * For views without aggregates, all attributes are used as keys to identify a
		 * tuple in a view.
		 */
		if (!query->hasAggs)
			keys = lappend(keys, attr);

		/* For views with aggregates, we need to build SET clause for updating aggregate
		 * values. */
		if (query->hasAggs && IsA(tle->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) tle->expr;
			const char *aggname = get_func_name(aggref->aggfnoid);

			/*
			 * We can use function names here because it is already checked if these
			 * can be used in IMMV by its OID at the definition time.
			 */

			/* count */
			if (!strcmp(aggname, "count"))
				append_set_clause_for_count(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* sum */
			else if (!strcmp(aggname, "sum"))
				append_set_clause_for_sum(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* avg */
			else if (!strcmp(aggname, "avg"))
				append_set_clause_for_avg(resname, aggs_set_old, aggs_set_new, aggs_list_buf,
										  format_type_be_qualified(aggref->aggtype));

			/* min/max */
			else if (!strcmp(aggname, "min") || !strcmp(aggname, "max"))
			{
				bool	is_min = (!strcmp(aggname, "min"));

				append_set_clause_for_minmax(resname, aggs_set_old, aggs_set_new, aggs_list_buf, is_min);

				/* make a resname list of min and max aggregates */
				minmax_list = lappend(minmax_list, resname);
				is_min_list = lappend_int(is_min_list, is_min);
			}
			else
				elog(ERROR, "unsupported aggregate function: %s", aggname);
		}
	}

	/* If we have GROUP BY clause, we use its entries as keys. */
	if (query->hasAggs && query->groupClause)
	{
		foreach (lc, query->groupClause)
		{
			SortGroupClause *sgcl = (SortGroupClause *) lfirst(lc);
			TargetEntry		*tle = get_sortgroupclause_tle(sgcl, query->targetList);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);

			keys = lappend(keys, attr);
		}
	}

	/* Start maintaining the materialized view. */
	OpenImmvIncrementalMaintenance();

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* For tuple deletion */
	if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		SPITupleTable  *tuptable_recalc = NULL;
		uint64			num_recalc;
		int				rc;

		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(OLD_DELTA_ENRNAME);
		enr->md.reliddesc = InvalidOid;
		enr->md.tupdesc = tupdesc_old;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(old_tuplestores);
		enr->reldata = old_tuplestores;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		if (use_count)
			/* apply old delta and get rows to be recalculated */
			apply_old_delta_with_count(matviewname, OLD_DELTA_ENRNAME,
									   keys, aggs_list_buf, aggs_set_old,
									   minmax_list, is_min_list,
									   count_colname, &tuptable_recalc, &num_recalc);
		else
			apply_old_delta(matviewname, OLD_DELTA_ENRNAME, keys);

		/*
		 * If we have min or max, we might have to recalculate aggregate values from base tables
		 * on some tuples. TIDs and keys such tuples are returned as a result of the above query.
		 */
		if (minmax_list && tuptable_recalc)
			recalc_and_set_values(tuptable_recalc, num_recalc, minmax_list, keys, matviewRel);

	}
	/* For tuple insertion */
	if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
	{
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		int rc;

		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(NEW_DELTA_ENRNAME);
		enr->md.reliddesc = InvalidOid;
		enr->md.tupdesc = tupdesc_new;;
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(new_tuplestores);
		enr->reldata = new_tuplestores;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		/* apply new delta */
		if (use_count)
			apply_new_delta_with_count(matviewname, NEW_DELTA_ENRNAME,
								keys, aggs_set_new, &target_list_buf, count_colname);
		else
			apply_new_delta(matviewname, NEW_DELTA_ENRNAME, &target_list_buf);
	}

	/* We're done maintaining the materialized view. */
	CloseImmvIncrementalMaintenance();

	table_close(matviewRel, NoLock);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * append_set_clause_for_count
 *
 * Append SET clause string for count aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list)
{
	/* For tuple deletion */
	if (buf_old)
	{
		/* resname = mv.resname - t.resname */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", NULL, NULL));
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* resname = mv.resname + diff.resname */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", NULL, NULL));
	}

	appendStringInfo(aggs_list, ", %s",
		quote_qualified_identifier("diff", resname)
	);
}

/*
 * append_set_clause_for_sum
 *
 * Append SET clause string for sum aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list)
{
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * append_set_clause_for_avg
 *
 * Append SET clause string for avg aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype)
{
	char *sum_col = IVM_colname("sum", resname);
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* avg = (mv.sum - t.sum)::aggtype / (mv.count - t.count) */
		appendStringInfo(buf_old,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, aggtype),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);

	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* avg = (mv.sum + diff.sum)::aggtype / (mv.count + diff.count) */
		appendStringInfo(buf_new,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, aggtype),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("sum", resname)),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * append_set_clause_for_minmax
 *
 * Append SET clause string for min or max aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 * is_min is true if this is min, false if not.
 */
static void
append_set_clause_for_minmax(const char *resname, StringInfo buf_old,
							 StringInfo buf_new, StringInfo aggs_list,
							 bool is_min)
{
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/*
		 * If the new value doesn't became NULL then use the value remaining
		 * in the view although this will be recomputated afterwords.
		 */
		appendStringInfo(buf_old,
			", %s = CASE WHEN %s THEN NULL ELSE %s END",
			quote_qualified_identifier(NULL, resname),
			get_null_condition_string(IVM_SUB, "mv", "t", count_col),
			quote_qualified_identifier("mv", resname)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/*
		 * min = LEAST(mv.min, diff.min)
		 * max = GREATEST(mv.max, diff.max)
		 */
		appendStringInfo(buf_new,
			", %s = CASE WHEN %s THEN NULL ELSE %s(%s,%s) END",
			quote_qualified_identifier(NULL, resname),
			get_null_condition_string(IVM_ADD, "mv", "diff", count_col),

			is_min ? "LEAST" : "GREATEST",
			quote_qualified_identifier("mv", resname),
			quote_qualified_identifier("diff", resname)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * get_operation_string
 *
 * Build a string to calculate the new aggregate values.
 */
static char *
get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType)
{
	StringInfoData buf;
	StringInfoData castString;
	char   *col1 = quote_qualified_identifier(arg1, col);
	char   *col2 = quote_qualified_identifier(arg2, col);
	char	op_char = (op == IVM_SUB ? '-' : '+');

	initStringInfo(&buf);
	initStringInfo(&castString);

	if (castType)
		appendStringInfo(&castString, "::%s", castType);

	if (!count_col)
	{
		/*
		 * If the attributes don't have count columns then calc the result
		 * by using the operator simply.
		 */
		appendStringInfo(&buf, "(%s OPERATOR(pg_catalog.%c) %s)%s",
			col1, op_char, col2, castString.data);
	}
	else
	{
		/*
		 * If the attributes have count columns then consider the condition
		 * where the result becomes NULL.
		 */
		char *null_cond = get_null_condition_string(op, arg1, arg2, count_col);

		appendStringInfo(&buf,
			"(CASE WHEN %s THEN NULL "
				"WHEN %s IS NULL THEN %s "
				"WHEN %s IS NULL THEN %s "
				"ELSE (%s OPERATOR(pg_catalog.%c) %s)%s END)",
			null_cond,
			col1, col2,
			col2, col1,
			col1, op_char, col2, castString.data
		);
	}

	return buf.data;
}

/*
 * get_null_condition_string
 *
 * Build a predicate string for CASE clause to check if an aggregate value
 * will became NULL after the given operation is applied.
 */
static char *
get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col)
{
	StringInfoData null_cond;
	initStringInfo(&null_cond);

	switch (op)
	{
		case IVM_ADD:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) 0 AND %s OPERATOR(pg_catalog.=) 0",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		case IVM_SUB:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) %s",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		default:
			elog(ERROR,"unknown operation");
	}

	return null_cond.data;
}


/*
 * apply_old_delta_with_count
 *
 * Execute a query for applying a delta table given by deltname_old
 * which contains tuples to be deleted from to a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct. Also, when a table in EXISTS sub queries
 * is modified.
 *
 * If the view desn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing resnames of aggregates and SET clause for
 * updating aggregate values.
 *
 * If the view has min or max aggregate, this requires a list of resnames of
 * min/max aggregates and a list of boolean which represents which entries in
 * minmax_list is min. These are necessary to check if we need to recalculate
 * min or max aggregate values. In this case, this query returns TID and keys
 * of tuples which need to be recalculated.  This result and the number of rows
 * are stored in tuptables and num_recalc repectedly.
 */
static void
apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				List *minmax_list, List *is_min_list,
				const char *count_colname,
				SPITupleTable **tuptable_recalc, uint64 *num_recalc)
{
	StringInfoData	querybuf;
	char   *match_cond;
	char   *updt_returning = "";
	char   *select_for_recalc = "SELECT";
	bool	agg_without_groupby = (list_length(keys) == 0);

	Assert(tuptable_recalc != NULL);
	Assert(num_recalc != NULL);

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/*
	 * We need a special RETURNING clause and SELECT statement for min/max to
	 * check which tuple needs re-calculation from base tables.
	 */
	if (minmax_list)
	{
		updt_returning = get_returning_string(minmax_list, is_min_list, keys);
		select_for_recalc = get_select_for_recalc_string(keys);
	}

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH t AS ("			/* collecting tid of target tuples in the view */
						"SELECT diff.%s, "			/* count column */
								"(diff.%s OPERATOR(pg_catalog.=) mv.%s AND %s) AS for_dlt, "
								"mv.ctid "
								"%s "				/* aggregate columns */
						"FROM %s AS mv, %s AS diff "
						"WHERE %s"					/* tuple matching condition */
					"), updt AS ("			/* update a tuple if this is not to be deleted */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.-) t.%s "
											"%s"	/* SET clauses for aggregates */
						"FROM t WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND NOT for_dlt "
						"%s"						/* RETURNING clause for recalc infomation */
					"), dlt AS ("			/* delete a tuple if this is to be deleted */
						"DELETE FROM %s AS mv USING t "
						"WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND for_dlt"
					") %s",							/* SELECT returning which tuples need to be recalculated */
					count_colname,
					count_colname, count_colname, (agg_without_groupby ? "false" : "true"),
					(aggs_list != NULL ? aggs_list->data : ""),
					matviewname, deltaname_old,
					match_cond,
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					updt_returning,
					matviewname,
					select_for_recalc);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Return tuples to be recalculated. */
	if (minmax_list)
	{
		*tuptable_recalc = SPI_tuptable;
		*num_recalc = SPI_processed;
	}
	else
	{
		*tuptable_recalc = NULL;
		*num_recalc = 0;
	}
}

/*
 * apply_old_delta
 *
 * Execute a query for applying a delta table given by deltname_old
 * which contains tuples to be deleted from to a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys)
{
	StringInfoData	querybuf;
	StringInfoData	keysbuf;
	char   *match_cond;
	ListCell *lc;

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&keysbuf);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		appendStringInfo(&keysbuf, "%s", quote_qualified_identifier("mv", resname));
		if (lnext(keys, lc))
			appendStringInfo(&keysbuf, ", ");
	}

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
	"DELETE FROM %s WHERE ctid IN ("
		"SELECT tid FROM (SELECT pg_catalog.row_number() over (partition by %s) AS \"__ivm_row_number__\","
								  "mv.ctid AS tid,"
								  "diff.\"__ivm_count__\""
						 "FROM %s AS mv, %s AS diff "
						 "WHERE %s) v "
					"WHERE v.\"__ivm_row_number__\" OPERATOR(pg_catalog.<=) v.\"__ivm_count__\")",
					matviewname,
					keysbuf.data,
					matviewname, deltaname_old,
					match_cond);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * apply_new_delta_with_count
 *
 * Execute a query for applying a delta table given by deltname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct. Also, when a table in EXISTS sub queries
 * is modified.
 *
 * If the view desn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing SET clause for updating aggregate values.
 */
static void
apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo aggs_set, StringInfo target_list,
				const char* count_colname)
{
	StringInfoData	querybuf;
	StringInfoData	returning_keys;
	ListCell	*lc;
	char	*match_cond = "";

	/* build WHERE condition for searching tuples to be updated */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&returning_keys);
	if (keys)
	{
		foreach (lc, keys)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
			char   *resname = NameStr(attr->attname);
			appendStringInfo(&returning_keys, "%s", quote_qualified_identifier("mv", resname));
			if (lnext(keys, lc))
				appendStringInfo(&returning_keys, ", ");
		}
	}
	else
		appendStringInfo(&returning_keys, "NULL");

	/* Search for matching tuples from the view and update if found or insert if not. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH updt AS ("		/* update a tuple if this exists in the view */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.+) diff.%s "
											"%s "	/* SET clauses for aggregates */
						"FROM %s AS diff "
						"WHERE %s "					/* tuple matching condition */
						"RETURNING %s"				/* returning keys of updated tuples */
					") INSERT INTO %s (%s)"	/* insert a new tuple if this doesn't existw */
						"SELECT %s FROM %s AS diff "
						"WHERE NOT EXISTS (SELECT 1 FROM updt AS mv WHERE %s);",
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					deltaname_new,
					match_cond,
					returning_keys.data,
					matviewname, target_list->data,
					target_list->data, deltaname_new,
					match_cond);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * apply_new_delta
 *
 * Execute a query for applying a delta table given by deltname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list)
{
	StringInfoData	querybuf;

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"INSERT INTO %s (%s) SELECT %s FROM ("
						"SELECT diff.*, pg_catalog.generate_series(1, diff.\"__ivm_count__\") AS __ivm_generate_series__ "
						"FROM %s AS diff) AS v",
					matviewname, target_list->data, target_list->data,
					deltaname_new);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * get_matching_condition_string
 *
 * Build a predicate string for looking for a tuple with given keys.
 */
static char *
get_matching_condition_string(List *keys)
{
	StringInfoData match_cond;
	ListCell	*lc;

	/* If there is no key columns, the condition is always true. */
	if (keys == NIL)
		return "true";

	initStringInfo(&match_cond);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		char   *mv_resname = quote_qualified_identifier("mv", resname);
		char   *diff_resname = quote_qualified_identifier("diff", resname);
		Oid		typid = attr->atttypid;

		/* Considering NULL values, we can not use simple = operator. */
		appendStringInfo(&match_cond, "(");
		generate_equal(&match_cond, typid, mv_resname, diff_resname);
		appendStringInfo(&match_cond, " OR (%s IS NULL AND %s IS NULL))",
						 mv_resname, diff_resname);

		if (lnext(keys, lc))
			appendStringInfo(&match_cond, " AND ");
	}

	return match_cond.data;
}

/*
 * get_returning_string
 *
 * Build a string for RETURNING clause of UPDATE used in apply_old_delta_with_count.
 * This clause returns ctid and a boolean value that indicates if we need to
 * recalculate min or max value, for each updated row.
 */
static char *
get_returning_string(List *minmax_list, List *is_min_list, List *keys)
{
	StringInfoData returning;
	char		*recalc_cond;
	ListCell	*lc;

	Assert(minmax_list != NIL && is_min_list != NIL);
	recalc_cond = get_minmax_recalc_condition_string(minmax_list, is_min_list);

	initStringInfo(&returning);

	appendStringInfo(&returning, "RETURNING mv.ctid AS tid, (%s) AS recalc", recalc_cond);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char *resname = NameStr(attr->attname);
		appendStringInfo(&returning, ", %s", quote_qualified_identifier("mv", resname));
	}

	return returning.data;
}

/*
 * get_minmax_recalc_condition_string
 *
 * Build a predicate string for checking if any min/max aggregate
 * value needs to be recalculated.
 */
static char *
get_minmax_recalc_condition_string(List *minmax_list, List *is_min_list)
{
	StringInfoData recalc_cond;
	ListCell	*lc1, *lc2;

	initStringInfo(&recalc_cond);

	Assert (list_length(minmax_list) == list_length(is_min_list));

	forboth (lc1, minmax_list, lc2, is_min_list)
	{
		char   *resname = (char *) lfirst(lc1);
		bool	is_min = (bool) lfirst_int(lc2);
		char   *op_str = (is_min ? ">=" : "<=");

		appendStringInfo(&recalc_cond, "%s OPERATOR(pg_catalog.%s) %s",
			quote_qualified_identifier("mv", resname),
			op_str,
			quote_qualified_identifier("t", resname)
		);

		if (lnext(minmax_list, lc1))
			appendStringInfo(&recalc_cond, " OR ");
	}

	return recalc_cond.data;
}

/*
 * get_select_for_recalc_string
 *
 * Build a query to return tid and keys of tuples which need
 * recalculation. This is used as the result of the query
 * built by apply_old_delta.
 */
static char *
get_select_for_recalc_string(List *keys)
{
	StringInfoData qry;
	ListCell	*lc;

	initStringInfo(&qry);

	appendStringInfo(&qry, "SELECT tid");
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		appendStringInfo(&qry, ", %s", NameStr(attr->attname));
	}

	appendStringInfo(&qry, " FROM updt WHERE recalc");

	return qry.data;
}

/*
 * recalc_and_set_values
 *
 * Recalculate tuples in a materialized from base tables and update these.
 * The tuples which needs recalculation are specified by keys, and resnames
 * of columns to be updated are specified by namelist. TIDs and key values
 * are given by tuples in tuptable_recalc. Its first attribute must be TID
 * and key values must be following this.
 */
static void
recalc_and_set_values(SPITupleTable *tuptable_recalc, int64 num_tuples,
					  List *namelist, List *keys, Relation matviewRel)
{
	TupleDesc   tupdesc_recalc = tuptable_recalc->tupdesc;
	Oid		   *keyTypes = NULL, *types = NULL;
	char	   *keyNulls = NULL, *nulls = NULL;
	Datum	   *keyVals = NULL, *vals = NULL;
	int			num_vals = list_length(namelist);
	int			num_keys = list_length(keys);
	uint64      i;

	/* If we have keys, initialize arrays for them. */
	if (keys)
	{
		keyTypes = palloc(sizeof(Oid) * num_keys);
		keyNulls = palloc(sizeof(char) * num_keys);
		keyVals = palloc(sizeof(Datum) * num_keys);
		/* a tuple contains keys to be recalculated and ctid to be updated*/
		Assert(tupdesc_recalc->natts == num_keys + 1);

		/* Types of key attributes  */
		for (i = 0; i < num_keys; i++)
			keyTypes[i] = TupleDescAttr(tupdesc_recalc, i + 1)->atttypid;
	}

	/* allocate memory for all attribute names and tid */
	types = palloc(sizeof(Oid) * (num_vals + 1));
	nulls = palloc(sizeof(char) * (num_vals + 1));
	vals = palloc(sizeof(Datum) * (num_vals + 1));

	/* For each tuple which needs recalculation */
	for (i = 0; i < num_tuples; i++)
	{
		int j;
		bool isnull;
		SPIPlanPtr plan;
		SPITupleTable *tuptable_newvals;
		TupleDesc   tupdesc_newvals;

		/* Set group key values as parameters if needed. */
		if (keys)
		{
			for (j = 0; j < num_keys; j++)
			{
				keyVals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, j + 2, &isnull);
				if (isnull)
					keyNulls[j] = 'n';
				else
					keyNulls[j] = ' ';
			}
		}

		/*
		 * Get recalculated values from base tables. The result must be
		 * only one tuple thich contains the new values for specified keys.
		 */
		plan = get_plan_for_recalc(matviewRel, namelist, keys, keyTypes);
		if (SPI_execute_plan(plan, keyVals, keyNulls, false, 0) != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_plan");
		if (SPI_processed != 1)
			elog(ERROR, "SPI_execute_plan returned zero or more than one rows");

		tuptable_newvals = SPI_tuptable;
		tupdesc_newvals = tuptable_newvals->tupdesc;

		Assert(tupdesc_newvals->natts == num_vals);

		/* Set the new values as parameters */
		for (j = 0; j < tupdesc_newvals->natts; j++)
		{
			if (i == 0)
				types[j] = TupleDescAttr(tupdesc_newvals, j)->atttypid;

			vals[j] = SPI_getbinval(tuptable_newvals->vals[0], tupdesc_newvals, j + 1, &isnull);
			if (isnull)
				nulls[j] = 'n';
			else
				nulls[j] = ' ';
		}
		/* Set TID of the view tuple to be updated as a parameter */
		types[j] = TIDOID;
		vals[j] = SPI_getbinval(tuptable_recalc->vals[i], tupdesc_recalc, 1, &isnull);
		nulls[j] = ' ';

		/* Update the view tuple to the new values */
		plan = get_plan_for_set_values(matviewRel, namelist, types);
		if (SPI_execute_plan(plan, vals, nulls, false, 0) != SPI_OK_UPDATE)
			elog(ERROR, "SPI_execute_plan");
	}
}

/*
 * get_plan_for_recalc
 *
 * Create or fetch a plan for recalculating value in the view's target list
 * from base tables using the definition query of materialized view specified
 * by matviewRel. namelist is a list of resnames of values to be recalculated.
 *
 * keys is a list of keys to identify tuples to be recalculated if this is not
 * empty. KeyTypes is an array of types of keys.
 */
static SPIPlanPtr
get_plan_for_recalc(Relation matviewRel, List *namelist, List *keys, Oid *keyTypes)
{
	MV_QueryKey hash_key;
	SPIPlanPtr	plan;

	/* Fetch or prepare a saved plan for the recalculation */
	mv_BuildQueryKey(&hash_key, RelationGetRelid(matviewRel), MV_PLAN_RECALC);
	if ((plan = mv_FetchPreparedPlan(&hash_key)) == NULL)
	{
		ListCell	   *lc;
		StringInfoData	str;
		char   *viewdef;

		/* get view definition of matview */
		viewdef = pg_ivm_get_viewdef(matviewRel, false);

		/*
		 * Build a query string for recalculating values. This is like
		 *
		 *  SELECT x1, x2, x3, ... FROM ( ... view definition query ...) mv
		 *   WHERE (key1, key2, ...) = ($1, $2, ...);
		 */

		initStringInfo(&str);
		appendStringInfo(&str, "SELECT ");
		foreach (lc, namelist)
		{
			appendStringInfo(&str, "%s", (char *) lfirst(lc));
			if (lnext(namelist, lc))
				appendStringInfoString(&str, ", ");
		}
		appendStringInfo(&str, " FROM (%s) mv", viewdef);

		if (keys)
		{
			int		i = 1;
			char	paramname[16];

			appendStringInfo(&str, " WHERE (");
			foreach (lc, keys)
			{
				Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
				char   *resname = NameStr(attr->attname);
				Oid		typid = attr->atttypid;

				sprintf(paramname, "$%d", i);
				appendStringInfo(&str, "(");
				generate_equal(&str, typid, resname, paramname);
				appendStringInfo(&str, " OR (%s IS NULL AND %s IS NULL))",
								 resname, paramname);

				if (lnext(keys, lc))
					appendStringInfoString(&str, " AND ");
				i++;
			}
			appendStringInfo(&str, ")");
		}
		else
			keyTypes = NULL;

		plan = SPI_prepare(str.data, list_length(keys), keyTypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&hash_key, plan);
	}

	return plan;
}

/*
 * get_plan_for_set_values
 *
 * Create or fetch a plan for applying new values calculated by
 * get_plan_for_recalc to a materialized view specified by matviewRel.
 * namelist is a list of resnames of attributes to be updated, and
 * valTypes is an array of types of the
 * values.
 */
static SPIPlanPtr
get_plan_for_set_values(Relation matviewRel, List *namelist, Oid *valTypes)
{
	MV_QueryKey	key;
	SPIPlanPtr	plan;
	char	   *matviewname;

	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	/* Fetch or prepare a saved plan for the real check */
	mv_BuildQueryKey(&key, RelationGetRelid(matviewRel), MV_PLAN_SET_VALUE);
	if ((plan = mv_FetchPreparedPlan(&key)) == NULL)
	{
		ListCell	  *lc;
		StringInfoData str;
		int		i;

		/*
		 * Build a query string for applying min/max values. This is like
		 *
		 *  UPDATE matviewname AS mv
		 *   SET (x1, x2, x3, x4) = ($1, $2, $3, $4)
		 *   WHERE ctid = $5;
		 */

		initStringInfo(&str);
		appendStringInfo(&str, "UPDATE %s AS mv SET (", matviewname);
		foreach (lc, namelist)
		{
			appendStringInfo(&str, "%s", (char *) lfirst(lc));
			if (lnext(namelist, lc))
				appendStringInfoString(&str, ", ");
		}
		appendStringInfo(&str, ") = ROW(");

		for (i = 1; i <= list_length(namelist); i++)
			appendStringInfo(&str, "%s$%d", (i==1 ? "" : ", "), i);

		appendStringInfo(&str, ") WHERE ctid OPERATOR(pg_catalog.=) $%d", i);

		plan = SPI_prepare(str.data, list_length(namelist) + 1, valTypes);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s", SPI_result_code_string(SPI_result), str.data);

		SPI_keepplan(plan);
		mv_HashPreparedPlan(&key, plan);
	}

	return plan;
}

/*
 * generate_equal
 *
 * Generate an equality clause using given operands' default equality
 * operator.
 */
static void
generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop)
{
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(opttype, TYPECACHE_EQ_OPR);
	if (!OidIsValid(typentry->eq_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an equality operator for type %s",
						format_type_be_qualified(opttype))));

	generate_operator_clause(querybuf,
							 leftop, opttype,
							 typentry->eq_opr,
							 rightop, opttype);
}

/*
 * mv_InitHashTables
 */
static void
mv_InitHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MV_QueryKey);
	ctl.entrysize = sizeof(MV_QueryHashEntry);
	mv_query_cache = hash_create("MV query cache",
								 MV_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(MV_TriggerHashEntry);
	mv_trigger_info = hash_create("MV trigger info",
								 MV_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * mv_FetchPreparedPlan
 */
static SPIPlanPtr
mv_FetchPreparedPlan(MV_QueryKey *key)
{
	MV_QueryHashEntry *entry;
	SPIPlanPtr	plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (MV_QueryHashEntry *) hash_search(mv_query_cache,
											  (void *) key,
											  HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/*
	 * Check whether the plan is still valid.  If it isn't, we don't want to
	 * simply rely on plancache.c to regenerate it; rather we should start
	 * from scratch and rebuild the query text too.  This is to cover cases
	 * such as table/column renames.  We depend on the plancache machinery to
	 * detect possible invalidations, though.
	 *
	 * CAUTION: this check is only trustworthy if the caller has already
	 * locked both materialized views and base tables.
	 *
	 * Also, check whether the search_path is still the same as when we made it.
	 * If it isn't, we need to rebuild the query text because the result of
	 * pg_ivm_get_viewdef() will change.
	 */
	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan) &&
		OverrideSearchPathMatchesCurrent(entry->search_path))
		return plan;

	/*
	 * Otherwise we might as well flush the cached plan now, to free a little
	 * memory space before we make a new one.
	 */
	if (plan)
		SPI_freeplan(plan);
	if (entry->search_path)
		pfree(entry->search_path);

	entry->plan = NULL;
	entry->search_path = NULL;

	return NULL;
}

/*
 * mv_HashPreparedPlan
 *
 * Add another plan to our private SPI query plan hashtable.
 */
static void
mv_HashPreparedPlan(MV_QueryKey *key, SPIPlanPtr plan)
{
	MV_QueryHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!mv_query_cache)
		mv_InitHashTables();

	/*
	 * Add the new plan.  We might be overwriting an entry previously found
	 * invalid by mv_FetchPreparedPlan.
	 */
	entry = (MV_QueryHashEntry *) hash_search(mv_query_cache,
											  (void *) key,
											  HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
	entry->search_path = GetOverrideSearchPath(TopMemoryContext);
}

/*
 * mv_BuildQueryKey
 *
 * Construct a hashtable key for a prepared SPI plan for IVM.
 */
static void
mv_BuildQueryKey(MV_QueryKey *key, Oid matview_id, int32 query_type)
{
	/*
	 * We assume struct MV_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	key->matview_id = matview_id;
	key->query_type = query_type;
}

/*
 * AtAbort_IVM
 *
 * Clean up hash entries for all materialized views. This is called at
 * transaction abort.
 */
void
AtAbort_IVM()
{
	HASH_SEQ_STATUS seq;
	MV_TriggerHashEntry *entry;

	if (mv_trigger_info)
	{
		hash_seq_init(&seq, mv_trigger_info);
		while ((entry = hash_seq_search(&seq)) != NULL)
			clean_up_IVM_hash_entry(entry, true);
	}

	in_delta_calculation = false;
}

/*
 * clean_up_IVM_hash_entry
 *
 * Clean up tuple stores and hash entries for a materialized view after its
 * maintenance finished.
 */
static void
clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort)
{
	bool found;
	ListCell *lc;

	foreach(lc, entry->tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;

		foreach(lc2, table->old_tuplestores)
		{
			Tuplestorestate *tup = (Tuplestorestate *) lfirst(lc2);
			tuplestore_end(tup);
		}
		foreach(lc2, table->new_tuplestores)
		{
			Tuplestorestate *tup = (Tuplestorestate *) lfirst(lc2);
			tuplestore_end(tup);
		}

		list_free(table->old_tuplestores);
		list_free(table->new_tuplestores);
		if (!is_abort)
		{
			ExecDropSingleTupleTableSlot(table->slot);
			table_close(table->rel, NoLock);
		}
	}
	list_free(entry->tables);

	if (!is_abort)
		UnregisterSnapshot(entry->snapshot);


	hash_search(mv_trigger_info, (void *) &entry->matview_id, HASH_REMOVE, &found);
}

/*
 * getColumnNameStartWith
 *
 * Search a column name which starts with the given string from the given RTE,
 * and return the first found one or NULL if not found.
 */
char *
getColumnNameStartWith(RangeTblEntry *rte, char *str, int *attnum)
{
	char *colname;
	ListCell *lc;
	Alias *alias = rte->eref;

	(*attnum) = 0;
	foreach(lc, alias->colnames)
	{
		(*attnum)++;
		if (strncmp(strVal(lfirst(lc)), str, strlen(str)) == 0)
		{
			colname = pstrdup(strVal(lfirst(lc)));
			return colname;
		}
	}
	return NULL;
}

/*
 * isIvmName
 *
 * Check if this is a IVM hidden column from the name.
 */
bool
isIvmName(const char *s)
{
	if (s)
		return (strncmp(s, "__ivm_", 6) == 0);
	return false;
}
