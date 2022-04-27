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
#include "access/table.h"
#include "access/xact.h"
#include "commands/tablecmds.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rowsecurity.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

#include "pg_ivm.h"


#define MV_INIT_QUERYHASHSIZE	16

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

	TransactionId	xid;	/* Transaction id before the first table is modified*/
	CommandId		cid;	/* Command id before the first table is modified */

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

	List   *rte_indexes;		/* List of RTE index of the modified table */
	RangeTblEntry *original_rte;	/* the original RTE saved before rewriting query */
} MV_TriggerTable;

static HTAB *mv_trigger_info = NULL;

/* ENR name for materialized view delta */
#define NEW_DELTA_ENRNAME "new_delta"
#define OLD_DELTA_ENRNAME "old_delta"

static int	immv_maintenance_depth = 0;

static uint64 refresh_immv_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString);

static void OpenImmvIncrementalMaintenance(void);
static void CloseImmvIncrementalMaintenance(void);
static Query *get_immv_query(Relation matviewRel);

static Query *rewrite_query_for_preupdate_state(Query *query, List *tables,
								  TransactionId xid, CommandId cid,
								  ParseState *pstate);
static void register_delta_ENRs(ParseState *pstate, Query *query, List *tables);
static char *make_delta_enr_name(const char *prefix, Oid relid, int count);
static RangeTblEntry *get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 TransactionId xid, CommandId cid,
				 QueryEnvironment *queryEnv);
static RangeTblEntry *union_ENRs(RangeTblEntry *rte, Oid relid, List *enr_rtes, const char *prefix,
		   QueryEnvironment *queryEnv);
static Query *rewrite_query_for_distinct(Query *query, ParseState *pstate);

static void calc_delta(MV_TriggerTable *table, int rte_index, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv);
static Query *rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, int rte_index);

static void apply_delta(Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname);
static void apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys);
static void apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, const char *count_colname);
static void apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list);
static void apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, const char* count_colname);
static char *get_matching_condition_string(List *keys);
static void generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop);

static void mv_InitHashTables(void);
static void clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry);

static List *get_securityQuals(Oid relId, int rt_index, Query *query);

/* SQL callable functions */
PG_FUNCTION_INFO_V1(IVM_immediate_before);
PG_FUNCTION_INFO_V1(IVM_immediate_maintenance);

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
static Query *
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
		Snapshot snapshot = GetActiveSnapshot();

		entry->matview_id = matviewOid;
		entry->before_trig_count = 0;
		entry->after_trig_count = 0;
		entry->xid = GetCurrentTransactionId();
		entry->cid = snapshot->curcid;
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
		table->rte_indexes = NIL;
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

	/* get view query*/
	query = get_immv_query(matviewRel);

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

	/* Set all tables in the query to pre-update state */
	rewritten = rewrite_query_for_preupdate_state(rewritten, entry->tables,
												  entry->xid, entry->cid,
												  pstate);
	/* Rewrite for DISTINCT clause */
	rewritten = rewrite_query_for_distinct(rewritten, pstate);

	/* Create tuplestores to store view deltas */
	if (entry->has_old)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		old_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_old = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_old,
									old_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);

		MemoryContextSwitchTo(oldcxt);
	}
	if (entry->has_new)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		new_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_new = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_new,
									new_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);
		MemoryContextSwitchTo(oldcxt);
	}

	/* for all modified tables */
	foreach(lc, entry->tables)
	{
		ListCell *lc2;

		table = (MV_TriggerTable *) lfirst(lc);

		/* loop for self-join */
		foreach(lc2, table->rte_indexes)
		{
			int	rte_index = lfirst_int(lc2);
			TupleDesc		tupdesc_old;
			TupleDesc		tupdesc_new;
			bool	use_count = false;
			char   *count_colname = NULL;

			count_colname = pstrdup("__ivm_count__");

			if (query->distinctClause)
				use_count = true;

			/* calculate delta tables */
			calc_delta(table, rte_index, rewritten, dest_old, dest_new,
					   &tupdesc_old, &tupdesc_new, queryEnv);

			/* Set the table in the query to post-update state */
			rewritten = rewrite_query_for_postupdate_state(rewritten, table, rte_index);

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
	clean_up_IVM_hash_entry(entry);
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
 * tables are modified. xid and cid are the transaction id and command id
 * before the first table was modified.
 */
static Query*
rewrite_query_for_preupdate_state(Query *query, List *tables,
								  TransactionId xid, CommandId cid,
								  ParseState *pstate)
{
	ListCell *lc;
	int num_rte = list_length(query->rtable);
	int i;


	/* register delta ENRs */
	register_delta_ENRs(pstate, query, tables);

	/* XXX: Is necessary? Is this right timing? */
	AcquireRewriteLocks(query, true, false);

	i = 1;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);

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
				lfirst(lc) = get_prestate_rte(r, table, xid, cid, pstate->p_queryEnv);
				table->rte_indexes = lappend_int(table->rte_indexes, i);
				break;
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
			/* if base table has RLS, set security condition to enr */
			rte->securityQuals = get_securityQuals(table->table_id, list_length(query->rtable) + 1, query);

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
			/* if base table has RLS, set security condition to enr*/
			rte->securityQuals = get_securityQuals(table->table_id, list_length(query->rtable) + 1, query);

			query->rtable = lappend(query->rtable, rte);
			table->new_rtes = lappend(table->new_rtes, rte);

			count++;
		}
	}
}

/*
 * get_prestate_rte
 *
 * Rewrite RTE of the modified table to a subquery which represents
 * "pre-state" table. The original RTE is saved in table->rte_original.
 */
static RangeTblEntry*
get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 TransactionId xid, CommandId cid,
				 QueryEnvironment *queryEnv)
{
	StringInfoData str;
	RawStmt *raw;
	Query *sub;
	Relation rel;
	ParseState *pstate;
	char *relname;
	int i;

	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * We can use NoLock here since AcquireRewriteLocks should
	 * have locked the rel already.
	 */
	rel = table_open(table->table_id, NoLock);
	relname = quote_qualified_identifier(
					get_namespace_name(RelationGetNamespace(rel)),
									   RelationGetRelationName(rel));
	table_close(rel, NoLock);

	initStringInfo(&str);
	appendStringInfo(&str,
		"SELECT t.* FROM %s t"
		" WHERE (age(t.xmin) - age(%u::text::xid) > 0) OR"
		" (t.xmin = %u AND t.cmin::text::int < %u)",
			relname, xid, xid, cid);

	for (i = 0; i < list_length(table->old_tuplestores); i++)
	{
		appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT * FROM %s",
			make_delta_enr_name("old", table->table_id, i));
	}

	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	sub = transformStmt(pstate, raw->stmt);

	/* If this query has setOperations, RTEs in rtables has a subquery which contains ENR */
	if (sub->setOperations != NULL)
	{
		ListCell *lc;

		/* add securityQuals for tuplestores */
		foreach (lc, sub->rtable)
		{
			RangeTblEntry *rte;
			RangeTblEntry *sub_rte;

			rte = (RangeTblEntry *)lfirst(lc);
			Assert(rte->subquery != NULL);

			sub_rte = (RangeTblEntry *)linitial(rte->subquery->rtable);
			if (sub_rte->rtekind == RTE_NAMEDTUPLESTORE)
				/* rt_index is always 1, bacause subquery has enr_rte only */
				sub_rte->securityQuals = get_securityQuals(sub_rte->relid, 1, sub);
		}
	}

	/* save the original RTE */
	table->original_rte = copyObject(rte);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = sub;
	rte->security_barrier = false;
	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	rte->requiredPerms = 0;		/* no permission check on subquery itself */
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;

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
 * Make a single table delta by unionning all transition tables of the modified table
 * whose RTE is specified by
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
	RangeTblEntry *enr_rte;

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

	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	sub = transformStmt(pstate, raw->stmt);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = sub;
	rte->security_barrier = false;
	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	rte->requiredPerms = 0;		/* no permission check on subquery itself */
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;
	/* if base table has RLS, set security condition to enr*/
	enr_rte = (RangeTblEntry *)linitial(sub->rtable);
	/* rt_index is always 1, bacause subquery has enr_rte only */
	enr_rte->securityQuals = get_securityQuals(relid, 1, sub);

	return rte;
}

/*
 * rewrite_query_for_distinct
 *
 * Rewrite query for counting DISTINCT clause.
 */
static Query *
rewrite_query_for_distinct(Query *query, ParseState *pstate)
{
	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;

	/* Add count(*) for counting distinct tuples in views */
	fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
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

/*
 * calc_delta
 *
 * Calculate view deltas generated under the modification of a table specified
 * by the RTE index.
 */
static void
calc_delta(MV_TriggerTable *table, int rte_index, Query *query,
			DestReceiver *dest_old, DestReceiver *dest_new,
			TupleDesc *tupdesc_old, TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv)
{
	ListCell *lc = list_nth_cell(query->rtable, rte_index - 1);
	RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

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
}

/*
 * rewrite_query_for_postupdate_state
 *
 * Rewrite the query so that the specified base table's RTEs will represent
 * "post-update" state of tables. This is called after the view delta
 * calculation due to changes on this table finishes.
 */
static Query*
rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, int rte_index)
{
	ListCell *lc = list_nth_cell(query->rtable, rte_index - 1);

	/* Retore the original RTE */
	lfirst(lc) = table->original_rte;

	return query;
}

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
	Relation	matviewRel;
	char	   *matviewname;
	ListCell	*lc;
	int			i;
	List	   *keys = NIL;


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

		i++;

		if (tle->resjunk)
			continue;

		keys = lappend(keys, attr);
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
									   keys, count_colname);
		else
			apply_old_delta(matviewname, OLD_DELTA_ENRNAME, keys);

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
								keys, &target_list_buf, count_colname);
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
 * apply_old_delta_with_count
 *
 * Execute a query for applying a delta table given by deltname_old
 * which contains tuples to be deleted from to a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct.
 */
static void
apply_old_delta_with_count(const char *matviewname, const char *deltaname_old,
				List *keys, const char *count_colname)
{
	StringInfoData	querybuf;
	char   *match_cond;

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH t AS ("			/* collecting tid of target tuples in the view */
						"SELECT diff.%s, "			/* count column */
								"(diff.%s OPERATOR(pg_catalog.=) mv.%s) AS for_dlt, "
								"mv.ctid "
						"FROM %s AS mv, %s AS diff "
						"WHERE %s"					/* tuple matching condition */
					"), updt AS ("			/* update a tuple if this is not to be deleted */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.-) t.%s "
						"FROM t WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND NOT for_dlt "
					")"
					/* delete a tuple if this is to be deleted */
					"DELETE FROM %s AS mv USING t "
					"WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND for_dlt",
					count_colname,
					count_colname, count_colname,
					matviewname, deltaname_old,
					match_cond,
					matviewname, count_colname, count_colname, count_colname,
					matviewname);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
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
		"SELECT tid FROM (SELECT row_number() over (partition by %s) AS \"__ivm_row_number__\","
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
 */
static void
apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, const char* count_colname)
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
						"FROM %s AS diff "
						"WHERE %s "					/* tuple matching condition */
						"RETURNING %s"				/* returning keys of updated tuples */
					") INSERT INTO %s (%s)"	/* insert a new tuple if this doesn't existw */
						"SELECT %s FROM %s AS diff "
						"WHERE NOT EXISTS (SELECT 1 FROM updt AS mv WHERE %s);",
					matviewname, count_colname, count_colname, count_colname,
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
						"SELECT diff.*, generate_series(1, diff.\"__ivm_count__\") "
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
						format_type_be(opttype))));

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
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(MV_TriggerHashEntry);
	mv_trigger_info = hash_create("MV trigger info",
								 MV_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);
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
			clean_up_IVM_hash_entry(entry);
	}
}

/*
 * clean_up_IVM_hash_entry
 *
 * Clean up tuple stores and hash entries for a materialized view after its
 * maintenance finished.
 */
static void
clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry)
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
	}
	list_free(entry->tables);

	hash_search(mv_trigger_info, (void *) &entry->matview_id, HASH_REMOVE, &found);
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

/*
 * get_securityQuals
 *
 * Get row security policy on a relation.
 * This is used by IVM for copying RLS from base table to enr.
 */
static List *
get_securityQuals(Oid relId, int rt_index, Query *query)
{
	ParseState *pstate;
	Relation rel;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;
	List *securityQuals;
	List *withCheckOptions;
	bool  hasRowSecurity;
	bool  hasSubLinks;

	securityQuals = NIL;
	pstate = make_parsestate(NULL);

	rel = table_open(relId, NoLock);
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, false);
	rte = nsitem->p_rte;

	get_row_security_policies(query, rte, rt_index,
							  &securityQuals, &withCheckOptions,
							  &hasRowSecurity, &hasSubLinks);

	/*
	 * Make sure the query is marked correctly if row level security
	 * applies, or if the new quals had sublinks.
	 */
	if (hasRowSecurity)
		query->hasRowSecurity = true;
	if (hasSubLinks)
		query->hasSubLinks = true;

	table_close(rel, NoLock);

	return securityQuals;
}
