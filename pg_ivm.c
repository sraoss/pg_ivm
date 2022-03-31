/*-------------------------------------------------------------------------
 *
 * pg_ivm.c
 *	  incremental view maintenance extension
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint_d.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_trigger_d.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

#define Natts_pg_ivm_immv 2

#define Anum_pg_ivm_immv_immvrelid 1
#define Anum_pg_ivm_immv_viewdef 2

PG_MODULE_MAGIC;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_intorel;

static int	immv_maintenance_depth = 0;

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


static Oid pg_ivm_immv_id = InvalidOid;
static Oid pg_ivm_immv_pkey_id = InvalidOid;

/* ENR name for materialized view delta */
#define NEW_DELTA_ENRNAME "new_delta"
#define OLD_DELTA_ENRNAME "old_delta"

typedef struct
{
	bool	has_agg;
} check_ivm_restriction_context;

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock);
static void CreateChangePreventTrigger(Oid matviewOid);
static void check_ivm_restriction(Node *node);
static bool check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context);
static Bitmapset *get_primary_key_attnos_from_query(Query *query, List **constraintList, bool is_create);

static bool isIvmName(const char *s);

static Oid PgIvmImmvRelationId(void);
static Oid PgIvmImmvPrimaryKeyIndexId(void);

void CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create);
void CreateIndexOnIMMV(Query *query, Relation matviewRel, bool is_create);
Query *rewriteQueryForIMMV(Query *query, List *colNames);
void AtAbort_IVM(void);

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
 
static uint64 refresh_immv_datafill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString);

bool ImmvIncrementalMaintenanceIsEnabled(void);
static void OpenImmvIncrementalMaintenance(void);
static void CloseImmvIncrementalMaintenance(void);

void		_PG_init(void);

PG_FUNCTION_INFO_V1(create_immv);

Datum
create_immv(PG_FUNCTION_ARGS)
{
	text       *t_relname = PG_GETARG_TEXT_PP(0);
	text       *t_sql = PG_GETARG_TEXT_PP(1);
	char *relname;
	char *sql;
	List *parsetree_list;
	RawStmt    *parsetree;

	ParseState *pstate = make_parsestate(NULL);
	Query *query;
	Query *viewQuery;
	IntoClause *into;
	CreateTableAsStmt *stmt;
	StringInfoData command_buf;

	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	uint64 processed;

	sql = text_to_cstring(t_sql);
	relname = text_to_cstring(t_relname);

	initStringInfo(&command_buf);
	appendStringInfo(&command_buf, "CREATE MATERIALIZED VIEW %s AS %s;", relname, sql);
	parsetree_list = pg_parse_query(command_buf.data);
	pstate->p_sourcetext = command_buf.data;

	/* XXX: should we check t_sql before command_buf? */
	if (list_length(parsetree_list) != 1)
		elog(ERROR, "invalid view definition");

	parsetree = linitial_node(RawStmt, parsetree_list);
	query = transformStmt(pstate, parsetree->stmt);

	Assert(query->commandType == CMD_UTILITY && IsA(query->utilityStmt, CreateTableAsStmt));

	stmt = (CreateTableAsStmt*) query->utilityStmt;
	into = stmt->into;
	query = castNode(Query, stmt->query);

	/* Check if the relation exists or not */
	if (CreateTableAsRelExists(stmt))
		elog(ERROR, "error");

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	viewQuery = (Query *) into->viewQuery;
	into->viewQuery = NULL;
	dest = CreateIntoRelDestReceiver(into);

	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	{
		/* check if the query is supported in IMMV definition */
		if (contain_mutable_functions((Node *) query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mutable function is not supported on incrementally maintainable materialized view"),
					 errhint("functions must be marked IMMUTABLE")));

		check_ivm_restriction((Node *) query);

		/* For IMMV, we need to rewrite matview query */
		query = rewriteQueryForIMMV(query, into->colNames);
	}

	{
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 */
		rewritten = QueryRewrite(query);

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for CREATE MATERIALIZED VIEW");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							 CURSOR_OPT_PARALLEL_OK, NULL);

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.  (This could only
		 * matter if the planner executed an allegedly-stable function that
		 * changed the database contents, but let's do it anyway to be
		 * parallel to the EXPLAIN code path.)
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create a QueryDesc, redirecting output to our tuple receiver */
		queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
									GetActiveSnapshot(), InvalidSnapshot,
									dest, NULL, NULL, 0);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount */
		processed = queryDesc->estate->es_processed;;

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}

	/* Create the "view" part of a materialized view. */
	{
		char   *querytree = nodeToString((Node *) viewQuery);
		Datum values[Natts_pg_ivm_immv];
		bool isNulls[Natts_pg_ivm_immv];
		Relation pgIvmImmv;
    	TupleDesc tupleDescriptor;
    	HeapTuple heapTuple;

		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		values[Anum_pg_ivm_immv_immvrelid -1 ] = ObjectIdGetDatum(address.objectId);
		values[Anum_pg_ivm_immv_viewdef -1 ] = CStringGetTextDatum(querytree);

		pgIvmImmv = table_open(PgIvmImmvRelationId(), RowExclusiveLock);

    	tupleDescriptor = RelationGetDescr(pgIvmImmv);
    	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

    	CatalogTupleInsert(pgIvmImmv, heapTuple);

		recordDependencyOnExpr(&address, (Node *) viewQuery, NIL,
							   DEPENDENCY_NORMAL);

		table_close(pgIvmImmv, NoLock);

		CommandCounterIncrement();
	}


	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		{
			Oid matviewOid = address.objectId;
			Relation matviewRel = table_open(matviewOid, NoLock);

			{
				/* Create an index on incremental maintainable materialized view, if possible */
				CreateIndexOnIMMV(viewQuery, matviewRel, true);

				/* Create triggers on incremental maintainable materialized view */
				CreateIvmTriggersOnBaseTables(viewQuery, matviewOid, true);

				CreateChangePreventTrigger(matviewOid);
			}
			table_close(matviewRel, NoLock);
		}
	}

	PG_RETURN_INT64(processed);
}

/*
 * rewriteQueryForIMMV -- rewrite view definition query for IMMV
 *
 * count(*) is added for counting distinct tuples in views.
 */
Query *
rewriteQueryForIMMV(Query *query, List *colNames)
{
	Query *rewritten;

	TargetEntry *tle;
	Node *node;
	ParseState *pstate = make_parsestate(NULL);
	FuncCall *fn;

	rewritten = copyObject(query);
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * Convert DISTINCT to GROUP BY and add count(*) for counting distinct
	 * tuples in views.
	 */
	if (rewritten->distinctClause)
	{
		rewritten->groupClause = transformDistinctClause(NULL, &rewritten->targetList, rewritten->sortClause, false);

		fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
		fn->agg_star = true;

		node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

		tle = makeTargetEntry((Expr *) node,
								list_length(rewritten->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
		rewritten->targetList = lappend(rewritten->targetList, tle);
		rewritten->hasAggs = true;
	}

	return rewritten;
}

static Oid
PgIvmImmvRelationId(void)
{
	if (!OidIsValid(pg_ivm_immv_id))
		pg_ivm_immv_id = get_relname_relid("pg_ivm_immv", PG_CATALOG_NAMESPACE);

	return pg_ivm_immv_id;
}

static Oid
PgIvmImmvPrimaryKeyIndexId(void)
{
	if (!OidIsValid(pg_ivm_immv_pkey_id))
		pg_ivm_immv_pkey_id = get_relname_relid("pg_ivm_immv_pkey", PG_CATALOG_NAMESPACE);

	return pg_ivm_immv_pkey_id;
}

/*
 * CreateIvmTriggersOnBaseTables -- create IVM triggers on all base tables
 */
void
CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create)
{
	Relids	relids = NULL;
	bool	ex_lock = false;
	Index	first_rtindex = is_create ? 1 : PRS2_NEW_VARNO + 1;
	RangeTblEntry *rte;

	/* Immediately return if we don't have any base tables. */
	if (list_length(qry->rtable) < first_rtindex)
		return;

	/*
	 * If the view has more than one base tables, we need an exclusive lock
	 * on the view so that the view would be maintained serially to avoid
	 * the inconsistency that occurs when two base tables are modified in
	 * concurrent transactions. However, if the view has only one table,
	 * we can use a weaker lock.
	 *
	 * The type of lock should be determined here, because if we check the
	 * view definition at maintenance time, we need to acquire a weaker lock,
	 * and upgrading the lock level after this increases probability of
	 * deadlock.
	 *
	 * XXX: For the current extension version, DISTINCT needs exclusive lock 
	 * to prevent inconsistency that can be avoided by using
	 * nulls_not_distinct which is available only in PG15 or later.
	 */

	rte = list_nth(qry->rtable, first_rtindex - 1);
	if (list_length(qry->rtable) > first_rtindex ||
		rte->rtekind != RTE_RELATION || qry->distinctClause)
		ex_lock = true;

	CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)qry, matviewOid, &relids, ex_lock);

	bms_free(relids);
}

static void
CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock)
{
	if (node == NULL)
		return;

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *query = (Query *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)query->jointree, matviewOid, relids, ex_lock);
			}
			break;

		case T_RangeTblRef:
			{
				int			rti = ((RangeTblRef *) node)->rtindex;
				RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

				if (rte->rtekind == RTE_RELATION && !bms_is_member(rte->relid, *relids))
				{
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, ex_lock);

					*relids = bms_add_member(*relids, rte->relid);
				}
			}
			break;

		case T_FromExpr:
			{
				FromExpr   *f = (FromExpr *) node;
				ListCell   *l;

				foreach(l, f->fromlist)
					CreateIvmTriggersOnBaseTablesRecurse(qry, lfirst(l), matviewOid, relids, ex_lock);
			}
			break;

		case T_JoinExpr:
			{
				JoinExpr   *j = (JoinExpr *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, j->larg, matviewOid, relids, ex_lock);
				CreateIvmTriggersOnBaseTablesRecurse(qry, j->rarg, matviewOid, relids, ex_lock);
			}
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
	}
}

/*
 * CreateIvmTrigger -- create IVM trigger on a base table
 */
static void
CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock)
{
	ObjectAddress	refaddr;
	ObjectAddress	address;
	CreateTrigStmt *ivm_trigger;
	List *transitionRels = NIL;

	Assert(timing == TRIGGER_TYPE_BEFORE || timing == TRIGGER_TYPE_AFTER);

	refaddr.classId = RelationRelationId;
	refaddr.objectId = viewOid;
	refaddr.objectSubId = 0;

	ivm_trigger = makeNode(CreateTrigStmt);
	ivm_trigger->relation = NULL;
	ivm_trigger->row = false;

	ivm_trigger->timing = timing;
	ivm_trigger->events = type;

	switch (type)
	{
		case TRIGGER_TYPE_INSERT:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_ins_before" : "IVM_trigger_ins_after");
			break;
		case TRIGGER_TYPE_DELETE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_del_before" : "IVM_trigger_del_after");
			break;
		case TRIGGER_TYPE_UPDATE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_upd_before" : "IVM_trigger_upd_after");
			break;
		default:
			elog(ERROR, "unsupported trigger type");
	}

	if (timing == TRIGGER_TYPE_AFTER)
	{
		if (type == TRIGGER_TYPE_INSERT || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_newtable";
			n->isNew = true;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
		if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_oldtable";
			n->isNew = false;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
	}

	ivm_trigger->funcname =
		(timing == TRIGGER_TYPE_BEFORE ? SystemFuncName("IVM_immediate_before") : SystemFuncName("IVM_immediate_maintenance"));

	ivm_trigger->columns = NIL;
	ivm_trigger->transitionRels = transitionRels;
	ivm_trigger->whenClause = NULL;
	ivm_trigger->isconstraint = false;
	ivm_trigger->deferrable = false;
	ivm_trigger->initdeferred = false;
	ivm_trigger->constrrel = NULL;
	ivm_trigger->args = list_make2(
		makeString(DatumGetPointer(DirectFunctionCall1(oidout, ObjectIdGetDatum(viewOid)))),
		makeString(DatumGetPointer(DirectFunctionCall1(boolout, BoolGetDatum(ex_lock))))
		);

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, true, false);

	recordDependencyOn(&address, &refaddr, DEPENDENCY_AUTO);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

static void
CreateChangePreventTrigger(Oid matviewOid)
{
	ObjectAddress	refaddr;
	ObjectAddress	address;
	CreateTrigStmt *ivm_trigger;

	int16 types[4] = {TRIGGER_TYPE_INSERT, TRIGGER_TYPE_DELETE,
					  TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_TRUNCATE};
	int i;

	refaddr.classId = RelationRelationId;
	refaddr.objectId = matviewOid;
	refaddr.objectSubId = 0;


	ivm_trigger = makeNode(CreateTrigStmt);
	ivm_trigger->relation = NULL;
	ivm_trigger->row = false;

	ivm_trigger->timing = TRIGGER_TYPE_BEFORE;
	ivm_trigger->trigname = "IVM_prevent_immv_change";
	ivm_trigger->funcname = SystemFuncName("IVM_prevent_immv_change");
	ivm_trigger->columns = NIL;
	ivm_trigger->transitionRels = NIL;
	ivm_trigger->whenClause = NULL;
	ivm_trigger->isconstraint = false;
	ivm_trigger->deferrable = false;
	ivm_trigger->initdeferred = false;
	ivm_trigger->constrrel = NULL;
	ivm_trigger->args = NIL;

	for (i = 0; i < 4; i++)
	{
		ivm_trigger->events = types[i];
		address = CreateTrigger(ivm_trigger, NULL, matviewOid, InvalidOid, InvalidOid,
							 InvalidOid, InvalidOid, InvalidOid, NULL, true, false);

		recordDependencyOn(&address, &refaddr, DEPENDENCY_AUTO);
	}

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}


/*
 * check_ivm_restriction --- look for specify nodes in the query tree
 */
static void
check_ivm_restriction(Node *node)
{
        check_ivm_restriction_context context = {false};

        check_ivm_restriction_walker(node, &context);
}

static bool
check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context)
{
        if (node == NULL)
                return false;

        /* This can recurse, so check for excessive recursion */
        check_stack_depth();

        switch (nodeTag(node))
        {
		case T_Query:
			{
				Query *qry = (Query *)node;
				ListCell   *lc;
				List       *vars;

				/* if contained CTE, return error */
				if (qry->cteList != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CTE is not supported on incrementally maintainable materialized view")));
				if (qry->havingQual != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg(" HAVING clause is not supported on incrementally maintainable materialized view")));
				if (qry->sortClause != NIL)	/* There is a possibility that we don't need to return an error */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ORDER BY clause is not supported on incrementally maintainable materialized view")));
				if (qry->limitOffset != NULL || qry->limitCount != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("LIMIT/OFFSET clause is not supported on incrementally maintainable materialized view")));
				if (qry->hasDistinctOn)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DISTINCT ON is not supported on incrementally maintainable materialized view")));
				if (qry->hasWindowFuncs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("window functions are not supported on incrementally maintainable materialized view")));
				if (qry->groupingSets != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("GROUPING SETS, ROLLUP, or CUBE clauses is not supported on incrementally maintainable materialized view")));
				if (qry->setOperations != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNION/INTERSECT/EXCEPT statements are not supported on incrementally maintainable materialized view")));
				if (list_length(qry->targetList) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("empty target list is not supported on incrementally maintainable materialized view")));
				if (qry->rowMarks != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("FOR UPDATE/SHARE clause is not supported on incrementally maintainable materialized view")));
				if (qry->hasSubLinks)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("subquery is not supported on incrementally maintainable materialized view")));

				/* system column restrictions */
				vars = pull_vars_of_level((Node *) qry, 0);
				foreach(lc, vars)
				{
					if (IsA(lfirst(lc), Var))
					{
						Var *var = (Var *) lfirst(lc);
						/* if system column, return error */
						if (var->varattno < 0)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("system column is not supported on incrementally maintainable materialized view")));
					}
				}

				context->has_agg |= qry->hasAggs;

				/* restrictions for rtable */
				foreach(lc, qry->rtable)
				{
					RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

					if (rte->tablesample != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("TABLESAMPLE clause is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_PARTITIONED_TABLE)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("partitioned table is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_RELATION && has_superclass(rte->relid))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("partitions is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_RELATION && find_inheritance_children(rte->relid, NoLock) != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("inheritance parent is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_FOREIGN_TABLE)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("foreign table is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_VIEW ||
						rte->relkind == RELKIND_MATVIEW)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("VIEW or MATERIALIZED VIEW is not supported on incrementally maintainable materialized view")));

					if (rte->rtekind == RTE_VALUES)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("VALUES is not supported on incrementally maintainable materialized view")));
					if (rte->rtekind == RTE_SUBQUERY)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("subquery is not supported on incrementally maintainable materialized view")));

				}

				query_tree_walker(qry, check_ivm_restriction_walker, (void *) context, QTW_IGNORE_RANGE_TABLE);

				break;
			}
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *)node;
				if (isIvmName(tle->resname))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("column name %s is not supported on incrementally maintainable materialized view", tle->resname)));
				if (context->has_agg && !IsA(tle->expr, Aggref) && contain_aggs_of_level((Node *) tle->expr, 0))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("expression containing an aggregate in it is not supported on incrementally maintainable materialized view")));

				expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
				break;
			}
		case T_JoinExpr:
			{
				JoinExpr *joinexpr = (JoinExpr *)node;

				if (joinexpr->jointype > JOIN_INNER)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("OUTER JOIN is not supported on incrementally maintainable materialized view")));

				expression_tree_walker(node, check_ivm_restriction_walker, NULL);
			}
			break;
		case T_Aggref:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("aggregate function is not supported on incrementally maintainable materialized view")));

				/* Check if this supports IVM */
/*
				Aggref *aggref = (Aggref *) node;
				const char *aggname = format_procedure(aggref->aggfnoid);

				if (aggref->aggfilter != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with FILTER clause is not supported on incrementally maintainable materialized view")));

				if (aggref->aggdistinct != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with DISTINCT arguments is not supported on incrementally maintainable materialized view")));

				if (aggref->aggorder != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with ORDER clause is not supported on incrementally maintainable materialized view")));

				if (!check_aggregate_supports_ivm(aggref->aggfnoid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function %s is not supported on incrementally maintainable materialized view", aggname)));
*/
				break;
			}
		default:
			expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
			break;
	}
	return false;
}

/*
 * CreateIndexOnIMMV
 *
 * Create a unique index on incremental maintainable materialized view.
 * If the view definition query has a GROUP BY clause, the index is created
 * on the columns of GROUP BY expressions. Otherwise, if the view contains
 * all primary key attritubes of its base tables in the target list, the index
 * is created on these attritubes. In other cases, no index is created.
 */
void
CreateIndexOnIMMV(Query *query, Relation matviewRel, bool is_create)
{
	ListCell *lc;
	IndexStmt  *index;
	ObjectAddress address;
	List *constraintList = NIL;
	char		idxname[NAMEDATALEN];
	List	   *indexoidlist = RelationGetIndexList(matviewRel);
	ListCell   *indexoidscan;

	snprintf(idxname, sizeof(idxname), "%s_index", RelationGetRelationName(matviewRel));

	index = makeNode(IndexStmt);

	/*
	 * We consider null values not distinct to make sure that views with DISTINCT
	 * or GROUP BY don't contain multiple NULL rows when NULL is inserted to
	 * a base table concurrently.
	 */
	//index->nulls_not_distinct = true;

	index->unique = true;
	index->primary = false;
	index->isconstraint = false;
	index->deferrable = false;
	index->initdeferred = false;
	index->idxname = idxname;
	index->relation =
		makeRangeVar(get_namespace_name(RelationGetNamespace(matviewRel)),
					 pstrdup(RelationGetRelationName(matviewRel)),
					 -1);
	index->accessMethod = DEFAULT_INDEX_TYPE;
	index->options = NIL;
	index->tableSpace = get_tablespace_name(matviewRel->rd_rel->reltablespace);
	index->whereClause = NULL;
	index->indexParams = NIL;
	index->indexIncludingParams = NIL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->oldCreateSubid = InvalidSubTransactionId;
	index->oldFirstRelfilenodeSubid = InvalidSubTransactionId;
	index->transformed = true;
	index->concurrent = false;
	index->if_not_exists = false;

	if (query->distinctClause)
	{
		/* create unique constraint on all columns */
		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);
			IndexElem  *iparam;

			iparam = makeNode(IndexElem);
			iparam->name = pstrdup(NameStr(attr->attname));
			iparam->expr = NULL;
			iparam->indexcolname = NULL;
			iparam->collation = NIL;
			iparam->opclass = NIL;
			iparam->opclassopts = NIL;
			iparam->ordering = SORTBY_DEFAULT;
			iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
			index->indexParams = lappend(index->indexParams, iparam);
		}
	}
	else
	{
		Bitmapset *key_attnos;

		/* create index on the base tables' primary key columns */
		key_attnos = get_primary_key_attnos_from_query(query, &constraintList, is_create);
		if (key_attnos)
		{
			foreach(lc, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);

				if (bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber, key_attnos))
				{
					IndexElem  *iparam;

					iparam = makeNode(IndexElem);
					iparam->name = pstrdup(NameStr(attr->attname));
					iparam->expr = NULL;
					iparam->indexcolname = NULL;
					iparam->collation = NIL;
					iparam->opclass = NIL;
					iparam->opclassopts = NIL;
					iparam->ordering = SORTBY_DEFAULT;
					iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
					index->indexParams = lappend(index->indexParams, iparam);
				}
			}
		}
		else
		{
			/* create no index, just notice that an appropriate index is necessary for efficient IVM */
			ereport(NOTICE,
					(errmsg("could not create an index on immv \"%s\" automatically",
							RelationGetRelationName(matviewRel)),
					 errdetail("This target list does not have all the primary key columns, "
							   "or this view does not contain DISTINCT clause."),
					 errhint("Create an index on the immv for efficient incremental maintenance.")));
			return;
		}
	}

	/* If we have a compatible index, we don't need to create another. */
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indexRel;
		bool		hasCompatibleIndex = false;

		indexRel = index_open(indexoid, AccessShareLock);

		if (CheckIndexCompatible(indexRel->rd_id,
								index->accessMethod,
								index->indexParams,
								index->excludeOpNames))
			hasCompatibleIndex = true;

		index_close(indexRel, AccessShareLock);

		if (hasCompatibleIndex)
			return;
	}

	address = DefineIndex(RelationGetRelid(matviewRel),
						  index,
						  InvalidOid,
						  InvalidOid,
						  InvalidOid,
						  false, true, false, false, true);

	ereport(NOTICE,
			(errmsg("created index \"%s\" on immv \"%s\"",
					idxname, RelationGetRelationName(matviewRel))));

	/*
	 * Make dependencies so that the index is dropped if any base tables's
	 * primary key is dropped.
	 */
	foreach(lc, constraintList)
	{
		Oid constraintOid = lfirst_oid(lc);
		ObjectAddress	refaddr;

		refaddr.classId = ConstraintRelationId;
		refaddr.objectId = constraintOid;
		refaddr.objectSubId = 0;

		recordDependencyOn(&address, &refaddr, DEPENDENCY_NORMAL);
	}
}


/*
 * get_primary_key_attnos_from_query
 *
 * Identify the columns in base tables' primary keys in the target list.
 *
 * Returns a Bitmapset of the column attnos of the primary key's columns of
 * tables that used in the query.  The attnos are offset by
 * FirstLowInvalidHeapAttributeNumber as same as get_primary_key_attnos.
 *
 * If any table has no primary key or any primary key's columns is not in
 * the target list, return NULL.  We also return NULL if any pkey constraint
 * is deferrable.
 *
 * constraintList is set to a list of the OIDs of the pkey constraints.
 */
static Bitmapset *
get_primary_key_attnos_from_query(Query *query, List **constraintList, bool is_create)
{
	List *key_attnos_list = NIL;
	ListCell *lc;
	int i;
	Bitmapset *keys = NULL;
	Relids	rels_in_from;
	PlannerInfo root;


	/*
	 * Collect primary key attributes from all tables used in query. The key attributes
	 * sets for each table are stored in key_attnos_list in order by RTE index.
	 */
	i = 1;
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);
		Bitmapset *key_attnos;
		bool	has_pkey = true;
		Index	first_rtindex = is_create ? 1 : PRS2_NEW_VARNO + 1;

		/* skip NEW/OLD entries */
		if (i >= first_rtindex)
		{
			/* for tables, call get_primary_key_attnos */
			if (r->rtekind == RTE_RELATION)
			{
				Oid constraintOid;
				key_attnos = get_primary_key_attnos(r->relid, false, &constraintOid);
				*constraintList = lappend_oid(*constraintList, constraintOid);
				has_pkey = (key_attnos != NULL);
			}
			/* for other RTEs, store NULL into key_attnos_list */
			else
				key_attnos = NULL;
		}
		else
			key_attnos = NULL;

		/*
		 * If any table or subquery has no primary key or its pkey constraint is deferrable,
		 * we cannot get key attributes for this query, so return NULL.
		 */
		if (!has_pkey)
			return NULL;

		key_attnos_list = lappend(key_attnos_list, key_attnos);
		i++;
	}

	/* Collect key attributes appearing in the target list */
	i = 1;
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) flatten_join_alias_vars(query, lfirst(lc));

		if (IsA(tle->expr, Var))
		{
			Var *var = (Var*) tle->expr;
			Bitmapset *attnos = list_nth(key_attnos_list, var->varno - 1);

			/* check if this attribute is from a base table's primary key */
			if (bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber, attnos))
			{
				/*
				 * Remove found key attributes from key_attnos_list, and add this
				 * to the result list.
				 */
				bms_del_member(attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
				keys = bms_add_member(keys, i - FirstLowInvalidHeapAttributeNumber);
			}
		}
		i++;
	}

	/* Collect relations appearing in the FROM clause */
	rels_in_from = pull_varnos_of_level(&root, (Node *)query->jointree, 0);

	/*
	 * Check if all key attributes of relations in FROM are appearing in the target
	 * list.  If an attribute remains in key_attnos_list in spite of the table is used
	 * in FROM clause, the target is missing this key attribute, so we return NULL.
	 */
	i = 1;
	foreach(lc, key_attnos_list)
	{
		Bitmapset *bms = (Bitmapset *)lfirst(lc);
		if (!bms_is_empty(bms) && bms_is_member(i, rels_in_from))
			return NULL;
		i++;
	}

	return keys;
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

/*
 * IVM_immediate_before
 *
 * IVM trigger function invoked before base table is modified. If this is
 * invoked firstly in the same statement, we save the transaction id and the
 * command id at that time.
 */
PG_FUNCTION_INFO_V1(IVM_immediate_before);

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

PG_FUNCTION_INFO_V1(IVM_immediate_maintenance);

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
		Tuplestorestate *old = NULL;
		TupleDesc tupdesc = RelationGetDescr(rel);
		TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		tuplestore_rescan(trigdata->tg_oldtable);
		old = tuplestore_begin_heap(false, false, work_mem);
		while (tuplestore_gettupleslot(trigdata->tg_oldtable, true, false, slot))
			tuplestore_puttupleslot(old, slot);
		ExecDropSingleTupleTableSlot(slot);

		table->old_tuplestores = lappend(table->old_tuplestores, old);
		entry->has_old = true;

		MemoryContextSwitchTo(oldcxt);
	}
	if (trigdata->tg_newtable)
	{
		Tuplestorestate *new = NULL;
		TupleDesc tupdesc = RelationGetDescr(rel);
		TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		tuplestore_rescan(trigdata->tg_newtable);
		new = tuplestore_begin_heap(false, false, work_mem);
		while (tuplestore_gettupleslot(trigdata->tg_newtable, true, false, slot))
			tuplestore_puttupleslot(new, slot);
		ExecDropSingleTupleTableSlot(slot);

		table->new_tuplestores = lappend(table->new_tuplestores, new);
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
	CheckTableNotInUse(matviewRel, "refresh a materialized view incrementally");

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

			/* apply the delta tables to the materialized view */
			apply_delta(matviewOid, old_tuplestore, new_tuplestore,
						tupdesc_old, tupdesc_new, query, use_count,
						count_colname);

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
					") "			        /* delete a tuple if this is to be deleted */
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
 * generate_equals
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

static void
IvmXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT)
		AtAbort_IVM();
}

static void
IvmSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
		AtAbort_IVM();
}

PG_FUNCTION_INFO_V1(IVM_prevent_immv_change);

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

Datum
IVM_prevent_immv_change(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel = trigdata->tg_relation;

	if (!ImmvIncrementalMaintenanceIsEnabled())
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot change materialized view \"%s\"",
						RelationGetRelationName(rel))));

	return PointerGetDatum(NULL);
}

void
_PG_init(void)
{
	RegisterXactCallback(IvmXactCallback, NULL);
	RegisterSubXactCallback(IvmSubXactCallback, NULL);
}
