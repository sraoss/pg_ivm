/*-------------------------------------------------------------------------
 *
 * createas.c
 *	  incremental view maintenance extension
 *    Routines for creating IMMVs
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger_d.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "pg_ivm.h"

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


typedef struct
{
	bool	has_agg;
} check_ivm_restriction_context;

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock);
static void check_ivm_restriction(Node *node);
static bool check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context);
static Bitmapset *get_primary_key_attnos_from_query(Query *query, List **constraintList, bool is_create);

static void StoreImmvQuery(Oid viewOid, bool ispopulated, Query *viewQuery);

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM < 140000)
static bool CreateTableAsRelExists(CreateTableAsStmt *ctas);
#endif

/*
 * ExecCreateImmv -- execute a create_immv() function
 *
 * This imitates PostgreSQL's ExecCreateTableAs().
 */
ObjectAddress
ExecCreateImmv(ParseState *pstate, CreateTableAsStmt *stmt,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *viewQuery = (Query *) into->viewQuery;

	/*
	 * We use this always true flag to imitate ExecCreaetTableAs(9
	 * aiming to make it easier to follow up the original code.
	 */
	const bool	is_ivm = true;

	/* must be a CREATE MATERIALIZED VIEW statement */
	Assert(is_matview);

	/*
	 * Set into->viewQuery must to NULL because we want to  make a
	 * table instead of a materialized view. Before that, save the
	 * view query.
	 */
	viewQuery = (Query *) into->viewQuery;
	into->viewQuery = NULL;

	/* Check if the relation exists or not */
	if (CreateTableAsRelExists(stmt))
		return InvalidObjectAddress;

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	dest = CreateIntoRelDestReceiver(into);

	/*
	 * The contained Query must be a SELECT.
	 */
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	if (is_matview && is_ivm)
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

	if (into->skipData)
	{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */

		/* XXX: Currently, WITH NO DATA is not supported in the extension version */
		//address = create_ctas_nodata(query->targetList, into);
	}
	else
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
			elog(ERROR, "unexpected rewrite result for %s",
				 is_matview ? "CREATE MATERIALIZED VIEW" :
				 "CREATE TABLE AS SELECT");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							 CURSOR_OPT_PARALLEL_OK, params);

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
									dest, params, queryEnv, 0);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount if we're given a qc to fill */
		if (qc)
			SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->estate->es_processed);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}

	/* Create the "view" part of an IMMV. */
	StoreImmvQuery(address.objectId, !into->skipData, viewQuery);

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		if (is_ivm)
		{
			Oid matviewOid = address.objectId;
			Relation matviewRel = table_open(matviewOid, NoLock);

			if (!into->skipData)
			{
				/* Create an index on incremental maintainable materialized view, if possible */
				CreateIndexOnIMMV(viewQuery, matviewRel, true);

				/* Create triggers on incremental maintainable materialized view */
				CreateIvmTriggersOnBaseTables(viewQuery, matviewOid, true);

				/* Create triggers to prevent IMMV from beeing changed */
				CreateChangePreventTrigger(matviewOid);
			}
			table_close(matviewRel, NoLock);
		}
	}

	return address;
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

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
#endif
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
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate function is not supported on incrementally maintainable materialized view")));
			break;
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

	/* XXX: nulls_not_distinct is available in PG15 or later */
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
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	PlannerInfo root;
#endif

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
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	rels_in_from = pull_varnos_of_level(&root, (Node *)query->jointree, 0);
#else
	rels_in_from = pull_varnos_of_level((Node *)query->jointree, 0);
#endif
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
 * Store the query for the IMMV to pg_ivwm_immv
 */
static void
StoreImmvQuery(Oid viewOid, bool ispopulated, Query *viewQuery)
{
	char   *querytree = nodeToString((Node *) viewQuery);
	Datum values[Natts_pg_ivm_immv];
	bool isNulls[Natts_pg_ivm_immv];
	Relation pgIvmImmv;
   	TupleDesc tupleDescriptor;
   	HeapTuple heapTuple;
	ObjectAddress	address;

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_ivm_immv_immvrelid -1 ] = ObjectIdGetDatum(viewOid);
	values[Anum_pg_ivm_immv_ispopulated -1 ] = BoolGetDatum(ispopulated);
	values[Anum_pg_ivm_immv_viewdef -1 ] = CStringGetTextDatum(querytree);

	pgIvmImmv = table_open(PgIvmImmvRelationId(), RowExclusiveLock);

   	tupleDescriptor = RelationGetDescr(pgIvmImmv);
   	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

   	CatalogTupleInsert(pgIvmImmv, heapTuple);

	address.classId = RelationRelationId;
	address.objectId = viewOid;
	address.objectSubId = 0;

	recordDependencyOnExpr(&address, (Node *) viewQuery, NIL,
						   DEPENDENCY_NORMAL);

	table_close(pgIvmImmv, NoLock);

	CommandCounterIncrement();
}

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM < 140000)
/*
 * CreateTableAsRelExists --- check existence of relation for CreateTableAsStmt
 *
 * Utility wrapper checking if the relation pending for creation in this
 * CreateTableAsStmt query already exists or not.  Returns true if the
 * relation exists, otherwise false.
 */
static bool
CreateTableAsRelExists(CreateTableAsStmt *ctas)
{
	Oid			nspid;
	IntoClause *into = ctas->into;

	nspid = RangeVarGetCreationNamespace(into->rel);

	if (get_relname_relid(into->rel->relname, nspid))
	{
		if (!ctas->if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists",
							into->rel->relname)));

		/* The relation exists and IF NOT EXISTS has been specified */
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists, skipping",
						into->rel->relname)));
		return true;
	}

	/* Relation does not exist, it can be created */
	return false;
}
#endif
