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
#include "optimizer/prep.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

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
	bool	has_subquery;
	bool    in_exists_subquery; /* true, if it is in a exists subquery */
	List    *exists_qual_vars;
	int		sublevels_up;
} check_ivm_restriction_context;

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock);
static void check_ivm_restriction(Node *node);
static bool check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context);
static Bitmapset *get_primary_key_attnos_from_query(Query *query, List **constraintList, bool is_create);
static bool check_aggregate_supports_ivm(Oid aggfnoid);

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
		query = rewriteQueryForIMMV(viewQuery, into->colNames);

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
				CreateIvmTriggersOnBaseTables(query, matviewOid, true);

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
 * Also, additional hidden columns are added for aggregate values.
 *
 * EXISTS sublink is rewritten to LATERAL subquery with HAVING
 * clause to check count(*) > 0. In addition, a counting column
 * referring to count(*) in this subquery is added to the original
 * target list.
 */
Query *
rewriteQueryForIMMV(Query *query, List *colNames)
{
	Query *rewritten;

	TargetEntry *tle;
	Node *node;
	ParseState *pstate = make_parsestate(NULL);
	FuncCall *fn;

	/*
	 * Check the length of colunm name list not to override names of
	 * additional columns
	 */
	if (list_length(colNames) > list_length(query->targetList))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("too many column names were specified")));

	rewritten = copyObject(query);
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * If this query has EXISTS clause, rewrite query and
	 * add __ivm_exists_count_X__ column.
	 */
	if (rewritten->hasSubLinks)
	{
		ListCell *lc;
		RangeTblEntry *rte;
		int varno = 0;

		/* rewrite EXISTS sublink to LATERAL subquery */
		rewrite_query_for_exists_subquery(rewritten);

		/* Add counting column referring to count(*) in EXISTS clause */
		foreach(lc, rewritten->rtable)
		{
			char *columnName;
			int attnum;
			Node *countCol = NULL;
			varno++;

			rte = (RangeTblEntry *) lfirst(lc);
			if (!rte->subquery || !rte->lateral)
				continue;
			pstate->p_rtable = rewritten->rtable;

			columnName = getColumnNameStartWith(rte, "__ivm_exists", &attnum);
			if (columnName == NULL)
				continue;
			countCol = (Node *) makeVar(varno, attnum,
						INT8OID, -1, InvalidOid, 0);

			if (countCol != NULL)
			{
				tle = makeTargetEntry((Expr *) countCol,
											list_length(rewritten->targetList) + 1,
											pstrdup(columnName),
											false);
				rewritten->targetList = list_concat(rewritten->targetList, list_make1(tle));
			}
		}
	}


	/* group keys must be in targetlist */
	if (rewritten->groupClause)
	{
		ListCell *lc;
		foreach(lc, rewritten->groupClause)
		{
			SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(scl, rewritten->targetList);

			if (tle->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("GROUP BY expression not appearing in select list is not supported on incrementally maintainable materialized view")));
		}
	}
	/* Convert DISTINCT to GROUP BY.  count(*) will be added afterward. */
	else if (!rewritten->hasAggs && rewritten->distinctClause)
		rewritten->groupClause = transformDistinctClause(NULL, &rewritten->targetList, rewritten->sortClause, false);

	/* Add additional columns for aggregate values */
	if (rewritten->hasAggs)
	{
		ListCell *lc;
		List *aggs = NIL;
		AttrNumber next_resno = list_length(rewritten->targetList) + 1;

		foreach(lc, rewritten->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			char *resname = (colNames == NIL || foreach_current_index(lc) >= list_length(colNames) ?
								tle->resname : strVal(list_nth(colNames, tle->resno - 1)));

			if (IsA(tle->expr, Aggref))
				makeIvmAggColumn(pstate, (Aggref *) tle->expr, resname, &next_resno, &aggs);
		}
		rewritten->targetList = list_concat(rewritten->targetList, aggs);
	}

	/* Add count(*) for counting distinct tuples in views */
	if (rewritten->distinctClause || rewritten->hasAggs)
	{
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
 * makeIvmAggColumn -- make additional aggregate columns for IVM
 *
 * For an aggregate column specified by aggref, additional aggregate columns
 * are added, which are used to calculate the new aggregate value in IMMV.
 * An additional aggregate columns has a name based on resname
 * (ex. ivm_count_resname), and resno specified by next_resno. The created
 * columns are returned to aggs, and the resno for the next column is also
 * returned to next_resno.
 *
 * Currently, an additional count() is created for aggref other than count.
 * In addition, sum() is created for avg aggregate column.
 */
void
makeIvmAggColumn(ParseState *pstate, Aggref *aggref, char *resname, AttrNumber *next_resno, List **aggs)
{
	TargetEntry *tle_count;
	Node *node;
	FuncCall *fn;
	Const	*dmy_arg = makeConst(INT4OID,
								 -1,
								 InvalidOid,
								 sizeof(int32),
								 Int32GetDatum(1),
								 false,
								 true); /* pass by value */
	const char *aggname = get_func_name(aggref->aggfnoid);

	/*
	 * For aggregate functions except count, add count() func with the same arg parameters.
	 * This count result is used for determining if the aggregate value should be NULL or not.
	 * Also, add sum() func for avg because we need to calculate an average value as sum/count.
	 *
	 * XXX: If there are same expressions explicitly in the target list, we can use this instead
	 * of adding new duplicated one.
	 */
	if (strcmp(aggname, "count") != 0)
	{
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		fn = makeFuncCall(list_make1(makeString("count")), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(list_make1(makeString("count")), NIL, -1);
#endif

		/* Make a Func with a dummy arg, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, list_make1(dmy_arg), NULL, fn, false, -1);
		((Aggref *)node)->args = aggref->args;

		tle_count = makeTargetEntry((Expr *) node,
									*next_resno,
									pstrdup(makeObjectName("__ivm_count",resname, "_")),
									false);
		*aggs = lappend(*aggs, tle_count);
		(*next_resno)++;
	}
	if (strcmp(aggname, "avg") == 0)
	{
		List *dmy_args = NIL;
		ListCell *lc;
		foreach(lc, aggref->aggargtypes)
		{
			Oid		typeid = lfirst_oid(lc);
			Type	type = typeidType(typeid);

			Const *con = makeConst(typeid,
								   -1,
								   typeTypeCollation(type),
								   typeLen(type),
								   (Datum) 0,
								   true,
								   typeByVal(type));
			dmy_args = lappend(dmy_args, con);
			ReleaseSysCache(type);
		}
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		fn = makeFuncCall(list_make1(makeString("sum")), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(list_make1(makeString("sum")), NIL, -1);
#endif

		/* Make a Func with dummy args, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, dmy_args, NULL, fn, false, -1);
		((Aggref *)node)->args = aggref->args;

		tle_count = makeTargetEntry((Expr *) node,
									*next_resno,
									pstrdup(makeObjectName("__ivm_sum",resname, "_")),
									false);
		*aggs = lappend(*aggs, tle_count);
		(*next_resno)++;
	}
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

	/*
	 * is_create must be true in pg_ivm because the view definition doesn't
	 * contain NEW/OLD RTE.
	 * XXX: This argument should be removed?
	 */
	Assert(is_create);

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
	 * XXX: For the current extension version, DISTINCT and aggregates with GROUP
	 * need exclusive lock to prevent inconsistency that can be avoided by using
	 * nulls_not_distinct which is available only in PG15 or later.
	 * XXX: This lock is not necessary if all columns in group keys or distinct
	 * target list are not nullable.
	 */

	rte = list_nth(qry->rtable, first_rtindex - 1);
	if (list_length(qry->rtable) > first_rtindex ||
		rte->rtekind != RTE_RELATION || qry->distinctClause ||
		(qry->hasAggs && qry->groupClause))
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
				ListCell *lc;

				CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)query->jointree, matviewOid, relids, ex_lock);
				foreach(lc, query->cteList)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
					Assert(IsA(cte->ctequery, Query));
					CreateIvmTriggersOnBaseTablesRecurse((Query *) cte->ctequery, cte->ctequery, matviewOid, relids, ex_lock);
				}
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
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_BEFORE, true);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_AFTER, true);

					*relids = bms_add_member(*relids, rte->relid);
				}
				else if (rte->rtekind == RTE_SUBQUERY)
				{
					Query *subquery = rte->subquery;
					Assert(rte->subquery != NULL);
					CreateIvmTriggersOnBaseTablesRecurse(subquery, (Node *)subquery, matviewOid, relids, ex_lock);
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
		case TRIGGER_TYPE_TRUNCATE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_truncate_before" : "IVM_trigger_truncate_after");
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

	/*
	 * XXX: When using DELETE or UPDATE, we must use exclusive lock for now
	 * because apply_old_delta(_with_count) doesn't work in concurrent situations.
	 *
	 * If the view doesn't have aggregate, distinct, or tuple duplicate, then it
	 * would work. However, we don't have any way to guarantee the view has a unique
	 * key before opening the IMMV at the maintenance time because users may drop
	 * the unique index. We need something to resolve the issue!!
	 */
	if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		ex_lock = true;

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
	check_ivm_restriction_context context = {false, false, false, NIL, 0};

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

				if (qry->groupClause != NIL && !qry->hasAggs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("GROUP BY clause without aggregate is not supported on incrementally maintainable materialized view")));
				if (qry->havingQual != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("HAVING clause is not supported on incrementally maintainable materialized view")));
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
				if (qry->hasRecursive)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("recursive query is not supported on incrementally maintainable materialized view")));

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
				/* subquery restrictions */
				if (context->sublevels_up > 0 && qry->distinctClause != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DISTINCT clause in nested query are not supported on incrementally maintainable materialized view")));
				if (context->sublevels_up > 0 && qry->hasAggs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate functions in nested query are not supported on incrementally maintainable materialized view")));

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
					if (rte->relkind == RELKIND_RELATION && isImmv(rte->relid))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("including IMMV in definition is not supported on incrementally maintainable materialized view")));

					if (rte->rtekind == RTE_SUBQUERY)
					{
						context->has_subquery = true;

						context->sublevels_up++;
						check_ivm_restriction_walker((Node *)rte->subquery, context);
						context->sublevels_up--;
					}
				}

				query_tree_walker(qry, check_ivm_restriction_walker, (void *) context, QTW_IGNORE_RT_SUBQUERIES);

				/*
				 * additional restriction checks for exists subquery
				 *
				 * When contain EXISTS clauses, and it has a column refernces
				 * a table outside, its column must be included by target list.
				 */
				if (context->exists_qual_vars != NIL && context->sublevels_up == 0)
				{
					ListCell *lc;

					foreach (lc, context->exists_qual_vars)
					{
						Var	*var = (Var *) lfirst(lc);
						ListCell *lc2;
						bool found = false;

						foreach(lc2, qry->targetList)
						{
							TargetEntry	*tle = lfirst(lc2);
							Var *var2;

							if (!IsA(tle->expr, Var))
								continue;
							var2 = (Var *) tle->expr;
							if (var->varno == var2->varno && var->varattno == var2->varattno)
							{
								found = true;
								break;
							}
						}
						if (!found)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("targetlist must contain vars that are referred to in EXISTS subquery")));
					}
				}

				break;
			}
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;

				if (isIvmName(cte->ctename))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH query name %s is not supported on incrementally maintainable materialized view", cte->ctename)));

				/* 
				 * When a table in a unreferenced CTE is TRUNCATEd, the contents of the
				 * IMMV is not affected so it must not be truncated. For confirming it
				 * at the maintenance time, we have to check if the modified table used
				 * in a CTE is actually referenced. Although it would be possible, we
				 * just disallow to create such IMMVs for now since such unreferenced
				 * CTE is useless unless it doesn't contain modifying commands, that is
				 * already prohibited.
				 */
				if (cte->cterefcount == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("Ureferenced WITH query is not supported on incrementally maintainable materialized view")));

				context->sublevels_up++;
				check_ivm_restriction_walker(cte->ctequery, (void *) context);
				context->sublevels_up--;
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

				expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
				break;
			}
		case T_Aggref:
			{
				/* Check if this supports IVM */
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

				expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
				break;
			}
		case T_Var:
			{
				Var	*variable = (Var *) node;
				/* Currently, only EXISTS clause is allowed here.
				 * If EXISTS subquery refers to vars of the upper query, collect these vars.
				 */
				if (variable->varlevelsup > 0 && context->in_exists_subquery)
					context->exists_qual_vars = lappend(context->exists_qual_vars, node);
				break;
			}
		case T_SubLink:
			{
				/* Now, EXISTS clause is supported only */
				Query *subselect;
				SubLink	*sublink = (SubLink *) node;
				if (sublink->subLinkType != EXISTS_SUBLINK)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("this query is not allowed on incrementally maintainable materialized view"),
							 errhint("subquery in WHERE clause only supports subquery with EXISTS clause")));
				if (context->sublevels_up > 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("nested subquery is not supported on incrementally maintainable materialized view")));

				subselect = (Query *)sublink->subselect;
				/* raise ERROR if the sublink has CTE */
				if (subselect->cteList)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CTE in EXIST clause is not supported on incrementally maintainable materialized view")));

				context->in_exists_subquery = true;
				context->sublevels_up++;
				check_ivm_restriction_walker(sublink->subselect, context);
				context->sublevels_up--;
				context->in_exists_subquery = false;
				break;
			}
		default:
			expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
			break;
	}
	return false;
}

/*
 * check_aggregate_supports_ivm
 *
 * Check if the given aggregate function is supporting IVM
 */
static bool
check_aggregate_supports_ivm(Oid aggfnoid)
{
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
	switch (aggfnoid)
	{
		/* count */
		case F_COUNT_ANY:
		case F_COUNT_:

		/* sum */
		case F_SUM_INT8:
		case F_SUM_INT4:
		case F_SUM_INT2:
		case F_SUM_FLOAT4:
		case F_SUM_FLOAT8:
		case F_SUM_MONEY:
		case F_SUM_INTERVAL:
		case F_SUM_NUMERIC:

		/* avg */
		case F_AVG_INT8:
		case F_AVG_INT4:
		case F_AVG_INT2:
		case F_AVG_NUMERIC:
		case F_AVG_FLOAT4:
		case F_AVG_FLOAT8:
		case F_AVG_INTERVAL:

		/* min */
		case F_MIN_ANYARRAY:
		case F_MIN_INT8:
		case F_MIN_INT4:
		case F_MIN_INT2:
		case F_MIN_OID:
		case F_MIN_FLOAT4:
		case F_MIN_FLOAT8:
		case F_MIN_DATE:
		case F_MIN_TIME:
		case F_MIN_TIMETZ:
		case F_MIN_MONEY:
		case F_MIN_TIMESTAMP:
		case F_MIN_TIMESTAMPTZ:
		case F_MIN_INTERVAL:
		case F_MIN_TEXT:
		case F_MIN_NUMERIC:
		case F_MIN_BPCHAR:
		case F_MIN_TID:
		case F_MIN_ANYENUM:
		case F_MIN_INET:
		case F_MIN_PG_LSN:

		/* max */
		case F_MAX_ANYARRAY:
		case F_MAX_INT8:
		case F_MAX_INT4:
		case F_MAX_INT2:
		case F_MAX_OID:
		case F_MAX_FLOAT4:
		case F_MAX_FLOAT8:
		case F_MAX_DATE:
		case F_MAX_TIME:
		case F_MAX_TIMETZ:
		case F_MAX_MONEY:
		case F_MAX_TIMESTAMP:
		case F_MAX_TIMESTAMPTZ:
		case F_MAX_INTERVAL:
		case F_MAX_TEXT:
		case F_MAX_NUMERIC:
		case F_MAX_BPCHAR:
		case F_MAX_TID:
		case F_MAX_ANYENUM:
		case F_MAX_INET:
		case F_MAX_PG_LSN:
			return true;

		default:
			return false;
	}

#else
	char *funcs[] = {
		/* count */
		"count(\"any\")",
		"count()",

		/* sum */
		"sum(int8)",
		"sum(int4)",
		"sum(int2)",
		"sum(float4)",
		"sum(float8)",
		"sum(money)",
		"sum(interval)",
		"sum(numeric)",

		/* avg */
		"avg(int8)",
		"avg(int4)",
		"avg(int2)",
		"avg(numeric)",
		"avg(float4)",
		"avg(float8)",
		"avg(interval)",

		/* min */
		"min(anyarray)",
		"min(int8)",
		"min(int4)",
		"min(int2)",
		"min(oid)",
		"min(float4)",
		"min(float8)",
		"min(date)",
		"min(time without time zone)",
		"min(time with time zone)",
		"min(money)",
		"min(timestamp without time zone)",
		"min(timestamp with time zone)",
		"min(interval)",
		"min(text)",
		"min(numeric)",
		"min(character)",
		"min(tid)",
		"min(anyenum)",
		"min(inet)",
		"min(pg_lsn)",

		/* max */
		"max(anyarray)",
		"max(int8)",
		"max(int4)",
		"max(int2)",
		"max(oid)",
		"max(float4)",
		"max(float8)",
		"max(date)",
		"max(time without time zone)",
		"max(time with time zone)",
		"max(money)",
		"max(timestamp without time zone)",
		"max(timestamp with time zone)",
		"max(interval)",
		"max(text)",
		"max(numeric)",
		"max(character)",
		"max(tid)",
		"max(anyenum)",
		"max(inet)",
		"max(pg_lsn)",

		NULL
	};

	char **fname = funcs;

	while (*fname != NULL)
	{
		if (DatumGetObjectId(DirectFunctionCall1(to_regprocedure, CStringGetTextDatum(*fname))) == aggfnoid)
			return true;
		fname++;
	}

	return false;

#endif
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


	/*
	 * is_create must be true in pg_ivm because the view definition doesn't
	 * contain NEW/OLD RTE.
	 * XXX: This argument should be removed?
	 */
	Assert(is_create);

	/*
	 * For aggregate without GROUP BY, we do not need to create an index
	 * because the view has only one row.
	 */
	if (query->hasAggs && query->groupClause == NIL)
		return;

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

	if (query->groupClause)
	{
		/* create unique constraint on GROUP BY expression columns */
		foreach(lc, query->groupClause)
		{
			SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(scl, query->targetList);
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
	else if (query->distinctClause)
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
							   "or this view does not contain GROUP BY or DISTINCT clause."),
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

	/* convert CTEs to subqueries */
	query = copyObject(query);
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
			/* for subqueries, scan recursively */
			if (r->rtekind == RTE_SUBQUERY)
			{
				key_attnos = get_primary_key_attnos_from_query(r->subquery, constraintList, true);
				has_pkey = (key_attnos != NULL);
			}
			/* for tables, call get_primary_key_attnos */
			else if (r->rtekind == RTE_RELATION)
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
			Bitmapset *key_attnos = list_nth(key_attnos_list, var->varno - 1);

			/* check if this attribute is from a base table's primary key */
			if (bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber, key_attnos))
			{
				/*
				 * Remove found key attributes from key_attnos_list, and add this
				 * to the result list.
				 */
				bms_del_member(key_attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
				keys = bms_add_member(keys, i - FirstLowInvalidHeapAttributeNumber);
			}
		}
		i++;
	}

	/* Collect RTE indexes of relations appearing in the FROM clause */
	rels_in_from = get_relids_in_jointree((Node *) query->jointree, false);

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
