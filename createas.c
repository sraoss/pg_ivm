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

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger_d.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/prep.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
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

/* utility functions for IMMV definition creation */
static ImmvAddress create_immv_internal(List *attrList, IntoClause *into);
static ImmvAddress create_immv_nodata(List *tlist, IntoClause *into);

typedef struct
{
	bool	has_agg;			/* the query has an aggregate */
	bool	allow_exists;		/* EXISTS subquery is allowed in the current node */
	bool    in_exists_subquery;	/* true, if under an EXISTS subquery */
	List    *exists_qual_vars;	/* Vars used in EXISTS subqueries */
	int		sublevels_up;		/* (current) nesting depth */
} check_ivm_restriction_context;

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, ImmvAddress immv_addr,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, ImmvAddress immv_addr, int16 type, int16 timing, bool ex_lock);
static void check_ivm_restriction(Node *node);
static bool check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context);
static Bitmapset *get_primary_key_attnos_from_query(Query *query, List **constraintList);
static bool check_aggregate_supports_ivm(Oid aggfnoid);

static void StoreImmvQuery(ImmvAddress immv_addr, Query *viewQuery);

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM < 140000)
static bool CreateTableAsRelExists(CreateTableAsStmt *ctas);
#endif

/*
 * create_immv_internal
 *
 * Internal utility used for the creation of the definition of an IMMV.
 * Caller needs to provide a list of attributes (ColumnDef nodes).
 *
 * This imitates PostgreSQL's create_ctas_internal().
 */
static ImmvAddress
create_immv_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	char		relkind;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ImmvAddress	immv_addr;
	pg_uuid_t	*immv_uuid;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	/* relkind of IMMV must be RELKIND_RELATION */
	relkind = RELKIND_RELATION;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = into->rel;
	create->tableElts = attrList;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;
	create->accessMethod = into->accessMethod;

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 */
	immv_addr.address = DefineRelation(create, relkind, InvalidOid, NULL, NULL);
	/*
	 * Generate the IMMV UUID.
	 * TODO: check for hash collision
	 */
	immv_uuid = DatumGetUUIDP(DirectFunctionCall1(gen_random_uuid, (Datum) NULL));
	memcpy(&immv_addr.immv_uuid, immv_uuid, sizeof(*immv_uuid));
	pfree(immv_uuid);

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(immv_addr.address.objectId, toast_options);

	/* Create the "view" part of an IMMV. */
	StoreImmvQuery(immv_addr, (Query *) into->viewQuery);
	CommandCounterIncrement();

	return immv_addr;
}

/*
 * create_immv_nodata
 *
 * Create an IMMV  when WITH NO DATA is used, starting from
 * the targetlist of the view definition.
 *
 * This imitates PostgreSQL's create_ctas_nodata().
 */
static ImmvAddress
create_immv_nodata(List *tlist, IntoClause *into)
{
	List	   *attrList;
	ListCell   *t,
			   *lc;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE TABLE AS, override the column
	 * names in the query.  (Too few column names are OK, too many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	foreach(t, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef  *col;
			char	   *colname;

			if (lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(into->colNames, lc);
			}
			else
				colname = tle->resname;

			col = makeColumnDef(colname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));

			/*
			 * It's possible that the column is of a collatable type but the
			 * collation could not be resolved, so double-check.  (We must
			 * check this here because DefineRelation would adopt the type's
			 * default collation rather than complaining.)
			 */
			if (!OidIsValid(col->collOid) &&
				type_is_collatable(col->typeName->typeOid))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("no collation was derived for column \"%s\" with collatable type %s",
								col->colname,
								format_type_be(col->typeName->typeOid)),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));

			attrList = lappend(attrList, col);
		}
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Create the relation definition using the ColumnDef list */
	return create_immv_internal(attrList, into);
}


/*
 * ExecCreateImmv -- execute a create_immv() function
 *
 * This imitates PostgreSQL's ExecCreateTableAs().
 */
ObjectAddress
ExecCreateImmv(ParseState *pstate, CreateTableAsStmt *stmt,
				  QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool		do_refresh = false;
	ImmvAddress	immv_addr;

	/* Check if the relation exists or not */
	if (CreateTableAsRelExists(stmt))
		return InvalidObjectAddress;

	/*
	 * The contained Query must be a SELECT.
	 */
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, always skip data during table creation, and use
	 * REFRESH instead (see below).
	 */
	do_refresh = !into->skipData;

	/* check if the query is supported in IMMV definition */
	if (contain_mutable_functions((Node *) query))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("mutable function is not supported on incrementally maintainable materialized view"),
				 errhint("functions must be marked IMMUTABLE")));

	check_ivm_restriction((Node *) query);

	/* For IMMV, we need to rewrite matview query */
	query = rewriteQueryForIMMV(query, into->colNames);

	/*
	 * If WITH NO DATA was specified, do not go through the rewriter,
	 * planner and executor.  Just define the relation using a code path
	 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
	 * from running the planner before all dependencies are set up.
	 */
	immv_addr = create_immv_nodata(query->targetList, into);

	/*
	 * For materialized views, reuse the REFRESH logic, which locks down
	 * security-restricted operations and restricts the search_path.  This
	 * reduces the chance that a subsequent refresh will fail.
	 */
	if (do_refresh)
	{
		Relation matviewRel;

		RefreshImmvByOid(immv_addr, true, false, pstate->p_sourcetext, qc);

		if (qc)
			qc->commandTag = CMDTAG_SELECT;

		matviewRel = table_open(immv_addr.address.objectId, NoLock);

		/* Create an index on incremental maintainable materialized view, if possible */
		CreateIndexOnIMMV(query, matviewRel);

		/* Create triggers to prevent IMMV from being changed */
		CreateChangePreventTrigger(immv_addr.address.objectId);

		table_close(matviewRel, NoLock);

		if (IsolationUsesXactSnapshot())
			ereport(WARNING,
					(errmsg("inconsistent view can be created in isolation level SERIALIZABLE or REPEATABLE READ"),
					 errdetail("The view may not include effects of a concurrent transaction."),
					 errhint("create_immv should be used in isolation level READ COMMITTED, "
							 "or execute refresh_immv to make sure the view is consistent.")));
	}

	return immv_addr.address;
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

	Node *node;
	ParseState *pstate = make_parsestate(NULL);
	FuncCall *fn;

	/*
	 * Check the length of column name list not to override names of
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
				TargetEntry *tle = makeTargetEntry((Expr *) countCol,
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
		TargetEntry *tle;

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
		fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(SystemFuncName("count"), NIL, -1);
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
		fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(SystemFuncName("count"), NIL, -1);
#endif

		/* Make a Func with a dummy arg, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, list_make1(dmy_arg), NULL, fn, false, -1);
		((Aggref *) node)->args = aggref->args;

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
		fn = makeFuncCall(SystemFuncName("sum"), NIL, COERCE_EXPLICIT_CALL, -1);
#else
		fn = makeFuncCall(SystemFuncName("sum"), NIL, -1);
#endif

		/* Make a Func with dummy args, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, dmy_args, NULL, fn, false, -1);
		((Aggref *) node)->args = aggref->args;

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
CreateIvmTriggersOnBaseTables(Query *qry, ImmvAddress immv_addr)
{
	Relids	relids = NULL;
	bool	ex_lock = false;
	RangeTblEntry *rte;

	/* Immediately return if we don't have any base tables. */
	if (list_length(qry->rtable) < 1)
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

	rte = list_nth(qry->rtable, 0);
	if (list_length(qry->rtable) > 1 || rte->rtekind != RTE_RELATION ||
		qry->distinctClause || (qry->hasAggs && qry->groupClause))
		ex_lock = true;

	CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)qry, immv_addr, &relids, ex_lock);

	bms_free(relids);
}

static void
CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, ImmvAddress immv_addr,
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

				CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *) query->jointree, immv_addr, relids, ex_lock);
				foreach(lc, query->cteList)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
					Assert(IsA(cte->ctequery, Query));
					CreateIvmTriggersOnBaseTablesRecurse((Query *) cte->ctequery, cte->ctequery, immv_addr, relids, ex_lock);
				}
			}
			break;

		case T_RangeTblRef:
			{
				int			rti = ((RangeTblRef *) node)->rtindex;
				RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

				if (rte->rtekind == RTE_RELATION && !bms_is_member(rte->relid, *relids))
				{
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_BEFORE, true);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, immv_addr, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_AFTER, true);

					*relids = bms_add_member(*relids, rte->relid);
				}
				else if (rte->rtekind == RTE_SUBQUERY)
				{
					Query *subquery = rte->subquery;
					Assert(rte->subquery != NULL);
					CreateIvmTriggersOnBaseTablesRecurse(subquery, (Node *) subquery, immv_addr, relids, ex_lock);
				}
			}
			break;

		case T_FromExpr:
			{
				FromExpr   *f = (FromExpr *) node;
				ListCell   *l;

				foreach(l, f->fromlist)
					CreateIvmTriggersOnBaseTablesRecurse(qry, lfirst(l), immv_addr, relids, ex_lock);
			}
			break;

		case T_JoinExpr:
			{
				JoinExpr   *j = (JoinExpr *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, j->larg, immv_addr, relids, ex_lock);
				CreateIvmTriggersOnBaseTablesRecurse(qry, j->rarg, immv_addr, relids, ex_lock);
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
CreateIvmTrigger(Oid relOid, ImmvAddress immv_addr, int16 type, int16 timing, bool ex_lock)
{
	ObjectAddress	refaddr;
	ObjectAddress	address;
	CreateTrigStmt *ivm_trigger;
	List *transitionRels = NIL;

	Assert(timing == TRIGGER_TYPE_BEFORE || timing == TRIGGER_TYPE_AFTER);

	refaddr.classId = RelationRelationId;
	refaddr.objectId = immv_addr.address.objectId;
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

	ivm_trigger->trigname = psprintf("%s_%d_%d", ivm_trigger->trigname, relOid,
									 immv_addr.address.objectId);
	ivm_trigger->funcname =
		(timing == TRIGGER_TYPE_BEFORE ?
			PgIvmFuncName("IVM_immediate_before") : PgIvmFuncName("IVM_immediate_maintenance"));

	ivm_trigger->columns = NIL;
	ivm_trigger->transitionRels = transitionRels;
	ivm_trigger->whenClause = NULL;
	ivm_trigger->isconstraint = false;
	ivm_trigger->deferrable = false;
	ivm_trigger->initdeferred = false;
	ivm_trigger->constrrel = NULL;
	ivm_trigger->args = list_make2(
		makeString(DatumGetPointer(DirectFunctionCall1(uuid_out, UUIDPGetDatum(&immv_addr.immv_uuid)))),
		makeString(DatumGetPointer(DirectFunctionCall1(boolout, BoolGetDatum(ex_lock))))
		);

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, false, false);

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
	check_ivm_restriction_context context;

	context.has_agg = false;
	context.allow_exists = false;
	context.in_exists_subquery = false;
	context.exists_qual_vars = NIL;
	context.sublevels_up = 0;

	check_ivm_restriction_walker(node, &context);
}

static bool
check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context)
{
	/* EXISTS is allowed only in this node */
	bool allow_exists = context->allow_exists;
	context->allow_exists = false;

	if (node == NULL)
		return false;

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *qry = (Query *) node;
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
						/* if the view has a system column, raise an error */
						if (var->varattno < 0)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("system column is not supported on incrementally maintainable materialized view")));
					}
				}

				/* check that each type in the target list has an equality operator */
				if (context->sublevels_up == 0)
				{
					foreach(lc, qry->targetList)
					{
						TargetEntry *tle = (TargetEntry *) lfirst(lc);
						Oid		atttype = exprType((Node *) tle->expr);
						Oid		opclass;


						opclass = GetDefaultOpClass(atttype, BTREE_AM_OID);
						if (!OidIsValid(opclass))
							ereport(ERROR,
										(errcode(ERRCODE_UNDEFINED_OBJECT),
										 errmsg("data type %s has no default operator class for access method \"%s\"",
												format_type_be(atttype), "btree")));
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
						context->sublevels_up++;
						check_ivm_restriction_walker((Node *) rte->subquery, context);
						context->sublevels_up--;
					}
				}

				query_tree_walker(qry, check_ivm_restriction_walker, (void *) context, QTW_IGNORE_RT_SUBQUERIES);

				/*
				 * additional restriction checks for exists subquery
				 *
				 * If the query has an EXISTS subquery and columns of a table in
				 * the outer query are used in the EXISTS subquery, those columns
				 * must be included in the target list. These columns are required
				 * to identify tuples in the view to be affected by modification
				 * of tables in the EXISTS subquery.
				 */
				if (context->exists_qual_vars != NIL && context->sublevels_up == 0)
				{
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
				 * When a table in an unreferenced CTE is TRUNCATEd, the contents of the
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
				TargetEntry *tle = (TargetEntry *) node;

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
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;

				check_ivm_restriction_walker((Node *) from->fromlist, context);

				/*
				 * EXIST is allowed directly under FROM clause
				 */
				context->allow_exists = true;
				check_ivm_restriction_walker(from->quals, context);
				break;
			}
		case T_JoinExpr:
			{
				JoinExpr *joinexpr = (JoinExpr *) node;

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
				/*
				 * Currently, only EXISTS clause is allowed here.
				 * If EXISTS subquery refers to vars of the upper query, collect these vars.
				 */
				if (variable->varlevelsup > 0 && context->in_exists_subquery)
					context->exists_qual_vars = lappend(context->exists_qual_vars, node);
				break;
			}
		case T_BoolExpr:
			{
				BoolExpr *expr = (BoolExpr *) node;
				BoolExprType type = ((BoolExpr *) node)->boolop;
				ListCell *lc;

				switch (type)
				{
					case AND_EXPR:
						foreach(lc, expr->args)
						{
							Node *opnode = (Node *) lfirst(lc);

							/*
							 * EXIST is allowed under AND expression only if it is
							 * directly under WHERE.
							 */
							if (allow_exists)
								context->allow_exists = true;
							check_ivm_restriction_walker(opnode, context);
						}
						break;
					case OR_EXPR:
					case NOT_EXPR:
						if (checkExprHasSubLink(node))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("this query is not allowed on incrementally maintainable materialized view"),
									 errhint("OR or NOT conditions and EXISTS condition can not be used together")));

						expression_tree_walker((Node *) expr->args, check_ivm_restriction_walker, (void *) context);
						break;
				}
				break;
			}
		case T_SubLink:
			{
				Query *subselect;
				SubLink	*sublink = (SubLink *) node;

				/* Only EXISTS clause is supported if it is directly under WHERE */
				if (!allow_exists || sublink->subLinkType != EXISTS_SUBLINK)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("this query is not allowed on incrementally maintainable materialized view"),
							 errhint("sublink only supports subquery with EXISTS clause in WHERE clause")));

				if (context->sublevels_up > 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("nested sublink is not supported on incrementally maintainable materialized view")));

				subselect = (Query *) sublink->subselect;

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
 * all primary key attributes of its base tables in the target list, the index
 * is created on these attributes. In other cases, no index is created.
 */
void
CreateIndexOnIMMV(Query *query, Relation matviewRel)
{
	ListCell *lc;
	IndexStmt  *index;
	ObjectAddress address;
	List *constraintList = NIL;
	char		idxname[NAMEDATALEN];
	List	   *indexoidlist = RelationGetIndexList(matviewRel);
	ListCell   *indexoidscan;

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
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
	index->oldNumber = InvalidRelFileNumber;
	index->oldFirstRelfilelocatorSubid = InvalidSubTransactionId;
#else
	index->oldNode = InvalidOid;
	index->oldFirstRelfilenodeSubid = InvalidSubTransactionId;
#endif
	index->oldCreateSubid = InvalidSubTransactionId;
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
		key_attnos = get_primary_key_attnos_from_query(query, &constraintList);
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
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
						  -1,
#endif
						  false, true, false, false, true);

	ereport(NOTICE,
			(errmsg("created index \"%s\" on immv \"%s\"",
					idxname, RelationGetRelationName(matviewRel))));

	/*
	 * Make dependencies so that the index is dropped if any base tables'
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
get_primary_key_attnos_from_query(Query *query, List **constraintList)
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
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);
		Bitmapset *key_attnos;
		bool	has_no_pkey = false;

		/* for subqueries, scan recursively */
		if (r->rtekind == RTE_SUBQUERY)
		{
			key_attnos = get_primary_key_attnos_from_query(r->subquery, constraintList);
			has_no_pkey = (key_attnos == NULL);
		}
		/* for tables, call get_primary_key_attnos */
		else if (r->rtekind == RTE_RELATION)
		{
			Oid constraintOid;
			key_attnos = get_primary_key_attnos(r->relid, false, &constraintOid);
			*constraintList = lappend_oid(*constraintList, constraintOid);
			has_no_pkey = (key_attnos == NULL);
		}
		/*
		 * Ignore join rels, because they are flatten later by
		 * flatten_join_alias_vars(). Store NULL into key_attnos_list
		 * as a dummy.
		 */
		else if (r->rtekind == RTE_JOIN)
		{
			key_attnos = NULL;
		}
		/* for other RTEs, we assume they have no candidate key */
		else
			has_no_pkey = true;

		/*
		 * If any table or subquery has no primary key or its pkey constraint
		 * is deferrable (i.e., get_primary_key_attnos returned NULL),
		 * we cannot get key attributes for this query, so return NULL.
		 */
		if (has_no_pkey)
			return NULL;

		key_attnos_list = lappend(key_attnos_list, key_attnos);
	}

	/* Collect key attributes appearing in the target list */
	i = 1;
	foreach(lc, query->targetList)
	{
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
		TargetEntry *tle = (TargetEntry *) flatten_join_alias_vars(NULL, query, lfirst(lc));
#else
		TargetEntry *tle = (TargetEntry *) flatten_join_alias_vars(query, lfirst(lc));
#endif

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
				key_attnos = bms_del_member(key_attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
				if (bms_is_empty(key_attnos))
				{
					key_attnos_list = list_delete_nth_cell(key_attnos_list, var->varno - 1);
					key_attnos_list = list_insert_nth(key_attnos_list, var->varno - 1, NULL);
				}
				keys = bms_add_member(keys, i - FirstLowInvalidHeapAttributeNumber);
			}
		}
		i++;
	}

	/* Collect RTE indexes of relations appearing in the FROM clause */
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 160000)
	rels_in_from = get_relids_in_jointree((Node *) query->jointree, false, false);
#else
	rels_in_from = get_relids_in_jointree((Node *) query->jointree, false);
#endif

	/*
	 * Check if all key attributes of relations in FROM are appearing in the target
	 * list.  If an attribute remains in key_attnos_list in spite of the table is used
	 * in FROM clause, the target is missing this key attribute, so we return NULL.
	 */
	i = 1;
	foreach(lc, key_attnos_list)
	{
		Bitmapset *bms = (Bitmapset *) lfirst(lc);
		if (!bms_is_empty(bms) && bms_is_member(i, rels_in_from))
			return NULL;
		i++;
	}

	return keys;
}

/*
 * Store the query for the IMMV to pg_ivm_immv
 */
static void
StoreImmvQuery(ImmvAddress immv_addr, Query *viewQuery)
{
	char   *querystring;
	int		save_nestlevel;
	Datum values[Natts_pg_ivm_immv];
	bool isNulls[Natts_pg_ivm_immv];
	Relation matviewRel;
	Relation pgIvmImmv;
	TupleDesc tupleDescriptor;
	HeapTuple heapTuple;
	ObjectAddress	address;

	/*
	 * Restrict search_path so that pg_ivm_get_viewdef_internal returns a
	 * fully-qualified query.
	 */
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();
	matviewRel = table_open(immv_addr.address.objectId, AccessShareLock);
	querystring = pg_ivm_get_viewdef_internal(viewQuery, matviewRel, true);
	table_close(matviewRel, NoLock);
	/* Roll back the search_path change. */
	AtEOXact_GUC(false, save_nestlevel);

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_ivm_immv_immvrelid -1 ] = ObjectIdGetDatum(immv_addr.address.objectId);
	values[Anum_pg_ivm_immv_immvuuid -1 ] = UUIDPGetDatum(&immv_addr.immv_uuid);
	values[Anum_pg_ivm_immv_querystring - 1] = CStringGetTextDatum(querystring);
	values[Anum_pg_ivm_immv_ispopulated -1 ] = BoolGetDatum(false);

	pgIvmImmv = table_open(PgIvmImmvRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgIvmImmv);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgIvmImmv, heapTuple);

	address.classId = RelationRelationId;
	address.objectId = immv_addr.address.objectId;
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
