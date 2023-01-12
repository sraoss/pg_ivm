/*-------------------------------------------------------------------------
 *
 * subselect.c
 *	  incremental view maintenance extension
 *	  Routines for CTE support.
 *
 * Portions Copyright (c) 2023, IVM Development Group
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteManip.h"

#include "pg_ivm.h"

typedef struct inline_cte_walker_context
{
	const char *ctename;		/* name and relative level of target CTE */
	int			levelsup;
	Query	   *ctequery;		/* query to substitute */
} inline_cte_walker_context;

static bool inline_cte_walker(Node *node, inline_cte_walker_context *context);

/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
void
inline_cte(PlannerInfo *root, CommonTableExpr *cte)
{
	struct inline_cte_walker_context context;

	context.ctename = cte->ctename;
	/* Start at levelsup = -1 because we'll immediately increment it */
	context.levelsup = -1;
	context.ctequery = castNode(Query, cte->ctequery);

	(void) inline_cte_walker((Node *) root->parse, &context);
}

static bool
inline_cte_walker(Node *node, inline_cte_walker_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		context->levelsup++;

		/*
		 * Visit the query's RTE nodes after their contents; otherwise
		 * query_tree_walker would descend into the newly inlined CTE query,
		 * which we don't want.
		 */
		(void) query_tree_walker(query, inline_cte_walker, context,
								 QTW_EXAMINE_RTES_AFTER);

		context->levelsup--;

		return false;
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_CTE &&
			strcmp(rte->ctename, context->ctename) == 0 &&
			rte->ctelevelsup == context->levelsup)
		{
			/*
			 * Found a reference to replace.  Generate a copy of the CTE query
			 * with appropriate level adjustment for outer references (e.g.,
			 * to other CTEs).
			 */
			Query	   *newquery = copyObject(context->ctequery);

			if (context->levelsup > 0)
				IncrementVarSublevelsUp((Node *) newquery, context->levelsup, 1);

			/*
			 * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
			 *
			 * Historically, a FOR UPDATE clause has been treated as extending
			 * into views and subqueries, but not into CTEs.  We preserve this
			 * distinction by not trying to push rowmarks into the new
			 * subquery.
			 */
			rte->rtekind = RTE_SUBQUERY;
			rte->subquery = newquery;
			rte->security_barrier = false;

			/* Zero out CTE-specific fields */
			rte->ctename = NULL;
			rte->ctelevelsup = 0;
			rte->self_reference = false;
			rte->coltypes = NIL;
			rte->coltypmods = NIL;
			rte->colcollations = NIL;
		}

		return false;
	}

	return expression_tree_walker(node, inline_cte_walker, context);
}
