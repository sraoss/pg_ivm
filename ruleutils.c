/*-------------------------------------------------------------------------
 *
 * ruleutils.c
 *	  incremental view maintenance extension
 *    Routines for convert stored expressions/querytrees back to
 *	  source text
 *
 * Portions Copyright (c) 2022, IVM Development Group
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 150000)
#include "utils/ruleutils.h"
#elif defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 140000)
#include "ruleutils_14.c"
#else
#include "ruleutils_13.c"
#endif

#include "pg_ivm.h"

/* Standard conversion of a "bool pretty" option to detailed flags */
#define GET_PRETTY_FLAGS(pretty) \
	((pretty) ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA) \
	 : PRETTYFLAG_INDENT)

/* ----------
 * pg_ivm_get_viewdef
 *
 * Public entry point to deparse a view definition query parsetree.
 * The pretty flags are determined by GET_PRETTY_FLAGS(pretty).
 *
 * The result is a palloc'd C string.
 * ----------
 */
char *
pg_ivm_get_viewdef(Relation immvrel, bool pretty)
{
	Query *query = get_immv_query(immvrel);
	TupleDesc resultDesc = RelationGetDescr(immvrel);

#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 150000)
	ListCell *lc;
	int colno = 0;

	/*
	 * Rewrite the result column name using the view's tuple
	 * descriptor.
	 *
	 * The column name is usually figured out in get_query_def
	 * using a tupleDesc specified as an argument, but this
	 * function is static, so we cannot directly call it.
	 * Therefore, we rewrite them prior to calling the public
	 * function pg_get_querydef (for PG15 or higher).
	 */
	query = copyObject(query);
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
			continue;           /* ignore junk entries */

		colno++;
		if (resultDesc && colno <= resultDesc->natts)
			tle->resname = NameStr(TupleDescAttr(resultDesc, colno - 1)->attname);
	}

	return pg_get_querydef(query, pretty);

#else
	StringInfoData buf;
	int			prettyFlags;

	prettyFlags = GET_PRETTY_FLAGS(pretty);

	initStringInfo(&buf);

	/*
	 * For PG14 or earlier, we use get_query_def which is copied
	 * from the core because any public function for this purpose
	 * is not available.
	 */
	get_query_def(query, &buf, NIL, resultDesc, true,
				  prettyFlags, WRAP_COLUMN_DEFAULT, 0);

	return buf.data;
#endif
}
