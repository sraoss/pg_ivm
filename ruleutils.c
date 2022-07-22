/*-------------------------------------------------------------------------
 *
 * ruleutils.c
 *	  XXX:Functions to convert stored expressions/querytrees back to
 *	  source text
 *
 * Portions Copyright (c) 2022, IVM Development Group
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ruleutils.c
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
 * pg_get_querydef
 *
 * Public entry point to deparse one query parsetree.
 * The pretty flags are determined by GET_PRETTY_FLAGS(pretty).
 *
 * The result is a palloc'd C string.
 * ----------
 */
char *
pg_ivm_get_querydef(Query *query, bool pretty)
{
#if defined(PG_VERSION_NUM) && (PG_VERSION_NUM >= 150000)
	return pg_get_querydef(query, pretty);
#else
	StringInfoData buf;
	int			prettyFlags;

	prettyFlags = GET_PRETTY_FLAGS(pretty);

	initStringInfo(&buf);

	get_query_def(query, &buf, NIL, NULL, true,
				  prettyFlags, WRAP_COLUMN_DEFAULT, 0);

	return buf.data;
#endif
}
