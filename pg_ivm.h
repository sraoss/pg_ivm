/*-------------------------------------------------------------------------
 *
 * pg_ivm.h
 *	  incremental view maintenance extension
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef _PG_IVM_H_
#define _PG_IVM_H_

#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "nodes/params.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"

#define Natts_pg_ivm_immv 3

#define Anum_pg_ivm_immv_immvrelid 1
#define Anum_pg_ivm_immv_withnodata 2
#define Anum_pg_ivm_immv_viewdef 3

/* pg_ivm.c */

extern void CreateChangePreventTrigger(Oid matviewOid);
extern Oid PgIvmImmvRelationId(void);
extern Oid PgIvmImmvPrimaryKeyIndexId(void);

/* createas.c */

extern ObjectAddress ExecCreateImmv(ParseState *pstate, CreateTableAsStmt *stmt,
									ParamListInfo params, QueryEnvironment *queryEnv,
									QueryCompletion *qc);
extern void CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid, bool is_create);
extern void CreateIndexOnIMMV(Query *query, Relation matviewRel, bool is_create);
extern Query *rewriteQueryForIMMV(Query *query, List *colNames);

/* matview.c */

extern ObjectAddress ExecRefreshImmv(const char *relname, bool skipData, QueryCompletion *qc);
//extern DestReceiver *CreateTransientRelDestReceiver(Oid oid);
extern bool ImmvIncrementalMaintenanceIsEnabled(void);
extern Datum IVM_immediate_before(PG_FUNCTION_ARGS);
extern Datum IVM_immediate_maintenance(PG_FUNCTION_ARGS);
extern void AtAbort_IVM(void);
extern bool isIvmName(const char *s);


#endif
