/*-------------------------------------------------------------------------
 *
 * pg_ivm.c
 *	  incremental view maintenance extension
 *    Routines for user interfaces and callback functions
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 2022, IVM Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_trigger_d.h"
#include "commands/trigger.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_ivm.h"

PG_MODULE_MAGIC;

static Oid pg_ivm_immv_id = InvalidOid;
static Oid pg_ivm_immv_pkey_id = InvalidOid;

void		_PG_init(void);

static void IvmXactCallback(XactEvent event, void *arg);
static void IvmSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
							   SubTransactionId parentSubid, void *arg);

/* SQL callable functions */
PG_FUNCTION_INFO_V1(create_immv);
PG_FUNCTION_INFO_V1(IVM_prevent_immv_change);

/*
 * Call back functions for cleaning up
 */
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


/*
 * Module load callback
 */
void
_PG_init(void)
{
	RegisterXactCallback(IvmXactCallback, NULL);
	RegisterSubXactCallback(IvmSubXactCallback, NULL);
}


/*
 * User inerface for creating an IMMV
 */
Datum
create_immv(PG_FUNCTION_ARGS)
{
	text	*t_relname = PG_GETARG_TEXT_PP(0);
	text	*t_sql = PG_GETARG_TEXT_PP(1);
	char	*relname = text_to_cstring(t_relname);
	char	*sql = text_to_cstring(t_sql);
	List	*parsetree_list;
	RawStmt	*parsetree;
	QueryCompletion qc;

	ParseState *pstate = make_parsestate(NULL);
	Query *query;
	CreateTableAsStmt *stmt;
	StringInfoData command_buf;


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
	query = castNode(Query, stmt->query);

	ExecCreateImmv(pstate, stmt, NULL, NULL, &qc);

	PG_RETURN_INT64(qc.nprocessed);
}

/*
 * Trigger function to prevent IMMV from being changed
 */
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


/*
 * Create triggers to prevent IMMV from being changed
 */
void
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
 * Get relid of pg_ivm_immv
 */
Oid
PgIvmImmvRelationId(void)
{
	if (!OidIsValid(pg_ivm_immv_id))
		pg_ivm_immv_id = get_relname_relid("pg_ivm_immv", PG_CATALOG_NAMESPACE);

	return pg_ivm_immv_id;
}

/*
 * Get relid of pg_ivm_immv's primary key
 */
Oid
PgIvmImmvPrimaryKeyIndexId(void)
{
	if (!OidIsValid(pg_ivm_immv_pkey_id))
		pg_ivm_immv_pkey_id = get_relname_relid("pg_ivm_immv_pkey", PG_CATALOG_NAMESPACE);

	return pg_ivm_immv_pkey_id;
}

