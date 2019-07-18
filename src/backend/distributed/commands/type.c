/*-------------------------------------------------------------------------
 *
 * type.c
 *    Commands for TYPE statements.
 *    The following types are supported in citus
 *     - Composite Types
 *     - Enum Types (TODO)
 *
 *    Base types are more complex and often involve c code from extensions.
 *    These types should be created by creating the extension on all the
 *    workers as well. Therefore types created during the creation of an
 *    extension are not propagated to the worker nodes.
 *
 *    Types will be created on all active workers on type creation and
 *    during the node activate protocol.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/metadata_sync.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"


static const char * get_composite_type_stmt_sql(CompositeTypeStmt *stmt);
static void appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void appendColumnDef(StringInfo str, ColumnDef *columnDef);
static void appendColumnDefList(StringInfo str, List *columnDefs);
static void makeRangeVarQualified(RangeVar *var);


List *
PlanCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString)
{
	Oid schemaId = InvalidOid;
	const char *compositeTypeStmtSql = NULL;

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	makeRangeVarQualified(stmt->typevar);

	/* make sure the namespace used for the creation of the type exists on all workers */
	schemaId = RangeVarGetCreationNamespace(stmt->typevar);
	EnsureSchemaExistsOnAllNodes(schemaId);

	/* reconstruct creation statement in a portable fashion */
	compositeTypeStmtSql = get_composite_type_stmt_sql(stmt);

	/* to prevent recursion with mx we disable ddl propagation */
	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */

	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, compositeTypeStmtSql, NULL);


	return NULL;
}


/*
 * makeRangeVarQualified will fill in the schemaname in RangeVar if it is not already
 * present. The schema used will be the default schemaname for creation of new objects as
 * returned by RangeVarGetCreationNamespace.
 */
static void
makeRangeVarQualified(RangeVar *var)
{
	if (var->schemaname == NULL)
	{
		Oid creationSchema = RangeVarGetCreationNamespace(var);
		var->schemaname = get_namespace_name(creationSchema);
	}
}


List *
PlanDropTypeStmt(DropStmt *stmt, const char *queryString)
{
	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/* to prevent recursion with mx we disable ddl propagation */
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, queryString, NULL);

	return NULL;
}


List *
PlanCreateEnumStmt(CreateEnumStmt *createEnumStmt, const char *queryString)
{
	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/* to prevent recursion with mx we disable ddl propagation */
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, queryString, NULL);

	return NULL;
}


/*
 * get_composite_type_stmt_sql builds and returns a string representing the
 * CompositeTypeStmt for application on a remote server.
 */
static const char *
get_composite_type_stmt_sql(CompositeTypeStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCompositeTypeStmt(&sql, stmt);

	return sql.data;
}


/*
 * appendCompositeTypeStmt appends the sql string to recreate a CompositeTypeStmt to the
 * provided buffer, ending in a ; for concatination of multiple statements.
 */
static void
appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->typevar->schemaname,
														stmt->typevar->relname);
	appendStringInfo(str, "CREATE TYPE %s AS (", identifier);
	appendColumnDefList(str, stmt->coldeflist);
	appendStringInfo(str, ");");
}


/*
 * appendColumnDefList appends the definition of a list of ColumnDef items to the provided
 * buffer, adding separators as necessary.
 */
static void
appendColumnDefList(StringInfo str, List *columnDefs)
{
	ListCell *columnDefCell = NULL;
	foreach(columnDefCell, columnDefs)
	{
		if (columnDefCell != list_head(columnDefs))
		{
			appendStringInfo(str, ", ");
		}
		appendColumnDef(str, castNode(ColumnDef, lfirst(columnDefCell)));
	}
}


/*
 * appendColumnDef appends the definition of one ColumnDef completely qualifiedto the
 * provided buffer.
 */
static void
appendColumnDef(StringInfo str, ColumnDef *columnDef)
{
	Oid typeOid = LookupTypeNameOid(NULL, columnDef->typeName, false);
	appendStringInfo(str, "%s %s", columnDef->colname, format_type_be_qualified(typeOid));
}
