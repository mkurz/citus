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


/* forward declaration for helper functions*/
static void makeRangeVarQualified(RangeVar *var);


/* forward declaration for deparse functions */
static const char * deparse_composite_type_stmt(CompositeTypeStmt *stmt);
static void appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void appendColumnDef(StringInfo str, ColumnDef *columnDef);
static void appendColumnDefList(StringInfo str, List *columnDefs);

static const char * deparse_create_enum_stmt(CreateEnumStmt *stmt);
static void appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt);
static void appendStringList(StringInfo str, List *strings);


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
	compositeTypeStmtSql = deparse_composite_type_stmt(stmt);
	ereport(LOG, (errmsg("deparsed composite type statement"),
				  errdetail("sql: %s", compositeTypeStmtSql)));

	/* to prevent recursion with mx we disable ddl propagation */
	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, compositeTypeStmtSql, NULL);


	return NULL;
}


List *
PlanCreateEnumStmt(CreateEnumStmt *stmt, const char *queryString)
{
	Oid schemaId = InvalidOid;
	char *objname = NULL;
	const char *createEnumStmtSql = NULL;

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/* make sure the namespace used for the creation of the type exists on all workers */
	schemaId = QualifiedNameGetCreationNamespace(stmt->typeName, &objname);
	EnsureSchemaExistsOnAllNodes(schemaId);

	/* reconstruct creation statement in a portable fashion */
	createEnumStmtSql = deparse_create_enum_stmt(stmt);
	ereport(LOG, (errmsg("deparsed enum type statement"),
				  errdetail("sql: %s", createEnumStmtSql)));

	/* to prevent recursion with mx we disable ddl propagation */
	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, createEnumStmtSql, NULL);

	return NULL;
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


/********************************************************************************
 * Section with helper functions
 *********************************************************************************/


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


/********************************************************************************
 * Section with deparse functions
 *********************************************************************************/


/*
 * deparse_composite_type_stmt builds and returns a string representing the
 * CompositeTypeStmt for application on a remote server.
 */
static const char *
deparse_composite_type_stmt(CompositeTypeStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCompositeTypeStmt(&sql, stmt);

	return sql.data;
}


static const char *
deparse_create_enum_stmt(CreateEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCreateEnumStmt(&sql, stmt);

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


static void
appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt)
{
	RangeVar *typevar = NULL;
	const char *identifier = NULL;

	/* extract the name from the statement and make fully qualified as a rangevar */
	typevar = makeRangeVarFromNameList(stmt->typeName);
	makeRangeVarQualified(typevar);

	/* create the identifier from the fully qualified rangevar */
	identifier = quote_qualified_identifier(typevar->schemaname, typevar->relname);

	appendStringInfo(str, "CREATE TYPE %s AS ENUM (", identifier);
	appendStringList(str, stmt->vals);
	appendStringInfo(str, ");");
}


static void
appendStringList(StringInfo str, List *strings)
{
	ListCell *stringCell = NULL;
	foreach(stringCell, strings)
	{
		const char *string = strVal(lfirst(stringCell));
		if (stringCell != list_head(strings))
		{
			appendStringInfoString(str, ", ");
		}

		string = quote_literal_cstr(string);
		appendStringInfoString(str, string);
	}
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
			appendStringInfoString(str, ", ");
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
	Oid collationOid = GetColumnDefCollation(NULL, columnDef, typeOid);
	appendStringInfo(str, "%s %s", columnDef->colname, format_type_be_qualified(typeOid));

	if (OidIsValid(collationOid))
	{
		const char *collationName = get_collation_name(collationOid);
		appendStringInfo(str, " COLLATE %s", quote_identifier(collationName));
	}
}
