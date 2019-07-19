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
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"


#define AlterEnumIsRename(stmt) (stmt->oldVal != NULL)
#define AlterEnumIsAddValue(stmt) (stmt->oldVal == NULL)


/* forward declaration for helper functions*/
static void makeRangeVarQualified(RangeVar *var);
static List * FilterNameListForDistributedTypes(List *objects);
static bool type_is_distributed(Oid typid);


/* forward declaration for deparse functions */
static const char * deparse_composite_type_stmt(CompositeTypeStmt *stmt);
static void appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void appendColumnDef(StringInfo str, ColumnDef *columnDef);
static void appendColumnDefList(StringInfo str, List *columnDefs);

static const char * deparse_create_enum_stmt(CreateEnumStmt *stmt);
static void appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt);
static void appendStringList(StringInfo str, List *strings);

static const char * deparse_drop_type_stmt(DropStmt *stmt);
static void appendDropTypeStmt(StringInfo buf, DropStmt *stmt);
static void appendObjectList(StringInfo buf, List *objects);

static const char * deparse_alter_enum_stmt(AlterEnumStmt *stmt);
static void appendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt);


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


/*
 * PlanAlterTypeStmt is invoked for alter type statements for composite types (and possibly base types).
 */
List *
PlanAlterTypeStmt(AlterTableStmt *stmt, const char *queryString)
{
	Assert(stmt->relkind == OBJECT_TYPE);


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


/*
 * PlanAlterEnumStmt handles ALTER TYPE ... ADD VALUE for enum based types.
 */
List *
PlanAlterEnumStmt(AlterEnumStmt *stmt, const char *queryString)
{
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *alterEnumStmtSql = NULL;
	if (!type_is_distributed(typeOid))
	{
		return NIL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	alterEnumStmtSql = deparse_alter_enum_stmt(stmt);
	if (AlterEnumIsAddValue(stmt))
	{
		/*
		 * ADD VALUE can't be executed in a transaction, we will execute optimistically
		 * and on an error we will advise to fix the issue with the worker and rerun the
		 * query with the IF NOT EXTISTS modifier. The modifier is needed as the value
		 * might already be added to some nodes, but not all.
		 */

		/* TODO function name is unwieldly long, and runs serially which is not nice */
		List *commands = list_make2(DISABLE_DDL_PROPAGATION, (void *) alterEnumStmtSql);
		int result =
			SendBareOptionalCommandListToWorkersAsUser(ALL_WORKERS, commands, NULL);

		if (result != RESPONSE_OKAY)
		{
			const char *alterEnumStmtIfNotExistsSql = NULL;
			bool oldSkipIfNewValueExists = stmt->skipIfNewValExists;

			/* deparse the query with IF NOT EXISTS */
			stmt->skipIfNewValExists = true;
			alterEnumStmtIfNotExistsSql = deparse_alter_enum_stmt(stmt);
			stmt->skipIfNewValExists = oldSkipIfNewValueExists;

			ereport(WARNING, (errmsg("not all workers applied change to enum"),
							  errdetail("retry with: %s", alterEnumStmtIfNotExistsSql),
							  errhint("make sure the coordinators can communicate with "
									  "all workers")));
		}
	}
	else
	{
		/* other statements can be run in a transaction and will be dispatched here. */
		/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
		SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
		SendCommandToWorkersAsUser(ALL_WORKERS, alterEnumStmtSql, NULL);
	}

	return NIL;
}


List *
PlanDropTypeStmt(DropStmt *stmt, const char *queryString)
{
	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *oldTypes = stmt->objects;
	List *distributedTypes = FilterNameListForDistributedTypes(oldTypes);
	const char *dropStmtSql = NULL;

	if (list_length(distributedTypes) <= 0)
	{
		/*
		 * no distributed types to drop, we allow local drops of non distributed types on
		 * workers as well, hence we perform this check before ensuring being a
		 * coordinator
		 */
		return NULL;
	}

	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedTypes;
	dropStmtSql = deparse_drop_type_stmt(stmt);
	stmt->objects = oldTypes;

	/* to prevent recursion with mx we disable ddl propagation */
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, dropStmtSql, NULL);

	return NULL;
}


/********************************************************************************
 * Section with helper functions
 *********************************************************************************/


/*
 * FilterNameListForDistributedTypes takes a list of objects to delete, for Types this
 * will be a list of TypeName. This list is filtered against the types that are
 * distributed.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedTypes(List *objects)
{
	ListCell *objectCell = NULL;
	List *result = NIL;
	foreach(objectCell, objects)
	{
		TypeName *typeName = castNode(TypeName, lfirst(objectCell));
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		if (type_is_distributed(typeOid))
		{
			result = lappend(result, typeName);
		}
	}
	return result;
}


/*
 * type_is_distributed checks if a given type (based on its oid) is a distributed type.
 */
static bool
type_is_distributed(Oid typid)
{
	/* TODO keep track explicitly of distributed types and check here */
	switch (get_typtype(typid))
	{
		case TYPTYPE_COMPOSITE:
		case TYPTYPE_ENUM:
		{
			return true;
		}

		default:
		{
			return false;
		}
	}
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


static const char *
deparse_alter_enum_stmt(AlterEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendAlterEnumStmt(&sql, stmt);

	return sql.data;
}


static const char *
deparse_drop_type_stmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_TYPE);

	appendDropTypeStmt(&str, stmt);

	return str.data;
}


static void
appendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *identifier = format_type_be_qualified(typeOid);

	appendStringInfo(buf, "ALTER TYPE %s", identifier);

	if (AlterEnumIsRename(stmt))
	{
		/* Rename an existing label */
		appendStringInfo(buf, " RENAME VALUE %s TO %s;",
						 quote_literal_cstr(stmt->oldVal),
						 quote_literal_cstr(stmt->newVal));
	}
	else if (AlterEnumIsAddValue(stmt))
	{
		/* Add a new label */
		appendStringInfoString(buf, " ADD VALUE ");
		if (stmt->skipIfNewValExists)
		{
			appendStringInfoString(buf, "IF NOT EXISTS ");
		}
		appendStringInfoString(buf, quote_literal_cstr(stmt->newVal));

		if (stmt->newValNeighbor)
		{
			appendStringInfo(buf, " %s %s",
							 stmt->newValIsAfter ? "AFTER" : "BEFORE",
							 quote_literal_cstr(stmt->newValNeighbor));
		}

		appendStringInfoString(buf, ";");
	}
}


static void
appendDropTypeStmt(StringInfo buf, DropStmt *stmt)
{
	/*
	 * already tested at call site, but for future it might be collapsed in a
	 * deparse_drop_stmt so be safe and check again
	 */
	Assert(stmt->removeType == OBJECT_TYPE);

	appendStringInfo(buf, "DROP TYPE ");
	appendObjectList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


static void
appendObjectList(StringInfo buf, List *objects)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		TypeName *typeName = castNode(TypeName, lfirst(objectCell));
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		const char *identifier = format_type_be_qualified(typeOid);

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, identifier);
	}
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

	Assert(!columnDef->is_not_null); /* not null is not supported on composite types */

	appendStringInfo(str, "%s %s", columnDef->colname, format_type_be_qualified(typeOid));

	if (OidIsValid(collationOid))
	{
		const char *identifier = format_collate_be_qualified(collationOid);
		appendStringInfo(str, " COLLATE %s", identifier);
	}
}
