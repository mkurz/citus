/*-------------------------------------------------------------------------
 *
 * type.c
 *    Commands for TYPE statements.
 *    The following types are supported in citus
 *     - Composite Types
 *     - Enum Types
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

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/typcache.h"

#include "distributed/commands.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
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
static TypeName * makeTypeNameFromRangeVar(const RangeVar *relation);
static void EnsureSequentialModeForTypeDDL(void);


/* recreate functions */
static CompositeTypeStmt * RecreateCompositeTypeStmt(Oid typeOid);
static List * composite_type_coldeflist(Oid typeOid);
static CreateEnumStmt * RecreateEnumStmt(Oid typeOid);
static List * enum_vals_list(Oid typeOid);

/* forward declaration for deparse functions */
static void appendCompositeTypeStmt(StringInfo str, CompositeTypeStmt *stmt);
static void appendColumnDef(StringInfo str, ColumnDef *columnDef);
static void appendColumnDefList(StringInfo str, List *columnDefs);

static void appendCreateEnumStmt(StringInfo str, CreateEnumStmt *stmt);
static void appendStringList(StringInfo str, List *strings);

static void appendDropTypeStmt(StringInfo buf, DropStmt *stmt);
static void appendTypeNameList(StringInfo buf, List *objects);

static void appendAlterEnumStmt(StringInfo buf, AlterEnumStmt *stmt);

static void appendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt);
static void appendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd);
static void appendAlterTypeCmdAlterColumnType(StringInfo buf,
											  AlterTableCmd *alterTableCmd);


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
	ereport(DEBUG3, (errmsg("deparsed composite type statement"),
					 errdetail("sql: %s", compositeTypeStmtSql)));


	/* to prevent recursion with mx we disable ddl propagation */
	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
	EnsureSequentialModeForTypeDDL();
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
	const char *alterTypeStmtSql = NULL;
	TypeName *typeName = NULL;

	Assert(stmt->relkind == OBJECT_TYPE);

	/* check if type is distributed before we run the coordinator check */
	typeName = makeTypeNameFromRangeVar(stmt->relation);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	if (!type_is_distributed(typeOid))
	{
		return NIL;
	}

	EnsureCoordinator();

	/* reconstruct alter statement in a portable fashion */
	alterTypeStmtSql = deparse_alter_type_stmt(stmt);
	ereport(DEBUG3, (errmsg("deparsed alter type statement"),
					 errdetail("sql: %s", alterTypeStmtSql)));

	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
	EnsureSequentialModeForTypeDDL();
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, alterTypeStmtSql, NULL);

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
	ereport(DEBUG3, (errmsg("deparsed enum type statement"),
					 errdetail("sql: %s", createEnumStmtSql)));

	/* to prevent recursion with mx we disable ddl propagation */
	/* TODO, mx expects the extension owner to be used here, this requires an alter owner statement as well */
	EnsureSequentialModeForTypeDDL();
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
		EnsureSequentialModeForTypeDDL();
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
	EnsureSequentialModeForTypeDDL();
	SendCommandToWorkersAsUser(ALL_WORKERS, DISABLE_DDL_PROPAGATION, NULL);
	SendCommandToWorkersAsUser(ALL_WORKERS, dropStmtSql, NULL);

	return NULL;
}


Node *
RecreateTypeStatement(Oid typeOid)
{
	switch (get_typtype(typeOid))
	{
		case TYPTYPE_ENUM:
		{
			return (Node *) RecreateEnumStmt(typeOid);
		}

		case TYPTYPE_COMPOSITE:
		{
			return (Node *) RecreateCompositeTypeStmt(typeOid);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported type to generate create statement for"),
							errdetail("only enum and composite types can be recreated")));
		}
	}
}


static CompositeTypeStmt *
RecreateCompositeTypeStmt(Oid typeOid)
{
	CompositeTypeStmt *stmt = NULL;

	Assert(get_typtype(typeOid) == TYPTYPE_COMPOSITE);

	stmt = makeNode(CompositeTypeStmt);
	List *names = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->typevar = makeRangeVarFromNameList(names);
	stmt->coldeflist = composite_type_coldeflist(typeOid);

	return stmt;
}


static ColumnDef *
attributeFormToColumnDef(Form_pg_attribute attributeForm)
{
	return makeColumnDef(NameStr(attributeForm->attname),
						 attributeForm->atttypid,
						 -1,
						 attributeForm->attcollation);
}


static List *
composite_type_coldeflist(Oid typeOid)
{
	Relation relation = NULL;
	Oid relationId = InvalidOid;
	TupleDesc tupleDescriptor = NULL;
	int attributeIndex = 0;
	List *columnDefs = NIL;

	relationId = typeidTypeRelid(typeOid);
	relation = relation_open(relationId, AccessShareLock);

	tupleDescriptor = RelationGetDescr(relation);
	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attributeIndex);

		if (attributeForm->attisdropped)
		{
			/* skip logically hidden attributes */
			continue;
		}

		columnDefs = lappend(columnDefs, attributeFormToColumnDef(attributeForm));
	}

	relation_close(relation, AccessShareLock);

	return columnDefs;
}


static CreateEnumStmt *
RecreateEnumStmt(Oid typeOid)
{
	CreateEnumStmt *stmt = NULL;

	Assert(get_typtype(typeOid) == TYPTYPE_ENUM);

	stmt = makeNode(CreateEnumStmt);
	stmt->typeName = stringToQualifiedNameList(format_type_be_qualified(typeOid));
	stmt->vals = enum_vals_list(typeOid);

	return stmt;
}


static List *
enum_vals_list(Oid typeOid)
{
	Relation enum_rel = NULL;
	SysScanDesc enum_scan = NULL;
	HeapTuple enum_tuple = NULL;
	ScanKeyData skey = { 0 };

	List *vals = NIL;

	/* Scan pg_enum for the members of the target enum type. */
	ScanKeyInit(&skey,
				Anum_pg_enum_enumtypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(typeOid));

	enum_rel = heap_open(EnumRelationId, AccessShareLock);
	enum_scan = systable_beginscan(enum_rel,
								   EnumTypIdLabelIndexId,
								   true, NULL,
								   1, &skey);

	/* collect all value names in CREATE TYPE ... AS ENUM stmt */
	while (HeapTupleIsValid(enum_tuple = systable_getnext(enum_scan)))
	{
		Form_pg_enum en = (Form_pg_enum) GETSTRUCT(enum_tuple);
		vals = lappend(vals, makeString(pstrdup(NameStr(en->enumlabel))));
	}

	systable_endscan(enum_scan);
	heap_close(enum_rel, AccessShareLock);
	return vals;
}


bool
CompositeTypeExists(CompositeTypeStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromRangeVar(stmt->typevar);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, true);
	return OidIsValid(typeOid);
}


bool
EnumTypeExists(CreateEnumStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromNameList(stmt->typeName);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	return OidIsValid(typeOid);
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


static TypeName *
makeTypeNameFromRangeVar(const RangeVar *relation)
{
	List *names = NIL;
	if (relation->schemaname)
	{
		names = lappend(names, makeString(relation->schemaname));
	}
	names = lappend(names, makeString(relation->relname));

	return makeTypeNameFromNameList(names);
}


static void
EnsureSequentialModeForTypeDDL(void)
{
	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create or modify type because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating or altering a type, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail("Type is created or altered. To make sure subsequent "
							   "commands see the type correctly we need to make sure to "
							   "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/********************************************************************************
 * Section with deparse functions
 *********************************************************************************/


/*
 * deparse_composite_type_stmt builds and returns a string representing the
 * CompositeTypeStmt for application on a remote server.
 */
const char *
deparse_composite_type_stmt(CompositeTypeStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCompositeTypeStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_create_enum_stmt(CreateEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendCreateEnumStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_alter_enum_stmt(AlterEnumStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	appendAlterEnumStmt(&sql, stmt);

	return sql.data;
}


const char *
deparse_drop_type_stmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_TYPE);

	appendDropTypeStmt(&str, stmt);

	return str.data;
}


const char *
deparse_alter_type_stmt(AlterTableStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->relkind == OBJECT_TYPE);

	appendAlterTypeStmt(&str, stmt);

	return str.data;
}


static void
appendAlterTypeStmt(StringInfo buf, AlterTableStmt *stmt)
{
	TypeName *typeName = makeTypeNameFromRangeVar(stmt->relation);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
	const char *identifier = format_type_be_qualified(typeOid);
	ListCell *cmdCell = NULL;

	Assert(stmt->relkind = OBJECT_TYPE);

	appendStringInfo(buf, "ALTER TYPE %s", identifier);
	foreach(cmdCell, stmt->cmds)
	{
		AlterTableCmd *alterTableCmd = NULL;

		if (cmdCell != list_head(stmt->cmds))
		{
			appendStringInfoString(buf, ", ");
		}

		alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		appendAlterTypeCmd(buf, alterTableCmd);
	}

	appendStringInfoString(buf, ";");
}


static void
appendAlterTypeCmd(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	switch (alterTableCmd->subtype)
	{
		case AT_AddColumn:
		{
			appendAlterTypeCmdAddColumn(buf, alterTableCmd);
			break;
		}

		case AT_DropColumn:
		{
			appendAlterTypeCmdDropColumn(buf, alterTableCmd);
			break;
		}

		case AT_AlterColumnType:
		{
			appendAlterTypeCmdAlterColumnType(buf, alterTableCmd);
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported subtype for alter table command"),
							errdetail("sub command type: %d", alterTableCmd->subtype)));
		}
	}
}


static void
appendAlterTypeCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	appendStringInfoString(buf, " ADD ATTRIBUTE ");
	appendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));
}


static void
appendAlterTypeCmdDropColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_DropColumn);
	appendStringInfo(buf, " DROP ATTRIBUTE %s", quote_identifier(alterTableCmd->name));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


static void
appendAlterTypeCmdAlterColumnType(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AlterColumnType);
	appendStringInfo(buf, " ALTER ATTRIBUTE %s SET DATA TYPE ", quote_identifier(
						 alterTableCmd->name));
	appendColumnDef(buf, castNode(ColumnDef, alterTableCmd->def));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
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
	appendTypeNameList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
	appendStringInfoString(buf, ";");
}


static void
appendTypeNameList(StringInfo buf, List *objects)
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
 * appendColumnDef appends the definition of one ColumnDef completely qualified to the
 * provided buffer.
 *
 * If the colname is not set that part is ommitted. This is the case in alter column type
 * statements.
 */
static void
appendColumnDef(StringInfo str, ColumnDef *columnDef)
{
	Oid typeOid = LookupTypeNameOid(NULL, columnDef->typeName, false);
	Oid collationOid = GetColumnDefCollation(NULL, columnDef, typeOid);

	Assert(!columnDef->is_not_null); /* not null is not supported on composite types */

	if (columnDef->colname)
	{
		appendStringInfo(str, "%s ", columnDef->colname);
	}

	appendStringInfo(str, "%s", format_type_be_qualified(typeOid));

	if (OidIsValid(collationOid))
	{
		const char *identifier = format_collate_be_qualified(collationOid);
		appendStringInfo(str, " COLLATE %s", identifier);
	}
}
