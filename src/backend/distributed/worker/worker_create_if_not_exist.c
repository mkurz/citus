/*-------------------------------------------------------------------------
 *
 * worker_create_if_not_exist.c
 *     Implements the worker logic to execute create if not exists commands
 *     even when postgres does not have the IF NOT EXISTS modifier for the
 *     object.
 *
 *     This is done by sending the sql statement as the first argument to
 *     the worker_create_if_not_exists udf. The statement is parsed, and
 *     the relation is searched for in the catalog. If it is not found the
 *     create statement will be executed.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "parser/parse_type.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/regproc.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/worker_protocol.h"

PG_FUNCTION_INFO_V1(worker_create_if_not_exists);
PG_FUNCTION_INFO_V1(type_recreate_command);

/*
 * worker_create_if_not_exists(sqlStatement text)
 *
 * The sqlStatement is parsed and interpreted to find the relation it is supposed to
 * create. The command will only be executed if the relation does not already exist. The
 * relation is not checked to see if they are different. It is assumed that if the
 * relation exists it should not be created.
 */
Datum
worker_create_if_not_exists(PG_FUNCTION_ARGS)
{
	text *sqlStatementText = PG_GETARG_TEXT_P(0);
	const char *sqlStatement = text_to_cstring(sqlStatementText);

	Node *parseTree = ParseTreeNode(sqlStatement);

	switch (parseTree->type)
	{
		case T_CompositeTypeStmt:
		{
			if (CompositeTypeExists(castNode(CompositeTypeStmt, parseTree)))
			{
				return BoolGetDatum(false);
			}

			/* type does not exist, fall through to create type */
			break;
		}

		case T_CreateEnumStmt:
		{
			if (EnumTypeExists(castNode(CreateEnumStmt, parseTree)))
			{
				return BoolGetDatum(false);
			}

			/* type does not exist, fall through to create type */
			break;
		}

		default:
		{
			/*
			 * should not be reached, indicates the coordinator is sending unsupported
			 * statements
			 */
			ereport(ERROR, (errmsg("unsupported create statement for "
								   "worker_create_if_not_exists")));
			return BoolGetDatum(false);
		}
	}

	CitusProcessUtility(parseTree, sqlStatement, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	/* type has been created */
	return BoolGetDatum(true);
}


/*
 * type_recreate_command(typename text) text
 */
Datum
type_recreate_command(PG_FUNCTION_ARGS)
{
	text *typeNameText = PG_GETARG_TEXT_P(0);
	const char *typeNameStr = text_to_cstring(typeNameText);
	List *typeNameList = stringToQualifiedNameList(typeNameStr);
	TypeName *typeName = makeTypeNameFromNameList(typeNameList);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, false);

	Node *stmt = RecreateTypeStatement(typeOid);

	switch (stmt->type)
	{
		case T_CreateEnumStmt:
		{
			const char *createEnumStmtSql = NULL;
			createEnumStmtSql = deparse_create_enum_stmt(castNode(CreateEnumStmt, stmt));
			return CStringGetTextDatum(createEnumStmtSql);
		}

		case T_CompositeTypeStmt:
		{
			const char *compositeTypeStmtSql = NULL;
			compositeTypeStmtSql = deparse_composite_type_stmt(castNode(CompositeTypeStmt,
																		stmt));
			return CStringGetTextDatum(compositeTypeStmtSql);
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported statement for deparse")));
		}
	}

	PG_RETURN_VOID();
}
