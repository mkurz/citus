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

#include "distributed/commands.h"
#include "distributed/metadata_sync.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

List *
PlanCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString)
{
	/*
	 * managing types can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here
	 */
	EnsureCoordinator();

	/* to prevent recursion with mx we disable ddl propagation */
	SendCommandToWorkers(ALL_WORKERS, DISABLE_DDL_PROPAGATION);
	SendCommandToWorkers(ALL_WORKERS, queryString);

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
	SendCommandToWorkers(ALL_WORKERS, DISABLE_DDL_PROPAGATION);
	SendCommandToWorkers(ALL_WORKERS, queryString);

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
	SendCommandToWorkers(ALL_WORKERS, DISABLE_DDL_PROPAGATION);
	SendCommandToWorkers(ALL_WORKERS, queryString);

	return NULL;
}
