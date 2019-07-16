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
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

List *
PlanCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString)
{
	List *workerNodeList = ActivePrimaryNodeList();
	WorkerNode *workerNode = NULL;
	ListCell *workerNodeCell = NULL;

	foreach(workerNodeCell, workerNodeList)
	{
		workerNode = (WorkerNode *) lfirst(workerNodeCell);
		SendCommandToWorker(workerNode->workerName, workerNode->workerPort,
							queryString);
	}

	return NULL;
}

List *
PlanDropTypeStmt(DropStmt *stmt, const char *queryString)
{
	List *workerNodeList = ActivePrimaryNodeList();
	WorkerNode *workerNode = NULL;
	ListCell *workerNodeCell = NULL;

	// TODO test if drop statement for type is for a type that is supported by citus

	foreach(workerNodeCell, workerNodeList)
	{
		workerNode = (WorkerNode *) lfirst(workerNodeCell);
		SendCommandToWorker(workerNode->workerName, workerNode->workerPort,
							queryString);
	}

	return NULL;
}
