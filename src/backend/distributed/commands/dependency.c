/*-------------------------------------------------------------------------
 *
 * dependency.c
 *    Functions to follow and record dependencies for objects to be
 *    created in the right order.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type_d.h"
#include "nodes/pg_list.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/metadata_sync.h"
#include "distributed/commands.h"


static bool ShouldFollowDependency(const ObjectAddress *toFollow);
static bool IsObjectAddressInList(const ObjectAddress *findAddress, List *addressList);
static bool IsObjectAddressOwnedByExtension(const ObjectAddress *target);
static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static List * GetDependenciesCreateDDLCommands(List *dependencies);


/*
 * EnsureDependenciesExists finds all the dependencies that we can distribute and makes
 * sure these are available on all workers. If not available they will be created on the
 * workers via a separate session that will be committed directly so that the objects are
 * visible to potentially multiple sessions creating the shards.
 */
void
EnsureDependenciesExistsOnAllNodes(const ObjectAddress *target)
{
	List *workerNodeList = NULL;
	const uint64 connectionFlag = FORCE_NEW_CONNECTION;
	ListCell *workerNodeCell = NULL;

	List *dependencies = NIL;
	List *ddlCommands = NIL;

	/* collect all dependencies in creation order and get their ddl commands */
	GetDependenciesForObject(target, &dependencies);
	ddlCommands = GetDependenciesCreateDDLCommands(dependencies);

	if (list_length(ddlCommands) <= 0)
	{
		/* no command to execute on workers */
		return;
	}

	workerNodeList = ActivePrimaryNodeList();
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		MultiConnection *connection = NULL;

		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		connection = GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
												   CitusExtensionOwnerName(), NULL);

		ExecuteCriticalRemoteCommandList(connection, ddlCommands);

		/* make sure connection is not reused either */
		CloseConnection(connection);
	}
}


static List *
GetDependenciesCreateDDLCommands(List *dependencies)
{
	List *ddlCommands = NULL;
	ListCell *dependencyCell = NULL;

	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		List *dependencyDDLCommands = GetDependencyCreateDDLCommands(dependency);
		ddlCommands = list_concat(ddlCommands, dependencyDDLCommands);
	}
	return ddlCommands;
}


static List *
GetDependencyCreateDDLCommands(const ObjectAddress *dependency)
{
	switch (getObjectClass(dependency))
	{
		case OCLASS_SCHEMA:
		{
			const char *schemaDDLCommand = CreateSchemaDDLCommand(dependency->objectId);

			if (schemaDDLCommand == NULL)
			{
				/* no schema to create */
				return NIL;
			}

			return list_make1((void *) schemaDDLCommand);
		}

		case OCLASS_TYPE:
		{
			return CreateTypeDDLCommandsIdempotent(dependency);
		}

		default:
		{
			return NIL;
		}
	}
}


/*
 * GetDependenciesForObject returns a list of ObjectAddesses to be created in order
 * before the target object could safely be created on a worker. Some of the object might
 * already be created on a worker. It should be created in an idempotent way.
 */
void
GetDependenciesForObject(const ObjectAddress *target, List **dependencyList)
{
	Relation depRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;

	depRel = heap_open(DependRelationId, AccessShareLock);

	/* scan pg_depend for classid = $1 AND objid = $2 using pg_depend_depender_index */
	ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->classId));
	ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->objectId));
	depScan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		ObjectAddress dependency = { 0 };
		ObjectAddress *dependencyPtr = NULL;
		ObjectAddressSet(dependency, pg_depend->refclassid, pg_depend->refobjid);

		/*
		 * Dependencies are traversed depth first and added to the dependencyList. By
		 * adding them depth first we make sure the dependencies are created in the right
		 * order.
		 *
		 * Dependencies we do not support the creation for are ignored and assumed to be
		 * created on the workers via a different process.
		 */

		if (pg_depend->deptype != DEPENDENCY_NORMAL)
		{
			continue;
		}

		if (IsObjectAddressInList(&dependency, *dependencyList))
		{
			continue;
		}

		if (!ShouldFollowDependency(&dependency))
		{
			continue;
		}

		/* recursion first to cause the depth first behaviour described above */
		GetDependenciesForObject(&dependency, dependencyList);

		/* palloc and copy the dependency entry to be able to add it to the list */
		dependencyPtr = palloc0(sizeof(ObjectAddress));
		*dependencyPtr = dependency;
		*dependencyList = lappend(*dependencyList, dependencyPtr);
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);
}


static bool
IsObjectAddressInList(const ObjectAddress *findAddress, List *addressList)
{
	ListCell *addressCell = NULL;
	foreach(addressCell, addressList)
	{
		ObjectAddress *currentAddress = (ObjectAddress *) lfirst(addressCell);

		/* equality check according as used in postgres for object_address_present */
		if (findAddress->classId == currentAddress->classId && findAddress->objectId ==
			currentAddress->objectId)
		{
			if (findAddress->objectSubId == currentAddress->objectSubId ||
				currentAddress->objectSubId == 0)
			{
				return true;
			}
		}
	}

	return false;
}


static bool
ShouldFollowDependency(const ObjectAddress *toFollow)
{
	/*
	 * objects having a dependency on an extension are assumed to be created by the
	 * extension when that was created on the worker
	 */
	if (IsObjectAddressOwnedByExtension(toFollow))
	{
		return false;
	}

	/*
	 * looking at the type of a object to see if we should follow this dependency to
	 * create on the workers.
	 */
	switch (getObjectClass(toFollow))
	{
		case OCLASS_SCHEMA:
		{
			/* always follow */
			return true;
		}

		case OCLASS_TYPE:
		{
			switch (get_typtype(toFollow->objectId))
			{
				case TYPTYPE_ENUM:
				case TYPTYPE_COMPOSITE:
				{
					return true;
				}

				default:
				{
					/* type not supported */
					return false;
				}
			}

			/*
			 * should be unreachable, break here is to make sure the function has a path
			 * without return, instead of falling through to the next block */
			break;
		}

		default:
		{
			/* unsupported type */
			return false;
		}
	}

	/*
	 * all types should have returned above, compilers complaining about a non return path
	 * indicate a bug int the above switch. Fix it there instead of adding a return here.
	 */
}


static bool
IsObjectAddressOwnedByExtension(const ObjectAddress *target)
{
	Relation depRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;
	bool result = false;

	depRel = heap_open(DependRelationId, AccessShareLock);

	/* scan pg_depend for classid = $1 AND objid = $2 using pg_depend_depender_index */
	ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->classId));
	ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->objectId));
	depScan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		if (pg_depend->deptype == DEPENDENCY_EXTENSION)
		{
			result = true;
			break;
		}
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);

	return result;
}
