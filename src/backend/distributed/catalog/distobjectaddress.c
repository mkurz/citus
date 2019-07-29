/*-------------------------------------------------------------------------
 *
 * distobjectaddress.c
 *	  Functions to work with object addresses of distributed objects.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "access/skey.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "utils/fmgroids.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/builtins.h"

#include "distributed/catalog/distobjectaddress.h"
#include "distributed/catalog/pg_dist_object.h"
#include "distributed/metadata_cache.h"


/*
 * getDistObjectAddressFromPg maps a postgres object address to a citus distributed object
 * address.
 */
DistObjectAddress *
getDistObjectAddressFromPg(const ObjectAddress *address)
{
	DistObjectAddress *distAddress = palloc0(sizeof(DistObjectAddress));

	distAddress->classId = address->classId;
	distAddress->identifier = getObjectIdentity(address);

	return distAddress;
}


/*
 * getObjectAddresFromCitus maps a citus distributed object address to a postgres object
 * address
 */
ObjectAddress *
getObjectAddresFromCitus(const DistObjectAddress *distAddress)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	switch (distAddress->classId)
	{
		case TypeRelationId:
		{
			/* resolve identifier to typeOid for qualified identifier */
			List *names = stringToQualifiedNameList(distAddress->identifier);
			TypeName *typeName = makeTypeNameFromNameList(names);
			Oid typeOid = LookupTypeNameOid(NULL, typeName, false);

			/* fill in the pre allocated object address */
			ObjectAddressSet(*address, TypeRelationId, typeOid);

			return address;
		}

		case NamespaceRelationId:
		{
			List *names = NIL;
			const char *namespaceName = NULL;
			Oid namespaceOid = InvalidOid;

			names = stringToQualifiedNameList(distAddress->identifier);
			Assert(list_length(names) == 1);

			namespaceName = strVal(linitial(names));
			namespaceOid = get_namespace_oid(namespaceName, false);

			ObjectAddressSet(*address, NamespaceRelationId, namespaceOid);

			return address;
		};

		default:
		{
			ereport(ERROR, (errmsg("unrecognized object class: %u",
								   distAddress->classId)));
		}
	}
}


/*
 * makeDistObjectAddress creates a newly allocated DistObjectAddress that points to the
 * classId and identifier passed. No checks are performed to verify the object exists.
 */
DistObjectAddress *
makeDistObjectAddress(Oid classid, const char *identifier)
{
	DistObjectAddress *distAddress = palloc0(sizeof(DistObjectAddress));
	distAddress->classId = classid;
	distAddress->identifier = pstrdup(identifier);
	return distAddress;
}


/*
 * recordObjectDistributedByAddress marks an object as a distributed object by is postgres
 * address.
 */
void
recordObjectDistributedByAddress(const ObjectAddress *address)
{
	recordObjectDistributed(getDistObjectAddressFromPg(address));
}


/*
 * recordObjectDistributed mark an object as a distributed object in citus.
 */
void
recordObjectDistributed(const DistObjectAddress *distAddress)
{
	Relation pgDistObject = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_object];
	bool newNulls[Natts_pg_dist_object];

	/* open system catalog and insert new tuple */
	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_object_classid - 1] = ObjectIdGetDatum(distAddress->classId);
	newValues[Anum_pg_dist_object_identifier - 1] = CStringGetTextDatum(
		distAddress->identifier);

	newTuple = heap_form_tuple(RelationGetDescr(pgDistObject), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	CatalogTupleInsert(pgDistObject, newTuple);

	CommandCounterIncrement();
	heap_close(pgDistObject, NoLock);
}


/*
 * isObjectDistributedByAddress returns if the postgres object addressed by address is
 * know to be distributed by citus.
 */
bool
isObjectDistributedByAddress(const ObjectAddress *address)
{
	return isObjectDistributed(getDistObjectAddressFromPg(address));
}


/*
 * isObjectDistributed returns if the object identified by the distAddress is already
 * distributed in the cluster. This performs a local lookup in pg_dist_object.
 */
bool
isObjectDistributed(const DistObjectAddress *distAddress)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;
	bool result = false;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = $1 AND identifier = $2 */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_identifier, BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(distAddress->identifier));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel, InvalidOid, false, NULL, 2,
										  key);

	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		/* tuplpe found, we are done */
		result = true;
		break;
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return result;
}
