/*-------------------------------------------------------------------------
 *
 * distobjectaddress.h
 *	  Declarations for mapping between postgres' ObjectAddress and citus'
 *	  DistObjectAddress.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CATALOG_DISTOBJECTADDRESS_H
#define CITUS_CATALOG_DISTOBJECTADDRESS_H

#include "postgres.h"

#include "catalog/objectaddress.h"


/*
 * DistObjectAddress is the citus equivalent of a postgres ObjectAddress. They both
 * uniquely address and object within postgres. The big reason for citus to represent this
 * differently is the portability between postres versions.
 *
 * Postgres addresses objects by their classId, objectId (and sub object id). The objectId
 * is an Oid that is not stable between postgres upgrades. Instead of referencing the
 * object's by their Oid citus identifies the objects by their qualified identifier.
 *
 * Mapping functions that can map between postgres and citus object addresses are provided
 * to easily work with them.
 */
typedef struct DistObjectAddress
{
	Oid classId;
	const char *identifier;
} DistObjectAddress;


extern DistObjectAddress * getDistObjectAddressFromPg(const ObjectAddress *address);
extern ObjectAddress * getObjectAddresFromCitus(const DistObjectAddress *distAddress);

extern bool IsInPgDistObject(const ObjectAddress *address);
extern void InsertIntoPgDistObjectByAddress(const ObjectAddress *address);
extern void InsertIntoPgDistObject(Oid classId, const char *identifier);

#endif /* CITUS_CATALOG_DISTOBJECTADDRESS_H */
