/*-------------------------------------------------------------------------
 *
 * distributed_intermediate_results.c
 *   Functions for reading and writing distributed intermediate results.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "access/tupdesc.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/sharding.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/syscache.h"


typedef struct NodePair
{
	int sourceNodeId;
	int targetNodeId;
} NodePair;

typedef struct FragmentFetchSet
{
	NodePair nodes;
	List *fragmentsToFetch;
} FragmentFetchSet;

typedef struct TargetShardFragmentStats
{
	int sourceNodeId;
	uint64 sourceShardId;
	long byteCount;
	long rowCount;
} TargetShardFragmentStats;

typedef struct TargetShardFragments
{
	int targetShardIndex;
	List *fragments;
} TargetShardFragments;

typedef struct PredistributionStats
{
	int targetShardCount;
	TargetShardFragments *targetShardFragments;
} PredistributionStats;


static void PartitionDistributedQueryResult(char *distResultId, char *query,
											int distributionColumnIndex,
											int colocationId);
static void WrapTaskListForDistribution(List *taskList, char *resultPrefix, int
										distributionColumnIndex, Oid partitionColumnType, int
										partitionColumnTypeMod,
										ShardInterval **shardArray, int shardCount);
static PredistributionStats * ExecuteJobAndPredistributeResults(Job *job);
static Tuplestorestate * ExecuteJobIntoTupleStore(Job *job, TupleDesc resultDescriptor);
static PredistributionStats * CreatePredistributionStats(int targetShardCount);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(partition_distributed_query_result);


/*
 * partition_distributed_query_result executes a query and writes the results
 * into a set of local files according to the partition scheme and the partition
 * column.
 */
Datum
partition_distributed_query_result(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	text *distResultIdText = PG_GETARG_TEXT_P(0);
	char *distResultIdString = text_to_cstring(distResultIdText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);
	int distributionColumnIndex = PG_GETARG_INT32(2);
	int colocationId = PG_GETARG_INT32(2);

	Tuplestorestate *tupstore = NULL;
	TupleDesc tupleDescriptor = NULL;
	MemoryContext oldcontext = NULL;

	CheckCitusVersion(ERROR);

	/* check to see if query supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}

				 errmsg(
					 "materialize mode required, but it is not allowed in this context")));
	}

	/* get a tuple descriptor for our result type */
	switch (get_call_result_type(fcinfo, NULL, &tupleDescriptor))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		}

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
			break;
		}
	}

	tupleDescriptor = CreateTupleDescCopy(tupleDescriptor);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupleDescriptor;
	MemoryContextSwitchTo(oldcontext);

	PartitionDistributedQueryResult(distResultIdString, queryString,
									distributionColumnIndex, colocationId);

	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}


static void
PartitionDistributedQueryResult(char *distResultId, char *queryString,
								int distributionColumnIndex, int colocationId)
{
	Query *query = NULL;
	int cursorOptions = 0;
	ParamListInfo paramListInfo = NULL;
	PlannedStmt *queryPlan = NULL;
	DistributedPlan *distributedPlan = NULL;
	Job *workerJob = NULL;
	List *taskList = NIL;

	DistTableCacheEntry *tableEntry = NULL;
	int shardCount = 0;
	Oid relationId = InvalidOid;

	Var *partitionColumn = NULL;
	Oid partitionColumnType = InvalidOid;
	int32 partitionColumnTypeMod = 0;

	PredistributionStats *predistributionStats = NULL;

	/* parse the query */
	query = ParseQueryString(queryString);

	/* plan the query */
	queryPlan = pg_plan_query(query, cursorOptions, paramListInfo);
	if (!IsA(queryPlan->planTree, CustomScan))
	{
		/* TODO: fall back to partitioned pull-push */
		ereport(ERROR, (errmsg("query is not a simple distributed query")));
	}

	distributedPlan = GetDistributedPlan((CustomScan *) queryPlan->planTree);
	workerJob = distributedPlan->workerJob;
	taskList = distributedPlan->workerJob->taskList;

	/* TODO: check for weird plans (insert select) */

	relationId = ColocatedTableId(colocationId);
	if (relationId == InvalidOid)
	{
		ereport(ERROR, (errmsg("no relation exists for colocation ID %d", colocationId)));
	}

	tableEntry = DistributedTableCacheEntry(relationId);
	shardCount = tableEntry->shardIntervalArrayLength;

	if (shardCount == 0)
	{
		/* table does not have shards */
		ereport(ERROR, (errmsg("there are no shards for colocation ID %d",
							   colocationId)));
	}

	partitionColumn = PartitionColumn(relationId, 0);
	partitionColumnType = partitionColumn->vartype;
	partitionColumnTypeMod = partitionColumn->vartypmod;

	WrapTaskListForDistribution(taskList, distResultId, distributionColumnIndex,
								partitionColumnType, partitionColumnTypeMod,
								tableEntry->sortedShardIntervalArray,
								shardCount);

	predistributionStats = ExecuteJobAndPredistributeResults(workerJob);
	fetchTaskList = CreateShardFragmentFetchTaskList(predistributionStats,
													 targetDistribution);

}


static List *
CreateShardFragmentFetchTaskList(PredistributionStats *predistributionStats,
								 DistributionScheme *targetDistribution)
{
	List *fetchTaskList = NIL;
	int targetShardIndex = 0;
	HASHCTL info;
	uint32 hashFlags = 0;
	HTAB *targetNodes = NULL;
	FragmentFetchSet *fetchSet = NULL;
	HASH_SEQ_STATUS status;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(NodePair);
	info.entrysize = sizeof(FragmentFetchSet);
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT);

	targetNodes = hash_create("Node pair hash", 32, &info, hashFlags);

	for (targetShardIndex = 0; targetShardIndex < targetShardCount; targetShardIndex++)
	{
		TargetShardFragments *targetShardFragments =
			&(predistributionStats->targetShardFragments[targetShardIndex]);
		ListCell *fragmentCell = NULL;
		int targetNodeId = 0;

		foreach(fragmentCell, targetShardFragments->fragments)
		{
			TargetShardFragmentStats *fragmentStats = lfirst(fragmentCell);
			NodePair nodePair;
			bool foundPair = false;

			nodePair.sourceNodeId = fragmentStats.sourceNodeId;
			nodePair.targetNodeId = targetNodeId;

			fetchSet = hash_search(targetNodes, &nodePair, HASH_ENTER, &foundPair);
			fetchSet->fragmentsToFetch = lappend(fetchSet->fragmentsToFetch,
												 fragmentStats);
		}
	}

	hash_seq_init(&status, targetNodes);

	while ((fetchSet = hash_seq_search(&status)) != NULL)
	{
		Task *task = NULL;



		task = CitusMakeNode(Task);
		task->taskType = SQL_TASK;
		task->queryString = "TODO";
		task->taskPlacementList = NIL;

		fetchTaskList = lappend(fetchTaskList, task);
	}

	return fetchTaskList;
}


static void
WrapTaskListForDistribution(List *taskList, char *resultPrefix, int distributionColumnIndex,
							Oid partitionColumnType, int partitionColumnTypeMod,
							ShardInterval **shardArray, int
							shardCount)
{
	ListCell *taskCell = NULL;
	ArrayType *splitPointObject = SplitPointObject(shardArray, shardCount);
	StringInfo splitPointString = SplitPointArrayString(splitPointObject,
														partitionColumnType,
														partitionColumnTypeMod);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		StringInfo wrappedQuery = makeStringInfo();
		StringInfo taskPrefix = makeStringInfo();
		List *shardPlacementList = task->taskPlacementList;
		ShardPlacement *shardPlacement = NULL;

		if (list_length(shardPlacementList) > 1)
		{
			ereport(ERROR, (errmsg("repartitioning is currently only available for "
								   "queries on distributed tables without replication")));
		}

		shardPlacement = linitial(shardPlacementList);

		appendStringInfo(taskPrefix, "%s_" UINT64_FORMAT, resultPrefix,
						 task->anchorShardId);

		appendStringInfo(wrappedQuery,
						 "SELECT %d, %lld, partition_index, bytes_written, rows_written "
						 "FROM create_hash_partitioned_intermediate_result"
						 "(%s,%s,%d,%s)",
						 shardPlacement->nodeId,
						 task->anchorShardId,
						 quote_literal_cstr(taskPrefix->data),
						 quote_literal_cstr(task->queryString),
						 distributionColumnIndex,
						 splitPointString->data);

		task->queryString = wrappedQuery->data;
	}
}


/*
 * ExecuteJobAndPredistributeResults executes a job an
 */
static PredistributionStats *
ExecuteJobAndPredistributeResults(Job *job, DistributionScheme *targetDistribution)
{
	TupleDesc resultDescriptor = NULL;
	Tuplestorestate *resultStore = NULL;
	int resultColumnCount = 4;
	Oid hasOid = false;
	PredistributionStats *predistributionStats = NULL;

	resultDescriptor = CreateTemplateTupleDesc(resultColumnCount, hasOid);

	TupleDescInitEntry(resultDescriptor, (AttrNumber) 1, "shard_id",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 2, "partition_index",
					   INT4OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 3, "bytes_written",
					   INT8OID, -1, 0);
	TupleDescInitEntry(resultDescriptor, (AttrNumber) 4, "rows_written",
					   INT8OID, -1, 0);

	resultStore = ExecuteJobIntoTupleStore(job, resultDescriptor);
	predistributionStats = TupleStoreToPredistributionStats(resultStore, resultDescriptor,
															targetDistribution);

	return predistributionStats;
}


static Tuplestorestate *
ExecuteJobIntoTupleStore(Job *job, TupleDesc resultDescriptor)
{
	Tuplestorestate *tupleStore = NULL;

	PrepareMasterJobDirectory(job);
	MultiRealTimeExecute(job);

	resultStore = LoadTaskFilesIntoTupleStore(job, resultDescriptor);

	return resultStore;
}


static PredistributionStats *
TupleStoreToPredistributionStats(Tuplestorestate *tupleStore, TupleDesc resultDescriptor,
								  DistributionScheme *targetDistribution)
{
	TupleTableSlot *slot = MakeSingleTupleTableSlot(resultDescriptor);
	PredistributionStats *predistributionStats = NULL;

	predistributionStats = CreatePredistributionStats(targetDistribution->shardCount);

	while (tuplestore_gettupleslot(tupleStore, true, false, slot))
	{
		TargetShardFragmentStats *shardFragmentStats = palloc0(sizeof(TargetShardFragmentStats));
		TargetShardFragments *fragmentSet = NULL;
		List *sourcePlacementList = NIL;
		ShardPlacement *sourcePlacement = NULL;
		int sourceNodeId = 0;
		int64 sourceShardId = 0;
		int targetShardIndex = 0;
		int64 rowCount = 0;
		int64 byteCount = 0;
		bool isNull = false;

		sourceNodeId = DatumGetInt32(slot_getattr(slot, 1, &isNull));
		sourceShardId = DatumGetInt64(slot_getattr(slot, 2, &isNull));
		targetShardIndex = DatumGetInt32(slot_getattr(slot, 3, &isNull));
		byteCount = DatumGetInt64(slot_getattr(slot, 4, &isNull));
		rowCount = DatumGetInt64(slot_getattr(slot, 5, &isNull));

		/* protect against garbage results */
		if (targetShardIndex < 0 || targetShardIndex >= targetDistribution->shardCount)
		{
			ereport(ERROR, (errmsg("target shard index %d out of range", targetShardIndex)));
		}

		shardFragmentStats = palloc0(sizeof(TargetShardFragmentStats));
		shardFragmentStats->sourceNodeId = sourceNodeId;
		shardFragmentStats->sourceShardId = sourceShardId;
		shardFragmentStats->byteCount = byteCount;
		shardFragmentStats->rowCount = rowCount;

		fragmentSet = predistributionStats->targetShardFragments[targetShardIndex];
		fragmentSet->fragments = lappend(fragmentSet->fragments, shardFragmentStats);

		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
}


/*
 * CreatePredistributionStats creates a data structure for holding the statistics
 * returned by executing s Job wrapped in create_hash_partitioned_intermediate_result
 * calls.
 */
static PredistributionStats *
CreatePredistributionStats(int targetShardCount)
{
	PredistributionStats *predistributionStats = palloc0(sizeof(PredistributionStats));
	int targetShardIndex = 0;

	predistributionStats->targetShardCount = targetShardCount;
	predistributionStats->targetShardFragments =
		palloc0(shardCount * sizeof(TargetShardFragments));

	for (targetShardIndex = 0; targetShardIndex < targetShardCount; targetShardIndex++)
	{
		TargetShardFragments *fragmentSet =
			&(predistributionStats->targetShardFragments[targetShardIndex]);

		fragmentSet->targetShardIndex = targetShardIndex;
		fragmentSet->fragments = NIL;
	}

	return predistributionStats;
}
