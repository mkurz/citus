/*-------------------------------------------------------------------------
 *
 * extended_op_node_utils.c implements the logic for building the necessary
 * information that is shared among both the worker and master extended
 * op nodes.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/extended_op_node_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_partition.h"
#include "compat/optimizer/var.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"


static bool GroupedByDisjointPartitionColumn(List *tableNodeList,
											 MultiExtendedOp *opNode);
static bool ExtendedOpNodeContainsRepartitionSubquery(MultiExtendedOp *originalOpNode);

static bool HasNonPartitionColumnDistinctAgg(List *targetEntryList, Node *havingQual,
											 List *tableNodeList);
static bool PartitionColumnInTableList(Var *column, List *tableNodeList);
static bool ShouldPullDistinctColumn(bool repartitionSubquery,
									 bool groupedByDisjointPartitionColumn,
									 bool hasNonPartitionColumnDistinctAgg);


/*
 * BuildExtendedOpNodeProperties is a helper function that simply builds
 * the necessary information for processing the extended op node. The return
 * value should be used in a read-only manner.
 */
ExtendedOpNodeProperties
BuildExtendedOpNodeProperties(MultiExtendedOp *extendedOpNode)
{
	ExtendedOpNodeProperties extendedOpNodeProperties;
	List *tableNodeList = NIL;
	List *targetList = NIL;
	Node *havingQual = NULL;

	bool groupedByDisjointPartitionColumn = false;
	bool repartitionSubquery = false;
	bool hasNonPartitionColumnDistinctAgg = false;
	bool pullDistinctColumns = false;
	bool pushDownWindowFunctions = false;

	tableNodeList = FindNodesOfType((MultiNode *) extendedOpNode, T_MultiTable);
	groupedByDisjointPartitionColumn = GroupedByDisjointPartitionColumn(tableNodeList,
																		extendedOpNode);

	repartitionSubquery = ExtendedOpNodeContainsRepartitionSubquery(extendedOpNode);

	targetList = extendedOpNode->targetList;
	havingQual = extendedOpNode->havingQual;
	hasNonPartitionColumnDistinctAgg =
		HasNonPartitionColumnDistinctAgg(targetList, havingQual, tableNodeList);

	pullDistinctColumns =
		ShouldPullDistinctColumn(repartitionSubquery, groupedByDisjointPartitionColumn,
								 hasNonPartitionColumnDistinctAgg);

	/*
	 * TODO: Only window functions that can be pushed down reach here, thus,
	 * using hasWindowFuncs is safe for now. However, this should be fixed
	 * when we support pull-to-master window functions.
	 */
	pushDownWindowFunctions = extendedOpNode->hasWindowFuncs;

	extendedOpNodeProperties.groupedByDisjointPartitionColumn =
		groupedByDisjointPartitionColumn;
	extendedOpNodeProperties.repartitionSubquery = repartitionSubquery;
	extendedOpNodeProperties.hasNonPartitionColumnDistinctAgg =
		hasNonPartitionColumnDistinctAgg;
	extendedOpNodeProperties.pullDistinctColumns = pullDistinctColumns;
	extendedOpNodeProperties.pushDownWindowFunctions = pushDownWindowFunctions;

	return extendedOpNodeProperties;
}


/*
 * GroupedByDisjointPartitionColumn returns true if the query is grouped by the
 * partition column of a table whose shards have disjoint sets of partition values.
 */
static bool
GroupedByDisjointPartitionColumn(List *tableNodeList, MultiExtendedOp *opNode)
{
	bool result = false;
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		Oid relationId = tableNode->relationId;
		char partitionMethod = 0;

		if (relationId == SUBQUERY_RELATION_ID || !IsDistributedTable(relationId))
		{
			continue;
		}

		partitionMethod = PartitionMethod(relationId);
		if (partitionMethod != DISTRIBUTE_BY_RANGE &&
			partitionMethod != DISTRIBUTE_BY_HASH)
		{
			continue;
		}

		if (GroupedByColumn(opNode->groupClauseList, opNode->targetList,
							tableNode->partitionColumn))
		{
			result = true;
			break;
		}
	}

	return result;
}


/*
 * ExtendedOpNodeContainsRepartitionSubquery is a utility function that
 * returns true if the extended op node contains a re-partition subquery.
 */
static bool
ExtendedOpNodeContainsRepartitionSubquery(MultiExtendedOp *originalOpNode)
{
	MultiNode *parentNode = ParentNode((MultiNode *) originalOpNode);
	MultiNode *childNode = ChildNode((MultiUnaryNode *) originalOpNode);

	if (CitusIsA(parentNode, MultiTable) && CitusIsA(childNode, MultiCollect))
	{
		return true;
	}

	return false;
}


/*
 * HasNonPartitionColumnDistinctAgg returns true if target entry or having qualifier
 * has non-partition column reference in aggregate (distinct) definition. Note that,
 * it only checks aggs subfield of Aggref, it does not check FILTER or SORT clauses.
 * Having any non-column reference like operator expression, function call, or const
 * is considered as a non-partition column. Even if the expression contains partition column
 * like (column + 1), it needs to be evaluated at coordinator, since we can't reliably verify
 * the distinctness of the expression result like (column % 5) or (column + column).
 */
static bool
HasNonPartitionColumnDistinctAgg(List *targetEntryList, Node *havingQual,
								 List *tableNodeList)
{
	List *targetVarList = pull_var_clause((Node *) targetEntryList,
										  PVC_INCLUDE_AGGREGATES |
										  PVC_RECURSE_WINDOWFUNCS);

	/* having clause can't have window functions, no need to recurse for that */
	List *havingVarList = pull_var_clause((Node *) havingQual, PVC_INCLUDE_AGGREGATES);
	List *aggregateCheckList = list_concat(targetVarList, havingVarList);

	ListCell *aggregateCheckCell = NULL;
	foreach(aggregateCheckCell, aggregateCheckList)
	{
		Node *targetNode = lfirst(aggregateCheckCell);
		Aggref *targetAgg = NULL;
		List *varList = NIL;
		ListCell *varCell = NULL;
		bool isPartitionColumn = false;
		TargetEntry *firstTargetEntry = NULL;
		Node *firstTargetExprNode = NULL;

		if (IsA(targetNode, Var))
		{
			continue;
		}

		Assert(IsA(targetNode, Aggref));
		targetAgg = (Aggref *) targetNode;
		if (targetAgg->aggdistinct == NIL)
		{
			continue;
		}

		/*
		 * We are dealing with a more complex count distinct, it needs to be
		 * evaluated at coordinator level.
		 */
		if (list_length(targetAgg->args) > 1 || list_length(targetAgg->aggdistinct) > 1)
		{
			return true;
		}

		firstTargetEntry = linitial_node(TargetEntry, targetAgg->args);
		firstTargetExprNode = strip_implicit_coercions((Node *) firstTargetEntry->expr);
		if (!IsA(firstTargetExprNode, Var))
		{
			return true;
		}

		varList = pull_var_clause_default((Node *) targetAgg->args);
		foreach(varCell, varList)
		{
			Node *targetVar = (Node *) lfirst(varCell);

			Assert(IsA(targetVar, Var));

			isPartitionColumn =
				PartitionColumnInTableList((Var *) targetVar, tableNodeList);

			if (!isPartitionColumn)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * PartitionColumnInTableList returns true if provided column is a partition
 * column from provided table node list. It also returns false if a column is
 * partition column of an append distributed table.
 */
static bool
PartitionColumnInTableList(Var *column, List *tableNodeList)
{
	ListCell *tableNodeCell = NULL;
	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = lfirst(tableNodeCell);
		Var *partitionColumn = tableNode->partitionColumn;

		if (partitionColumn != NULL &&
			partitionColumn->varno == column->varno &&
			partitionColumn->varattno == column->varattno)
		{
			Assert(partitionColumn->varno == tableNode->rangeTableId);

			if (PartitionMethod(tableNode->relationId) != DISTRIBUTE_BY_APPEND)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * ShouldPullDistinctColumn returns true if distinct aggregate should pull
 * individual columns from worker to master and evaluate aggregate operation
 * at master.
 *
 * Pull cases are:
 * - repartition subqueries
 * - query has count distinct on a non-partition column on at least one target
 * - count distinct is on a non-partition column and query is not
 *   grouped on partition column
 */
static bool
ShouldPullDistinctColumn(bool repartitionSubquery,
						 bool groupedByDisjointPartitionColumn,
						 bool hasNonPartitionColumnDistinctAgg)
{
	if (repartitionSubquery)
	{
		return true;
	}

	if (groupedByDisjointPartitionColumn)
	{
		return false;
	}
	else if (!groupedByDisjointPartitionColumn && hasNonPartitionColumnDistinctAgg)
	{
		return true;
	}

	return false;
}
