/*-------------------------------------------------------------------------
 *
 * src/create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include "connection.h"
#include "create_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <optimizer/clauses.h>
#include <prune_shard_list.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <inttypes.h>
#include <access/xact.h>
#include <executor/spi.h>
#include <utils/typcache.h>

#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "pg_shard.h"


/* local function forward declarations */

static List * ParseWorkerNodeFile(char *workerNodeFilename);
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static bool ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(master_create_worker_shards);
PG_FUNCTION_INFO_V1(shard);
PG_FUNCTION_INFO_V1(shardall);
PG_FUNCTION_INFO_V1(master_create_cluster);


Datum shard(PG_FUNCTION_ARGS)
{
	MemoryContext oldContext = MemoryContextSwitchTo(CurTransactionContext);

	text *clusterNameText = PG_GETARG_TEXT_P(0);
	Datum key = PG_GETARG_DATUM(1);
	Oid keyType;
	int64 shardId;
	Datum clusterIdDatum;
	ShardInterval *shardInterval;

	List *shardIntervalList = NIL;
	bool isNull = false;

	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	Oid fetchClusterArgTypes[] = { TEXTOID };
	Datum fetchClusterArgValues[] = {
			PG_GETARG_DATUM(0), // cluster name
	};
	const int fetchClusterArgCount = sizeof(fetchClusterArgValues) / sizeof(fetchClusterArgValues[0]);

	Oid fetchShardsArgTypes[] = { INT4OID, INT4OID };
	Datum fetchShardsArgValues[2];
	const int fetchShardsArgCount = 2;

	Datum hashedValue = 0;
	int hashedValueInt;
	Datum shardIdDatum;
	FmgrInfo *hashFunction = NULL;
	TypeCacheEntry *typeEntry = NULL;


	// Guards
	if (!IsInTransactionChain(true)) {
		ereport(ERROR, (errmsg("shard() can only be run in a transaction!")));
	}

	if (GetShardInfo()->shardList != NULL) {
		ereport(ERROR, (errmsg("shard() already set in this transaction!")));
	}

	ereport(LOG, (errmsg("Starting sharded transaction on cluster '%s'", text_to_cstring(clusterNameText))));

	// Fetch cluster id and key type
	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT id, key_type FROM pgs_distribution_metadata.cluster "
											  "WHERE name = $1", fetchClusterArgCount, fetchClusterArgTypes,
									  fetchClusterArgValues, NULL, false, 0);

	Assert(spiStatus == SPI_OK_SELECT);

	clusterIdDatum = (SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								   1, &isNull));


	keyType = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
											2, &isNull));
	SPI_finish();


	// Calculate the hash of the sharding key
	typeEntry = lookup_type_cache(keyType, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);
	if (!OidIsValid(hashFunction->fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("could not identify a hash function for type %s",
					   format_type_be(keyType)),
				errdatatype(keyType)));
	}

	hashedValue = FunctionCall1(hashFunction, key);
	hashedValueInt = DatumGetInt32(hashedValue);

	ereport(LOG, (errmsg("Using hashed key '%d'", hashedValueInt)));

	fetchShardsArgValues[0] = clusterIdDatum;
	fetchShardsArgValues[1] = hashedValue;

	// Fetch the shards
	SPI_connect();
	spiStatus = SPI_execute_with_args("SELECT id FROM pgs_distribution_metadata.shard AS cs "
											    "WHERE cs.cluster_id = $1 AND min_value <= $2 AND max_value >= $2",
									  fetchShardsArgCount, fetchShardsArgTypes,
									  fetchShardsArgValues, NULL, false, 0);

	Assert(spiStatus == SPI_OK_SELECT);

	shardIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
				  1, &isNull);



	shardId = DatumGetInt64(shardIdDatum);

	SPI_finish();

	shardInterval = palloc0(sizeof(ShardInterval));
	shardInterval->id = shardId;
	shardInterval->valueTypeId = keyType;
	shardIntervalList = lappend(shardIntervalList, shardInterval);


	ExecuteDistributedStatementOnShards(shardIntervalList, "BEGIN;");

	SetShardInfo(shardIntervalList);
	MemoryContextSwitchTo(oldContext);

	PG_RETURN_VOID();
}

Datum shardall(PG_FUNCTION_ARGS)
{
	MemoryContext spiContext;
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	text *clusterNameText = PG_GETARG_TEXT_P(0);
	List *shardIntervalList = NIL;
	bool isNull = false;

	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	Oid fetchShardsArgTypes[] = { TEXTOID };
	Datum fetchShardsArgValues[] = {
			PG_GETARG_DATUM(0), // cluster name
	};

	const int fetchShardsArgCount = sizeof(fetchShardsArgValues) / sizeof(fetchShardsArgValues[0]);


	// Guards
	if (!IsInTransactionChain(true)) {
		ereport(ERROR, (errmsg("shard() can only be run in a transaction!")));
	}

	if (GetShardInfo()->shardList != NULL) {
		ereport(ERROR, (errmsg("shard() already set in this transaction!")));
	}

	ereport(LOG, (errmsg("Starting sharded transaction on cluster '%s'", text_to_cstring(clusterNameText))));

	ereport(LOG, (errmsg("Using hashed key ALL")));

	// Fetch all shards
	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT cs.id FROM pgs_distribution_metadata.shard AS cs "
											 "JOIN pgs_distribution_metadata.cluster AS c ON (c.id = cs.cluster_id AND c.name = $1)",
									  fetchShardsArgCount, fetchShardsArgTypes,
									  fetchShardsArgValues, NULL, false, 0);

	Assert(spiStatus == SPI_OK_SELECT);

	// TODO: Figure out how to get around the awkward memory dance
	spiContext = MemoryContextSwitchTo(TopTransactionContext);
	for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
	{
		ShardInterval *shardInterval;
		int64 shardId;
		Datum shardIdDatum = SPI_getbinval(SPI_tuptable->vals[rowNumber], SPI_tuptable->tupdesc,
									 1, &isNull);

		shardId = DatumGetInt64(shardIdDatum);

		shardInterval = palloc0(sizeof(ShardInterval));
		shardInterval->id = shardId;

		shardIntervalList = lappend(shardIntervalList, shardInterval);
	}

	MemoryContextSwitchTo(spiContext);

	SPI_finish();

	ExecuteDistributedStatementOnShards(shardIntervalList, "BEGIN;");

	SetShardInfo(shardIntervalList);

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_VOID();
}

Datum master_create_cluster(PG_FUNCTION_ARGS) {
	int32 shardCount = PG_GETARG_INT32(2);
	int32 replicationFactor = PG_GETARG_INT32(3);
	int32 shardIndex;
	int32 workerNodeCount = 0;
	uint32 hashTokenIncrement = 0;
	bool isNull = false;
	Datum clusterIdDatum;
	int32 clusterId;
	Oid createClusterArgTypes[] = { TEXTOID, REGTYPEOID };
	Datum createClusterArgValues[] = {
		PG_GETARG_DATUM(0), // cluster name
		PG_GETARG_DATUM(1)  // key type
	};
	const int createClusterArgCount = sizeof(createClusterArgValues) / sizeof(createClusterArgValues[0]);


	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	// Create the cluster
	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO pgs_distribution_metadata.cluster "
											  "(name, key_type) "
											  "VALUES ($1, $2) RETURNING id", createClusterArgCount, createClusterArgTypes,
									  createClusterArgValues, NULL, false, 0);

	Assert(spiStatus == SPI_OK_INSERT);

	clusterIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								  1, &isNull);
	clusterId = DatumGetInt32(clusterIdDatum);

	SPI_finish();


	/* calculate the split of the hash space */
	hashTokenIncrement = UINT_MAX / shardCount;

	// Create shards
	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		int64 shardId = -1;
		int32 placementCount = 0;
		uint32 placementIndex = 0;
		uint32 roundRobinNodeIndex;
		uint32 placementAttemptCount = 0;
		List *workerNodeList = NIL;


		int32 shardMinHashToken = INT_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + hashTokenIncrement - 1;

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT_MAX;
		}

		/* insert the shard metadata row along with its min/max values */
		shardId = CreateShardRow(clusterId, shardMinHashToken,
										shardMaxHashToken);

		/* load and sort the worker node list for deterministic placement */
		workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
		workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

		/* make sure we don't process cancel signals until all shards are created */
		HOLD_INTERRUPTS();

		workerNodeCount = list_length(workerNodeList);
		if (replicationFactor > workerNodeCount)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("replication_factor (%d) exceeds number of worker nodes "
								   "(%d)", replicationFactor, workerNodeCount),
					errhint("Add more worker nodes or try again with a lower "
									"replication factor.")));
		}

		roundRobinNodeIndex = shardIndex % workerNodeCount;

		/* if we have enough nodes, add an extra placement attempt for backup */
		placementAttemptCount = (uint32) replicationFactor;
		if (workerNodeCount > replicationFactor)
		{
			placementAttemptCount++;
		}


		/*
		 * Grabbing the shard metadata lock isn't technically necessary since
		 * we already hold an exclusive lock on the partition table, but we'll
		 * acquire it for the sake of completeness. As we're adding new active
		 * placements, the mode must be exclusive.
		 */
		LockShardDistributionMetadata(shardId, ExclusiveLock);

		// Create shard placements
		for (placementIndex = 0; placementIndex < placementAttemptCount; placementIndex++)
		{
			int32 candidateNodeIndex = (roundRobinNodeIndex + placementIndex) % workerNodeCount;

			WorkerNode *candidateNode = (WorkerNode *) list_nth(workerNodeList,
																candidateNodeIndex);
			char *nodeName = candidateNode->nodeName;
			uint32 nodePort = candidateNode->nodePort;

			CreateShardPlacementRow(shardId, STATE_FINALIZED, nodeName, nodePort);
			placementCount++;

			if (placementCount >= replicationFactor)
			{
				break;
			}
		}

		/* check if we created enough shard replicas */
		if (placementCount < replicationFactor)
		{
			ereport(ERROR, (errmsg("could not satisfy specified replication factor"),
					errdetail("Created %d shard replicas, less than the "
									  "requested replication factor of %d.",
							  placementCount, replicationFactor)));
		}

		RESUME_INTERRUPTS();
	}

	PG_RETURN_VOID();
}

/*
 * master_create_distributed_table inserts the table and partition column
 * information into the partition metadata table. Note that this function
 * currently assumes the table is hash partitioned.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	text *partitionColumnText = PG_GETARG_TEXT_P(1);
	char partitionMethod = PG_GETARG_CHAR(2);
	Oid distributedTableId = ResolveRelationId(tableNameText);
	char relationKind = '\0';
	char *partitionColumnName = text_to_cstring(partitionColumnText);
	char *tableName = text_to_cstring(tableNameText);
	Var *partitionColumn = NULL;

	/* verify target relation is either regular or foreign table */
	relationKind = get_rel_relkind(distributedTableId);
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("cannot distribute relation: %s", tableName),
						errdetail("Distributed relations must be regular or "
								  "foreign tables.")));
	}

	/* this will error out if no column exists with the specified name */
	partitionColumn = ColumnNameToColumn(distributedTableId, partitionColumnName);

	/* check for support function needed by specified partition method */
	if (partitionMethod == HASH_PARTITION_TYPE)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(partitionColumn, HASH_AM_OID,
														   HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(partitionColumn->vartype)),
							errdatatype(partitionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}
	}
	else if (partitionMethod == RANGE_PARTITION_TYPE)
	{
		Oid btreeSupportFunction = InvalidOid;

		/*
		 * Error out immediately since we don't yet support range partitioning,
		 * but the checks below are ready for when we do.
		 *
		 * TODO: Remove when range partitioning is supported.
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_shard only supports hash partitioning")));

		btreeSupportFunction = SupportFunctionForColumn(partitionColumn, BTREE_AM_OID,
														BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(partitionColumn->vartype)),
					 errdatatype(partitionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unrecognized table partition type: %c",
							   partitionMethod)));
	}

	/* insert row into the partition metadata table */
	InsertPartitionRow(distributedTableId, partitionMethod, partitionColumnText);

	PG_RETURN_VOID();
}


/* Finds the relationId from a potentially qualified relation name. */
Oid
ResolveRelationId(text *relationName)
{
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	Oid relationId = InvalidOid;
	bool failOK = false;        /* error if relation cannot be found */

	/* resolve relationId from passed in schema and relation name */
	relationNameList = textToQualifiedNameList(relationName);
	relation = makeRangeVarFromNameList(relationNameList);
	relationId = RangeVarGetRelid(relation, NoLock, failOK);

	return relationId;
}

/*
 * ParseWorkerNodeFile opens and parses the node name and node port from the
 * specified configuration file. The function relies on the file being at the
 * top level in the data directory.
 */
static List *
ParseWorkerNodeFile(char *workerNodeFilename)
{
	FILE *workerFileStream = NULL;
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(workerNodeFilename);
	char workerLinePattern[1024];
	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open worker list file \"%s\": %m",
							   workerFilePath)));
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), "%%%us%%*[ \t]%%10u",
			 MAX_NODE_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		uint32 nodePort = 0;
		int parsedValues = 0;
		char nodeName[MAX_NODE_LENGTH + 1];
		memset(nodeName, '\0', sizeof(nodeName));

		if (strnlen(workerNodeLine, MAXPGPATH) == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line exceeds the maximum "
								   "length of %d", MAXPGPATH)));
		}

		/* skip leading whitespace and check for # comment */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse out the node name and node port */
		parsedValues = sscanf(workerNodeLine, workerLinePattern, nodeName, &nodePort);
		if (parsedValues != 2)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("could not parse worker node line: %s",
								   workerNodeLine),
							errhint("Lines in the worker node file consist of a node "
									"name and port separated by whitespace. Lines that "
									"start with a '#' character are skipped.")));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
		workerNode->nodeName = palloc(sizeof(char) * MAX_NODE_LENGTH + 1);
		strlcpy(workerNode->nodeName, nodeName, MAX_NODE_LENGTH + 1);
		workerNode->nodePort = nodePort;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	free(workerFilePath);

	return workerNodeList;
}


/*
 * SortList takes in a list of void pointers, and sorts these pointers (and the
 * values they point to) by applying the given comparison function. The function
 * then returns the sorted list of pointers.
 */
List *
SortList(List *pointerList, int (*ComparisonFunction)(const void *, const void *))
{
	List *sortedList = NIL;
	uint32 arrayIndex = 0;
	uint32 arraySize = (uint32) list_length(pointerList);
	void **array = (void **) palloc0(arraySize * sizeof(void *));

	ListCell *pointerCell = NULL;
	foreach(pointerCell, pointerList)
	{
		void *pointer = lfirst(pointerCell);
		array[arrayIndex] = pointer;

		arrayIndex++;
	}

	/* sort the array of pointers using the comparison function */
	qsort(array, arraySize, sizeof(void *), ComparisonFunction);

	/* convert the sorted array of pointers back to a sorted list */
	for (arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
	{
		void *sortedPointer = array[arrayIndex];
		sortedList = lappend(sortedList, sortedPointer);
	}

	return sortedList;
}


/* Helper function to compare two workers by their node name and port number. */
static int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const WorkerNode *leftNode = *((const WorkerNode **) leftElement);
	const WorkerNode *rightNode = *((const WorkerNode **) rightElement);

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(leftNode->nodeName, rightNode->nodeName, MAX_NODE_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = (int) (leftNode->nodePort - rightNode->nodePort);
	return portCompare;
}


/*
 * ExecuteRemoteCommandList executes the given commands in a single transaction
 * on the specified node.
 */
bool
ExecuteRemoteCommandList(char *nodeName, uint32 nodePort, List *sqlCommandList)
{
	bool commandListExecuted = true;
	ListCell *sqlCommandCell = NULL;
	bool sqlCommandIssued = false;
	bool beginIssued = false;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		return false;
	}

	/* begin a transaction before we start executing commands */
	beginIssued = ExecuteRemoteCommand(connection, BEGIN_COMMAND);
	if (!beginIssued)
	{
		return false;
	}

	foreach(sqlCommandCell, sqlCommandList)
	{
		char *sqlCommand = (char *) lfirst(sqlCommandCell);

		sqlCommandIssued = ExecuteRemoteCommand(connection, sqlCommand);
		if (!sqlCommandIssued)
		{
			break;
		}
	}

	if (sqlCommandIssued)
	{
		bool commitIssued = ExecuteRemoteCommand(connection, COMMIT_COMMAND);
		if (!commitIssued)
		{
			commandListExecuted = false;
		}
	}
	else
	{
		ExecuteRemoteCommand(connection, ROLLBACK_COMMAND);
		commandListExecuted = false;
	}

	return commandListExecuted;
}


/*
 * ExecuteRemoteCommand executes the given sql command on the remote node, and
 * returns true if the command executed successfully. The command is allowed to
 * return tuples, but they are not inspected: this function simply reflects
 * whether the command succeeded or failed.
 */
static bool
ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand)
{
	PGresult *result = PQexec(connection, sqlCommand);
	bool commandSuccessful = true;

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(connection, result);
		commandSuccessful = false;
	}

	PQclear(result);
	return commandSuccessful;
}


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}
