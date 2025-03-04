/**
 * Query Coordinator canister type definitions
 * 
 * This module contains type definitions specific to the Query Coordinator Canister,
 * which is responsible for parsing, planning, distributing, and aggregating queries.
 */

import Time "mo:base/Time";
import Principal "mo:base/Principal";
import CommonTypes "../common/types";

module {
    // Import common types
    public type DatabaseId = CommonTypes.DatabaseId;
    public type ShardId = CommonTypes.ShardId;
    public type TransactionId = CommonTypes.TransactionId;
    public type Query = CommonTypes.Query;
    public type ResultSet = CommonTypes.ResultSet;
    public type FilterExpression = CommonTypes.FilterExpression;
    public type Key = CommonTypes.Key;
    public type Value = CommonTypes.Value;
    public type Record = CommonTypes.Record;
    
    // ===== Query Planning Types =====
    
    /// Represents an execution plan for a query
    public type ExecutionPlan = {
        queryId : Text;
        query : Query;
        planType : PlanType;
        shardPlans : [ShardPlan];
        aggregationType : ?AggregationType;
        costEstimate : CostEstimate;
    };
    
    /// Represents the type of execution plan
    public type PlanType = {
        #SingleShard;      // Execute query on one shard only
        #Broadcast;        // Execute same query on multiple shards
        #ScatterGather;    // Distribute query and aggregate results
        #Pipeline;         // Execute stages of the query in sequence
    };
    
    /// Represents a plan specific to a shard
    public type ShardPlan = {
        shardId : ShardId;
        operations : [ShardOperation];
        expectedResultSize : Nat;
        priority : Nat;
    };
    
    /// Represents an operation to be executed on a shard
    public type ShardOperation = {
        #Scan : {
            tableName : Text;
            filter : ?FilterExpression;
        };
        #IndexScan : {
            tableName : Text;
            indexName : Text;
            filter : ?FilterExpression;
        };
        #PointLookup : {
            tableName : Text;
            key : Key;
        };
        #Insert : {
            tableName : Text;
            records : [Record];
        };
        #Update : {
            tableName : Text;
            updates : [(Text, Value)];
            filter : ?FilterExpression;
        };
        #Delete : {
            tableName : Text;
            filter : ?FilterExpression;
        };
        #Join : {
            leftTable : Text;
            rightTable : Text;
            condition : FilterExpression;
            joinType : CommonTypes.JoinType;
        };
        #Aggregate : {
            operations : [AggregateOperation];
            groupBy : [Text];
        };
        #Sort : {
            columns : [(Text, {#Ascending; #Descending})];
        };
        #Limit : {
            count : Nat;
            offset : Nat;
        };
        #Project : {
            columns : [Text];
        };
    };
    
    /// Represents an aggregation operation
    public type AggregateOperation = {
        #Count : ?Text;
        #Sum : Text;
        #Avg : Text;
        #Min : Text;
        #Max : Text;
    };
    
    /// Represents a result aggregation type
    public type AggregationType = {
        #Union;           // Combine results
        #Merge;           // Merge sorted results
        #GroupAggregate;  // Combine and apply group functions
        #Join;            // Join results from different shards
    };
    
    /// Represents a cost estimate for query execution
    public type CostEstimate = {
        estimatedRows : Nat;
        estimatedCycles : Nat64;
        estimatedExecutionTime : Nat64;
    };
    
    // ===== Query Execution Types =====
    
    /// Represents a request to execute a query
    public type QueryRequest = {
        databaseId : DatabaseId;
        query : Text;      // SQL-like query string
        timeout : ?Nat64;  // Timeout in nanoseconds
        consistency : ?ConsistencyLevel;
    };
    
    /// Represents a response from executing a query
    public type QueryResponse = {
        queryId : Text;
        resultSet : ResultSet;
        executionTime : Nat64;
        cyclesConsumed : Nat64;
        warnings : [Text];
    };
    
    /// Represents a shard query
    public type ShardQuery = {
        queryId : Text;
        databaseId : DatabaseId;
        operations : [ShardOperation];
        transactionId : ?TransactionId;
    };
    
    /// Represents a shard result
    public type ShardResult = {
        queryId : Text;
        resultSet : ResultSet;
        executionTime : Nat64;
        cyclesConsumed : Nat64;
        warnings : [Text];
    };
    
    /// Represents a consistency level for queries
    public type ConsistencyLevel = {
        #Strong;           // Strong consistency, all shards synchronized
        #ReadCommitted;    // Read committed, may read stale data
        #ReadUncommitted;  // Read uncommitted, may read dirty data
        #Eventual;         // Eventual consistency, may read very stale data
    };
    
    // ===== Transaction Types =====
    
    /// Represents a transaction
    public type Transaction = {
        id : TransactionId;
        databaseId : DatabaseId;
        status : TransactionStatus;
        operations : [TransactionOperation];
        involvedShards : [ShardId];
        startTime : Time.Time;
        lastUpdateTime : Time.Time;
        timeout : Nat64;
    };
    
    /// Represents a transaction status
    public type TransactionStatus = {
        #Active;
        #Preparing;
        #Prepared;
        #Committing;
        #Committed;
        #Aborting;
        #Aborted;
    };
    
    /// Represents a transaction operation
    public type TransactionOperation = {
        #Insert : {
            tableName : Text;
            records : [Record];
        };
        #Update : {
            tableName : Text;
            updates : [(Text, Value)];
            filter : ?FilterExpression;
        };
        #Delete : {
            tableName : Text;
            filter : ?FilterExpression;
        };
    };
    
    /// Represents a transaction result
    public type TransactionResult = {
        transactionId : TransactionId;
        status : TransactionStatus;
        affectedRows : Nat;
    };
    
    /// Represents a two-phase commit prepare result
    public type PrepareResult = {
        #Ready;
        #Failed : Text;
    };
    
    /// Represents a two-phase commit commit result
    public type CommitResult = {
        #Committed;
        #Failed : Text;
    };
    
    // ===== Query Trace Types =====
    
    /// Represents a trace of query execution
    public type QueryTrace = {
        queryId : Text;
        query : Query;
        executionPlan : ExecutionPlan;
        shardTraces : [ShardQueryTrace];
        startTime : Time.Time;
        endTime : Time.Time;
        status : {
            #Completed;
            #Failed : Text;
            #InProgress;
        };
    };
    
    /// Represents a trace of a shard query execution
    public type ShardQueryTrace = {
        shardId : ShardId;
        operations : [ShardOperation];
        startTime : Time.Time;
        endTime : ?Time.Time;
        status : {
            #Completed : {
                rowsProcessed : Nat;
                cyclesConsumed : Nat64;
            };
            #Failed : Text;
            #InProgress;
        };
    };
    
    // ===== Status Types =====
    
    /// Represents the status of the Query Coordinator
    public type CoordinatorStatus = {
        activeQueries : Nat;
        activeTransactions : Nat;
        queryPlanCacheSize : Nat;
        averageQueryTime : Nat64;
        queriesPerSecond : Float;
        errorRate : Float;
        uptime : Nat64;
        memoryUsage : Nat;
        cyclesBalance : Nat;
    };
}