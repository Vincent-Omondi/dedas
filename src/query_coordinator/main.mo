/**
 * Query Coordinator Canister
 * 
 * The Query Coordinator is responsible for parsing, planning, distributing, and
 * aggregating queries across shards in the DEDaS system.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Cycles "mo:base/ExperimentalCycles";
import Debug "mo:base/Debug";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Random "mo:base/Random";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Trie "mo:base/Trie";
import TrieMap "mo:base/TrieMap";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";

import CommonTypes "../common/types";
import Errors "../common/errors";
import CoordinatorTypes "./types";

actor QueryCoordinator {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type TransactionId = CommonTypes.TransactionId;
    type Query = CommonTypes.Query;
    type ResultSet = CommonTypes.ResultSet;
    type QueryResponse = CommonTypes.QueryResponse;
    type Record = CommonTypes.Record;
    type Value = CommonTypes.Value;
    
    type ExecutionPlan = CoordinatorTypes.ExecutionPlan;
    type ShardPlan = CoordinatorTypes.ShardPlan;
    type ShardOperation = CoordinatorTypes.ShardOperation;
    type ShardQuery = CoordinatorTypes.ShardQuery;
    type ShardResult = CoordinatorTypes.ShardResult;
    type PlanType = CoordinatorTypes.PlanType;
    type ConsistencyLevel = CoordinatorTypes.ConsistencyLevel;
    type QueryRequest = CoordinatorTypes.QueryRequest;
    type Transaction = CoordinatorTypes.Transaction;
    type TransactionStatus = CoordinatorTypes.TransactionStatus;
    type TransactionOperation = CoordinatorTypes.TransactionOperation;
    type TransactionResult = CoordinatorTypes.TransactionResult;
    type PrepareResult = CoordinatorTypes.PrepareResult;
    type CommitResult = CoordinatorTypes.CommitResult;
    type QueryTrace = CoordinatorTypes.QueryTrace;
    type ShardQueryTrace = CoordinatorTypes.ShardQueryTrace;
    type CoordinatorStatus = CoordinatorTypes.CoordinatorStatus;
    
    type QueryError = Errors.QueryError;
    type QueryResult<T> = Errors.QueryResult<T>;
    type TransactionError = Errors.TransactionError;
    type TransactionResult<T> = Errors.TransactionResult<T>;
    
    // External actor interfaces
    
    type Registry = actor {
        getDatabaseSchema : (dbId : DatabaseId) -> async Result.Result<CommonTypes.DatabaseSchema, Errors.DatabaseError>;
        getShardsForKey : (dbId : DatabaseId, tableName : Text, key : ?CommonTypes.Key) -> async [ShardId];
        getShardInfo : (shardId : ShardId) -> async Result.Result<{canisterId : Principal}, Errors.RegistryError>;
    };
    
    type ShardActor = actor {
        executeQuery : (query : ShardQuery) -> async Result.Result<ShardResult, Errors.QueryError>;
        prepare : (txId : TransactionId, operations : [TransactionOperation]) -> async PrepareResult;
        commit : (txId : TransactionId) -> async CommitResult;
        abort : (txId : TransactionId) -> async ();
    };
    
    // ===== State =====
    
    // Registry canister reference
    private let registryCanisterId : Principal = Principal.fromText("aaaaa-aa"); // Placeholder, will be initialized properly
    
    // Active queries
    private let activeQueries = HashMap.HashMap<Text, QueryTrace>(10, Text.equal, Text.hash);
    
    // Query trace history (limited size)
    private let queryTraceHistory = HashMap.HashMap<Text, QueryTrace>(50, Text.equal, Text.hash);
    
    // Active transactions
    private let activeTransactions = HashMap.HashMap<TransactionId, Transaction>(10, Text.equal, Text.hash);
    
    // Query plan cache (query hash -> execution plan)
    private let queryPlanCache = HashMap.HashMap<Text, ExecutionPlan>(50, Text.equal, Text.hash);
    
    // Statistics
    private var totalQueries : Nat = 0;
    private var successfulQueries : Nat = 0;
    private var failedQueries : Nat = 0;
    private var totalQueryTime : Nat64 = 0;
    private var startTime : Time.Time = Time.now();
    
    // ===== Initialization =====
    
    private func getRegistryActor() : Registry {
        actor(Principal.toText(registryCanisterId)) : Registry
    };
    
    private func getShardActor(shardId : ShardId) : async ShardActor {
        let registry = getRegistryActor();
        let shardInfoResult = await registry.getShardInfo(shardId);
        
        switch (shardInfoResult) {
            case (#ok(info)) {
                actor(Principal.toText(info.canisterId)) : ShardActor
            };
            case (#err(error)) {
                Debug.trap("Failed to get shard actor: " # debug_show(error));
            };
        };
    };
    
    // ===== Helper Functions =====
    
    /// Generate a unique query ID
    private func generateQueryId() : Text {
        let now = Int.abs(Time.now());
        let randomSuffix = Int.abs(Time.now()) % 10000;
        "qry-" # Nat.toText(now) # "-" # Nat.toText(randomSuffix)
    };
    
    /// Generate a unique transaction ID
    private func generateTransactionId() : TransactionId {
        let now = Int.abs(Time.now());
        let randomSuffix = Int.abs(Time.now()) % 10000;
        "txn-" # Nat.toText(now) # "-" # Nat.toText(randomSuffix)
    };
    
    /// Hash a query for caching
    private func hashQuery(query : Query) : Text {
        // This is a simplified implementation
        // In production, we'd implement a proper query canonicalization and hashing
        Text.hash(debug_show(query))
    };
    
    /// Parse a query string into a Query object
    private func parseQuery(queryStr : Text) : QueryResult<Query> {
        // This is a placeholder for query parsing logic
        // In a real implementation, we'd have a full SQL parser here
        
        if (Text.size(queryStr) == 0) {
            return #err(#ParseError("Empty query string"));
        };
        
        let queryLower = Text.toLower(queryStr);
        
        // Very basic parsing just to demonstrate structure
        if (Text.startsWith(queryLower, #text "select")) {
            // For now, let's just support basic SELECT queries
            let selectQuery : CommonTypes.SelectQuery = {
                tables = [{table = "test_table"; alias = null; join = null}];
                projection = [{expression = "*"; alias = null}];
                filter = null;
                groupBy = [];
                having = null;
                orderBy = [];
                limit = null;
                offset = null;
            };
            
            return #ok(#Select(selectQuery));
        } else if (Text.startsWith(queryLower, #text "insert")) {
            // Basic INSERT support
            let insertQuery : CommonTypes.InsertQuery = {
                table = "test_table";
                columns = ["id", "name"];
                values = [[#Text("1"), #Text("test")]];
                returning = [];
            };
            
            return #ok(#Insert(insertQuery));
        } else if (Text.startsWith(queryLower, #text "update")) {
            // Basic UPDATE support
            let updateQuery : CommonTypes.UpdateQuery = {
                table = "test_table";
                updates = [("name", #Text("new_value"))];
                filter = null;
                returning = [];
            };
            
            return #ok(#Update(updateQuery));
        } else if (Text.startsWith(queryLower, #text "delete")) {
            // Basic DELETE support
            let deleteQuery : CommonTypes.DeleteQuery = {
                table = "test_table";
                filter = null;
                returning = [];
            };
            
            return #ok(#Delete(deleteQuery));
        } else {
            return #err(#ParseError("Unsupported query type"));
        };
    };
    
    /// Create an execution plan for a query
    private func createExecutionPlan(dbId : DatabaseId, query : Query) : async QueryResult<ExecutionPlan> {
        try {
            // Get database schema
            let registry = getRegistryActor();
            let schemaResult = await registry.getDatabaseSchema(dbId);
            
            switch (schemaResult) {
                case (#err(error)) {
                    return #err(#ExecutionError("Failed to get database schema: " # debug_show(error)));
                };
                case (#ok(schema)) {
                    // This is a simplified planning logic
                    // In a real implementation, we'd have a full query optimizer
                    
                    let queryId = generateQueryId();
                    var planType : PlanType = #SingleShard;
                    var shardPlans : [ShardPlan] = [];
                    
                    switch (query) {
                        case (#Select(selectQuery)) {
                            // For simplicity, let's assume each table is on its own set of shards
                            // In a real implementation, we'd need to handle joins across shards
                            
                            if (selectQuery.tables.size() == 0) {
                                return #err(#ValidationError("No tables specified in SELECT query"));
                            };
                            
                            let tableName = selectQuery.tables[0].table;
                            
                            // Find shards that contain this table
                            let shardIds = await registry.getShardsForKey(dbId, tableName, null);
                            
                            if (shardIds.size() == 0) {
                                return #err(#ExecutionError("No shards found for table: " # tableName));
                            };
                            
                            if (shardIds.size() == 1) {
                                // Single shard plan
                                planType := #SingleShard;
                                
                                shardPlans := [{
                                    shardId = shardIds[0];
                                    operations = [
                                        #Scan({
                                            tableName = tableName;
                                            filter = selectQuery.filter;
                                        }),
                                        #Project({
                                            columns = ["*"]; // Simplified
                                        })
                                    ];
                                    expectedResultSize = 100; // Placeholder
                                    priority = 1;
                                }];
                            } else {
                                // Multi-shard plan
                                planType := #ScatterGather;
                                
                                shardPlans := Array.map<ShardId, ShardPlan>(shardIds, func(shardId : ShardId) : ShardPlan {
                                    {
                                        shardId = shardId;
                                        operations = [
                                            #Scan({
                                                tableName = tableName;
                                                filter = selectQuery.filter;
                                            }),
                                            #Project({
                                                columns = ["*"]; // Simplified
                                            })
                                        ];
                                        expectedResultSize = 100; // Placeholder
                                        priority = 1;
                                    }
                                });
                            };
                        };
                        case (#Insert(insertQuery)) {
                            // For inserts, we need to determine which shard(s) should receive the data
                            let tableName = insertQuery.table;
                            
                            // In a real implementation, we'd use the primary key to determine the shard
                            // For simplicity, let's just use the first shard
                            
                            let shardIds = await registry.getShardsForKey(dbId, tableName, null);
                            
                            if (shardIds.size() == 0) {
                                return #err(#ExecutionError("No shards found for table: " # tableName));
                            };
                            
                            planType := #SingleShard;
                            
                            // Create records from values
                            let records : [Record] = [];
                            // In a real implementation, we'd convert values to records here
                            
                            shardPlans := [{
                                shardId = shardIds[0];
                                operations = [
                                    #Insert({
                                        tableName = tableName;
                                        records = records;
                                    })
                                ];
                                expectedResultSize = 1; // Insert usually affects one row
                                priority = 1;
                            }];
                        };
                        case (#Update(updateQuery)) {
                            // For updates, we might need to update data on multiple shards
                            let tableName = updateQuery.table;
                            
                            let shardIds = await registry.getShardsForKey(dbId, tableName, null);
                            
                            if (shardIds.size() == 0) {
                                return #err(#ExecutionError("No shards found for table: " # tableName));
                            };
                            
                            if (shardIds.size() == 1) {
                                planType := #SingleShard;
                                
                                shardPlans := [{
                                    shardId = shardIds[0];
                                    operations = [
                                        #Update({
                                            tableName = tableName;
                                            updates = updateQuery.updates;
                                            filter = updateQuery.filter;
                                        })
                                    ];
                                    expectedResultSize = 1; // Placeholder
                                    priority = 1;
                                }];
                            } else {
                                planType := #Broadcast;
                                
                                shardPlans := Array.map<ShardId, ShardPlan>(shardIds, func(shardId : ShardId) : ShardPlan {
                                    {
                                        shardId = shardId;
                                        operations = [
                                            #Update({
                                                tableName = tableName;
                                                updates = updateQuery.updates;
                                                filter = updateQuery.filter;
                                            })
                                        ];
                                        expectedResultSize = 1; // Placeholder
                                        priority = 1;
                                    }
                                });
                            };
                        };
                        case (#Delete(deleteQuery)) {
                            // For deletes, we might need to delete data from multiple shards
                            let tableName = deleteQuery.table;
                            
                            let shardIds = await registry.getShardsForKey(dbId, tableName, null);
                            
                            if (shardIds.size() == 0) {
                                return #err(#ExecutionError("No shards found for table: " # tableName));
                            };
                            
                            if (shardIds.size() == 1) {
                                planType := #SingleShard;
                                
                                shardPlans := [{
                                    shardId = shardIds[0];
                                    operations = [
                                        #Delete({
                                            tableName = tableName;
                                            filter = deleteQuery.filter;
                                        })
                                    ];
                                    expectedResultSize = 1; // Placeholder
                                    priority = 1;
                                }];
                            } else {
                                planType := #Broadcast;
                                
                                shardPlans := Array.map<ShardId, ShardPlan>(shardIds, func(shardId : ShardId) : ShardPlan {
                                    {
                                        shardId = shardId;
                                        operations = [
                                            #Delete({
                                                tableName = tableName;
                                                filter = deleteQuery.filter;
                                            })
                                        ];
                                        expectedResultSize = 1; // Placeholder
                                        priority = 1;
                                    }
                                });
                            };
                        };
                    };
                    
                    let executionPlan : ExecutionPlan = {
                        queryId = queryId;
                        query = query;
                        planType = planType;
                        shardPlans = shardPlans;
                        aggregationType = if (planType == #ScatterGather) {?#Union} else {null};
                        costEstimate = {
                            estimatedRows = 100; // Placeholder
                            estimatedCycles = 1_000_000_000; // Placeholder
                            estimatedExecutionTime = 1_000_000_000; // 1 second in nanoseconds
                        };
                    };
                    
                    return #ok(executionPlan);
                };
            };
        } catch (error) {
            return #err(#ExecutionError("Error creating execution plan: " # Error.message(error)));
        };
    };
    
    /// Execute a query on a shard
    private func executeOnShard(shardId : ShardId, query : ShardQuery) : async QueryResult<ShardResult> {
        try {
            let shardActor = await getShardActor(shardId);
            
            // Add cycles for remote execution
            // In production, we'd have a more sophisticated cycle management strategy
            Cycles.add(1_000_000_000); // 1B cycles as an example
            
            let result = await shardActor.executeQuery(query);
            
            switch (result) {
                case (#ok(shardResult)) {
                    return #ok(shardResult);
                };
                case (#err(error)) {
                    return #err(error);
                };
            };
        } catch (error) {
            return #err(#ExecutionError("Error executing query on shard " # shardId # ": " # Error.message(error)));
        };
    };
    
    /// Merge results from multiple shards
    private func mergeResults(results : [ShardResult]) : QueryResult<ResultSet> {
        if (results.size() == 0) {
            return #err(#ExecutionError("No results to merge"));
        };
        
        if (results.size() == 1) {
            return #ok(results[0].resultSet);
        };
        
        // For simplicity, let's just concatenate the rows
        // In a real implementation, we'd need more sophisticated merging
        // depending on the query type and aggregation
        
        let firstResult = results[0].resultSet;
        let columns = firstResult.columns;
        
        let allRows = Buffer.Buffer<[Value]>(firstResult.rowCount);
        
        for (result in results.vals()) {
            // Ensure column schemas match
            if (result.resultSet.columns != columns) {
                return #err(#ExecutionError("Column mismatch during result merging"));
            };
            
            // Add rows
            for (row in result.resultSet.rows.vals()) {
                allRows.add(row);
            };
        };
        
        let mergedResult : ResultSet = {
            columns = columns;
            rows = Buffer.toArray(allRows);
            rowCount = Buffer.size(allRows);
        };
        
        return #ok(mergedResult);
    };
    
    // ===== Public API =====
    
    /// Execute a query on the database
    public shared(msg) func executeQuery(request : QueryRequest) : async QueryResult<CommonTypes.QueryResponse> {
        try {
            let startTimeNanos = Time.now();
            
            // Parse query string
            let parseResult = parseQuery(request.query);
            
            switch (parseResult) {
                case (#err(error)) {
                    return #err(error);
                };
                case (#ok(query)) {
                    // Check plan cache
                    let queryHash = hashQuery(query);
                    var executionPlan : ExecutionPlan = switch (queryPlanCache.get(queryHash)) {
                        case (?cachedPlan) { cachedPlan };
                        case (null) {
                            // Create execution plan
                            let planResult = await createExecutionPlan(request.databaseId, query);
                            
                            switch (planResult) {
                                case (#err(error)) { return #err(error); };
                                case (#ok(plan)) {
                                    // Cache the plan
                                    queryPlanCache.put(queryHash, plan);
                                    plan
                                };
                            };
                        };
                    };
                    
                    // Create trace
                    let trace : QueryTrace = {
                        queryId = executionPlan.queryId;
                        query = query;
                        executionPlan = executionPlan;
                        shardTraces = [];
                        startTime = startTimeNanos;
                        endTime = startTimeNanos;
                        status = #InProgress;
                    };
                    
                    activeQueries.put(executionPlan.queryId, trace);
                    
                    // Execute on shards
                    let shardResultsBuffer = Buffer.Buffer<ShardResult>(executionPlan.shardPlans.size());
                    let shardTracesBuffer = Buffer.Buffer<ShardQueryTrace>(executionPlan.shardPlans.size());
                    
                    for (shardPlan in executionPlan.shardPlans.vals()) {
                        let shardQuery : ShardQuery = {
                            queryId = executionPlan.queryId;
                            databaseId = request.databaseId;
                            operations = shardPlan.operations;
                            transactionId = null;
                        };
                        
                        let shardTrace : ShardQueryTrace = {
                            shardId = shardPlan.shardId;
                            operations = shardPlan.operations;
                            startTime = Time.now();
                            endTime = null;
                            status = #InProgress;
                        };
                        
                        shardTracesBuffer.add(shardTrace);
                        
                        let shardResultResult = await executeOnShard(shardPlan.shardId, shardQuery);
                        
                        let updatedShardTrace : ShardQueryTrace = {
                            shardId = shardPlan.shardId;
                            operations = shardPlan.operations;
                            startTime = shardTrace.startTime;
                            endTime = ?Time.now();
                            status = switch (shardResultResult) {
                                case (#ok(result)) {
                                    #Completed({
                                        rowsProcessed = result.resultSet.rowCount;
                                        cyclesConsumed = result.cyclesConsumed;
                                    })
                                };
                                case (#err(error)) {
                                    #Failed(Errors.queryErrorToText(error))
                                };
                            };
                        };
                        
                        shardTracesBuffer.put(shardTracesBuffer.size() - 1, updatedShardTrace);
                        
                        switch (shardResultResult) {
                            case (#ok(result)) {
                                shardResultsBuffer.add(result);
                            };
                            case (#err(error)) {
                                // Update trace with error
                                let updatedTrace : QueryTrace = {
                                    queryId = trace.queryId;
                                    query = trace.query;
                                    executionPlan = trace.executionPlan;
                                    shardTraces = Buffer.toArray(shardTracesBuffer);
                                    startTime = trace.startTime;
                                    endTime = Time.now();
                                    status = #Failed(Errors.queryErrorToText(error));
                                };
                                
                                activeQueries.put(executionPlan.queryId, updatedTrace);
                                queryTraceHistory.put(executionPlan.queryId, updatedTrace);
                                
                                // Update statistics
                                totalQueries += 1;
                                failedQueries += 1;
                                
                                return #err(#ExecutionError("Shard execution failed: " # Errors.queryErrorToText(error)));
                            };
                        };
                    };
                    
                    // Merge results
                    let mergeResult = mergeResults(Buffer.toArray(shardResultsBuffer));
                    
                    let endTimeNanos = Time.now();
                    let executionTimeNanos = Nat64.fromIntWrap(endTimeNanos - startTimeNanos);
                    
                    switch (mergeResult) {
                        case (#err(error)) {
                            // Update trace with error
                            let updatedTrace : QueryTrace = {
                                queryId = trace.queryId;
                                query = trace.query;
                                executionPlan = trace.executionPlan;
                                shardTraces = Buffer.toArray(shardTracesBuffer);
                                startTime = trace.startTime;
                                endTime = endTimeNanos;
                                status = #Failed(Errors.queryErrorToText(error));
                            };
                            
                            activeQueries.put(executionPlan.queryId, updatedTrace);
                            queryTraceHistory.put(executionPlan.queryId, updatedTrace);
                            
                            // Update statistics
                            totalQueries += 1;
                            failedQueries += 1;
                            
                            return #err(error);
                        };
                        case (#ok(resultSet)) {
                            // Update trace with success
                            let updatedTrace : QueryTrace = {
                                queryId = trace.queryId;
                                query = trace.query;
                                executionPlan = trace.executionPlan;
                                shardTraces = Buffer.toArray(shardTracesBuffer);
                                startTime = trace.startTime;
                                endTime = endTimeNanos;
                                status = #Completed;
                            };
                            
                            activeQueries.delete(executionPlan.queryId);
                            queryTraceHistory.put(executionPlan.queryId, updatedTrace);
                            
                            // Update statistics
                            totalQueries += 1;
                            successfulQueries += 1;
                            totalQueryTime += executionTimeNanos;
                            
                            // Calculate total cycles consumed
                            var totalCyclesConsumed : Nat64 = 0;
                            for (shardResult in shardResultsBuffer.vals()) {
                                totalCyclesConsumed += shardResult.cyclesConsumed;
                            };
                            
                            let response : CommonTypes.QueryResponse = {
                                resultSet = resultSet;
                                executionTime = executionTimeNanos;
                            };
                            
                            return #ok(response);
                        };
                    };
                };
            };
        } catch (error) {
            return #err(#ExecutionError("Error executing query: " # Error.message(error)));
        };
    };
    
    /// Begin a new transaction
    public shared(msg) func beginTransaction(dbId : DatabaseId) : async TransactionResult<TransactionId> {
        let txId = generateTransactionId();
        
        let transaction : Transaction = {
            id = txId;
            databaseId = dbId;
            status = #Active;
            operations = [];
            involvedShards = [];
            startTime = Time.now();
            lastUpdateTime = Time.now();
            timeout = 60_000_000_000; // 60 seconds in nanoseconds
        };
        
        activeTransactions.put(txId, transaction);
        
        return #ok(txId);
    };
    
    /// Add operation to transaction
    public shared(msg) func addTransactionOperation(txId : TransactionId, operation : TransactionOperation) : async TransactionResult<()> {
        switch (activeTransactions.get(txId)) {
            case (?transaction) {
                if (transaction.status != #Active) {
                    return #err(#InvalidTransactionState("Transaction is not active"));
                };
                
                // Add operation to transaction
                let updatedOperations = Array.append<TransactionOperation>(transaction.operations, [operation]);
                
                // Update transaction
                let updatedTransaction : Transaction = {
                    id = transaction.id;
                    databaseId = transaction.databaseId;
                    status = transaction.status;
                    operations = updatedOperations;
                    involvedShards = transaction.involvedShards;
                    startTime = transaction.startTime;
                    lastUpdateTime = Time.now();
                    timeout = transaction.timeout;
                };
                
                activeTransactions.put(txId, updatedTransaction);
                
                return #ok();
            };
            case (null) {
                return #err(#TransactionNotFound("Transaction not found: " # txId));
            };
        };
    };
    
    /// Commit a transaction
    public shared(msg) func commitTransaction(txId : TransactionId) : async TransactionResult<TransactionResult> {
        switch (activeTransactions.get(txId)) {
            case (?transaction) {
                if (transaction.status != #Active) {
                    return #err(#InvalidTransactionState("Transaction is not active"));
                };
                
                if (transaction.operations.size() == 0) {
                    // Empty transaction, just remove it
                    activeTransactions.delete(txId);
                    
                    return #ok({
                        transactionId = txId;
                        status = #Committed;
                        affectedRows = 0;
                    });
                };
                
                // Determine involved shards
                let registry = getRegistryActor();
                var involvedShardsBuffer = Buffer.Buffer<ShardId>(4);
                
                // For each operation, identify involved shards
                // This is a simplified implementation
                for (operation in transaction.operations.vals()) {
                    let tableName = switch (operation) {
                        case (#Insert(op)) { op.tableName };
                        case (#Update(op)) { op.tableName };
                        case (#Delete(op)) { op.tableName };
                    };
                    
                    let shardIds = await registry.getShardsForKey(transaction.databaseId, tableName, null);
                    
                    for (shardId in shardIds.vals()) {
                        if (not Array.contains<ShardId>(Buffer.toArray(involvedShardsBuffer), shardId, Text.equal)) {
                            involvedShardsBuffer.add(shardId);
                        };
                    };
                };
                
                let involvedShards = Buffer.toArray(involvedShardsBuffer);
                
                if (involvedShards.size() == 0) {
                    return #err(#PrepareError("No shards involved in transaction"));
                };
                
                // Update transaction status
                let txWithShards : Transaction = {
                    id = transaction.id;
                    databaseId = transaction.databaseId;
                    status = #Preparing;
                    operations = transaction.operations;
                    involvedShards = involvedShards;
                    startTime = transaction.startTime;
                    lastUpdateTime = Time.now();
                    timeout = transaction.timeout;
                };
                
                activeTransactions.put(txId, txWithShards);
                
                // Execute two-phase commit
                
                // Phase 1: Prepare
                var prepareResults = Buffer.Buffer<(ShardId, PrepareResult)>(involvedShards.size());
                
                for (shardId in involvedShards.vals()) {
                    let shardActor = await getShardActor(shardId);
                    let prepareResult = await shardActor.prepare(txId, transaction.operations);
                    prepareResults.add((shardId, prepareResult));
                };
                
                // Check if all shards are ready
                let allReady = Array.all<(ShardId, PrepareResult)>(Buffer.toArray(prepareResults), func(item) {
                    let (_, result) = item;
                    result == #Ready
                });
                
                if (allReady) {
                    // Update transaction status
                    let txPrepared : Transaction = {
                        id = txWithShards.id;
                        databaseId = txWithShards.databaseId;
                        status = #Prepared;
                        operations = txWithShards.operations;
                        involvedShards = txWithShards.involvedShards;
                        startTime = txWithShards.startTime;
                        lastUpdateTime = Time.now();
                        timeout = txWithShards.timeout;
                    };
                    
                    activeTransactions.put(txId, txPrepared);
                    
                    // Phase 2: Commit
                    var commitSuccess = true;
                    var commitError = "";
                    
                    for (shardId in involvedShards.vals()) {
                        let shardActor = await getShardActor(shardId);
                        let commitResult = await shardActor.commit(txId);
                        
                        switch (commitResult) {
                            case (#Committed) {
                                // Success
                            };
                            case (#Failed(error)) {
                                commitSuccess := false;
                                commitError := error;
                            };
                        };
                    };
                    
                    // Update transaction status
                    let finalStatus : TransactionStatus = if (commitSuccess) {
                        #Committed
                    } else {
                        #Aborted
                    };
                    
                    let txFinal : Transaction = {
                        id = txPrepared.id;
                        databaseId = txPrepared.databaseId;
                        status = finalStatus;
                        operations = txPrepared.operations;
                        involvedShards = txPrepared.involvedShards;
                        startTime = txPrepared.startTime;
                        lastUpdateTime = Time.now();
                        timeout = txPrepared.timeout;
                    };
                    
                    activeTransactions.put(txId, txFinal);
                    
                    if (commitSuccess) {
                        // Success - could remove transaction from active list here
                        // or keep it for a while for reference
                        
                        return #ok({
                            transactionId = txId;
                            status = #Committed;
                            affectedRows = 0; // In a real implementation, we'd count affected rows
                        });
                    } else {
                        return #err(#CommitError("Commit failed: " # commitError));
                    };
                } else {
                    // Some shards failed to prepare
                    let failedShards = Buffer.Buffer<(ShardId, Text)>(4);
                    
                    for (item in prepareResults.vals()) {
                        let (shardId, result) = item;
                        switch (result) {
                            case (#Ready) {
                                // Success
                            };
                            case (#Failed(error)) {
                                failedShards.add((shardId, error));
                            };
                        };
                    };
                    
                    // Abort the transaction on all shards
                    for (shardId in involvedShards.vals()) {
                        let shardActor = await getShardActor(shardId);
                        ignore shardActor.abort(txId);
                    };
                    
                    // Update transaction status
                    let txAborted : Transaction = {
                        id = txWithShards.id;
                        databaseId = txWithShards.databaseId;
                        status = #Aborted;
                        operations = txWithShards.operations;
                        involvedShards = txWithShards.involvedShards;
                        startTime = txWithShards.startTime;
                        lastUpdateTime = Time.now();
                        timeout = txWithShards.timeout;
                    };
                    
                    activeTransactions.put(txId, txAborted);
                    
                    let errorMsg = "Transaction prepare failed on shard(s): " # 
                                  debug_show(Buffer.toArray(failedShards));
                    
                    return #err(#PrepareError(errorMsg));
                };
            };
            case (null) {
                return #err(#TransactionNotFound("Transaction not found: " # txId));
            };
        };
    };
    
    /// Rollback a transaction
    public shared(msg) func rollbackTransaction(txId : TransactionId) : async TransactionResult<()> {
        switch (activeTransactions.get(txId)) {
            case (?transaction) {
                if (transaction.status != #Active) {
                    return #err(#InvalidTransactionState("Cannot rollback - transaction is not active"));
                };
                
                // For transactions that have touched shards, abort on all involved shards
                if (transaction.involvedShards.size() > 0) {
                    for (shardId in transaction.involvedShards.vals()) {
                        let shardActor = await getShardActor(shardId);
                        ignore shardActor.abort(txId);
                    };
                };
                
                // Update transaction status
                let txAborted : Transaction = {
                    id = transaction.id;
                    databaseId = transaction.databaseId;
                    status = #Aborted;
                    operations = transaction.operations;
                    involvedShards = transaction.involvedShards;
                    startTime = transaction.startTime;
                    lastUpdateTime = Time.now();
                    timeout = transaction.timeout;
                };
                
                activeTransactions.put(txId, txAborted);
                
                return #ok();
            };
            case (null) {
                return #err(#TransactionNotFound("Transaction not found: " # txId));
            };
        };
    };
    
    /// Get query trace
    public query func getQueryTrace(queryId : Text) : async ?QueryTrace {
        Option.or(activeQueries.get(queryId), queryTraceHistory.get(queryId))
    };
    
    /// Get coordinator status
    public query func getStatus() : async CoordinatorStatus {
        let now = Time.now();
        let uptimeNanos = now - startTime;
        let averageQueryTimeNanos = if (successfulQueries > 0) {
            totalQueryTime / Nat64.fromNat(successfulQueries)
        } else {
            0
        };
        
        let qps = if (uptimeNanos > 0) {
            Float.fromInt(totalQueries) / Float.fromInt(uptimeNanos) * 1_000_000_000
        } else {
            0
        };
        
        let errorRate = if (totalQueries > 0) {
            Float.fromInt(failedQueries) / Float.fromInt(totalQueries)
        } else {
            0
        };
        
        {
            activeQueries = activeQueries.size();
            activeTransactions = activeTransactions.size();
            queryPlanCacheSize = queryPlanCache.size();
            averageQueryTime = averageQueryTimeNanos;
            queriesPerSecond = qps;
            errorRate = errorRate;
            uptime = Nat64.fromIntWrap(uptimeNanos);
            memoryUsage = 0; // In a real implementation, we'd track memory usage
            cyclesBalance = Cycles.balance();
        }
    };
}