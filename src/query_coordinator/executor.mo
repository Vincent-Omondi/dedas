/**
 * Query Executor module for DEDaS
 * 
 * This module is responsible for query execution planning, distribution,
 * and result aggregation across multiple shards.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";
import Error "mo:base/Error";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";
import CoordinatorTypes "./types";
import Parser "./parser";

module {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type TransactionId = CommonTypes.TransactionId;
    type Query = CommonTypes.Query;
    type ResultSet = CommonTypes.ResultSet;
    type Record = CommonTypes.Record;
    type Value = CommonTypes.Value;
    type TableSchema = CommonTypes.TableSchema;
    type ColumnDefinition = CommonTypes.ColumnDefinition;
    type FilterExpression = CommonTypes.FilterExpression;

    type ExecutionPlan = CoordinatorTypes.ExecutionPlan;
    type ShardPlan = CoordinatorTypes.ShardPlan;
    type ShardOperation = CoordinatorTypes.ShardOperation;
    type ShardQuery = CoordinatorTypes.ShardQuery;
    type ShardResult = CoordinatorTypes.ShardResult;
    type PlanType = CoordinatorTypes.PlanType;
    type AggregationType = CoordinatorTypes.AggregationType;
    type CostEstimate = CoordinatorTypes.CostEstimate;
    
    type QueryError = Errors.QueryError;
    type QueryResult<T> = Errors.QueryResult<T>;
    
    // External actor interfaces
    
    type Registry = actor {
        getDatabaseSchema : (dbId : DatabaseId) -> async Result.Result<CommonTypes.DatabaseSchema, Errors.DatabaseError>;
        getShardsForKey : (dbId : DatabaseId, tableName : Text, key : ?CommonTypes.Key) -> async [ShardId];
        getShardInfo : (shardId : ShardId) -> async Result.Result<{canisterId : Principal}, Errors.RegistryError>;
    };
    
    type ShardActor = actor {
        executeQuery : (query : ShardQuery) -> async Result.Result<ShardResult, Errors.QueryError>;
    };
    
    // ===== Query Execution Planning =====
    
    /// Create an execution plan for a SQL query
    public func createExecutionPlan(
        getRegistryActor : () -> Registry,
        dbId : DatabaseId,
        queryText : Text,
        cyclesPerShard : Nat64
    ) : async QueryResult<ExecutionPlan> {
        try {
            // Parse the query
            let parseResult = Parser.parseQuery(queryText);
            
            switch (parseResult) {
                case (#err(parseError)) {
                    return #err(parseError);
                };
                case (#ok(query)) {
                    // Get database schema
                    let registry = getRegistryActor();
                    let schemaResult = await registry.getDatabaseSchema(dbId);
                    
                    switch (schemaResult) {
                        case (#err(error)) {
                            return #err(#ExecutionError("Failed to get database schema: " # debug_show(error)));
                        };
                        case (#ok(schema)) {
                            // Generate query ID
                            let queryId = Utils.generateQueryId();
                            
                            // Analyze query based on type
                            switch (query) {
                                case (#Select(selectQuery)) {
                                    return await createSelectPlan(getRegistryActor, dbId, queryId, selectQuery, schema, cyclesPerShard);
                                };
                                case (#Insert(insertQuery)) {
                                    return await createInsertPlan(getRegistryActor, dbId, queryId, insertQuery, schema, cyclesPerShard);
                                };
                                case (#Update(updateQuery)) {
                                    return await createUpdatePlan(getRegistryActor, dbId, queryId, updateQuery, schema, cyclesPerShard);
                                };
                                case (#Delete(deleteQuery)) {
                                    return await createDeletePlan(getRegistryActor, dbId, queryId, deleteQuery, schema, cyclesPerShard);
                                };
                            };
                        };
                    };
                };
            };
        } catch (e) {
            #err(#ExecutionError("Error creating execution plan: " # Error.message(e)))
        };
    };
    
    /// Create execution plan for SELECT query
    private func createSelectPlan(
        getRegistryActor : () -> Registry,
        dbId : DatabaseId,
        queryId : Text,
        selectQuery : CommonTypes.SelectQuery,
        schema : CommonTypes.DatabaseSchema,
        cyclesPerShard : Nat64
    ) : async QueryResult<ExecutionPlan> {
        if (selectQuery.tables.size() == 0) {
            return #err(#ValidationError("No tables specified in SELECT query"));
        };
        
        // For now, we only handle queries against a single table
        let tableRef = selectQuery.tables[0];
        let tableName = tableRef.table;
        
        // Find the table schema
        let tableSchema = Array.find<TableSchema>(
            schema.tables,
            func(t) { t.name == tableName }
        );
        
        switch (tableSchema) {
            case (null) {
                return #err(#ValidationError("Table not found: " # tableName));
            };
            case (?table) {
                // Determine which shards contain this table
                let registry = getRegistryActor();
                let shardIds = await registry.getShardsForKey(dbId, tableName, null);
                
                if (shardIds.size() == 0) {
                    return #err(#ExecutionError("No shards found for table: " # tableName));
                };
                
                // Determine if we need a single shard or distributed plan
                let planType : PlanType = if (shardIds.size() == 1) {
                    #SingleShard
                } else {
                    // If the query has aggregates or GROUP BY, we need ScatterGather
                    if (selectQuery.groupBy.size() > 0) {
                        #ScatterGather
                    } else {
                        // Check if we are selecting by primary key
                        switch (selectQuery.filter) {
                            case (null) {
                                #Broadcast
                            };
                            case (?filter) {
                                if (isPrimaryKeyFilter(filter, table)) {
                                    #SingleShard
                                } else {
                                    #Broadcast
                                }
                            };
                        };
                    }
                };
                
                // Create shard plans based on plan type
                var shardPlans : [ShardPlan] = [];
                
                switch (planType) {
                    case (#SingleShard) {
                        // Find the specific shard for the filter if possible
                        var targetShardId = shardIds[0];
                        
                        switch (selectQuery.filter) {
                            case (?filter) {
                                if (isPrimaryKeyFilter(filter, table)) {
                                    let keyValue = extractKeyValue(filter, table);
                                    
                                    switch (keyValue) {
                                        case (?key) {
                                            let shardForKey = await registry.getShardsForKey(dbId, tableName, ?key);
                                            if (shardForKey.size() > 0) {
                                                targetShardId := shardForKey[0];
                                            };
                                        };
                                        case (null) {
                                            // No key value, use default shard
                                        };
                                    };
                                };
                            };
                            case (null) {
                                // No filter, use default shard
                            };
                        };
                        
                        // Create a plan for the single shard
                        shardPlans := [{
                            shardId = targetShardId;
                            operations = [
                                #Scan({
                                    tableName = tableName;
                                    filter = selectQuery.filter;
                                }),
                                #Project({
                                    columns = Array.map<CommonTypes.ProjectionElement, Text>(
                                        selectQuery.projection,
                                        func(p) { p.expression }
                                    );
                                })
                            ];
                            expectedResultSize = 100; // Placeholder estimate
                            priority = 1;
                        }];
                    };
                    case (#Broadcast) {
                        // Create identical plans for all shards
                        shardPlans := Array.map<ShardId, ShardPlan>(
                            shardIds,
                            func(shardId) {
                                {
                                    shardId = shardId;
                                    operations = [
                                        #Scan({
                                            tableName = tableName;
                                            filter = selectQuery.filter;
                                        }),
                                        #Project({
                                            columns = Array.map<CommonTypes.ProjectionElement, Text>(
                                                selectQuery.projection,
                                                func(p) { p.expression }
                                            );
                                        })
                                    ];
                                    expectedResultSize = 100; // Placeholder estimate
                                    priority = 1;
                                }
                            }
                        );
                    };
                    case (#ScatterGather) {
                        // More complex plan for aggregation and grouping
                        shardPlans := Array.map<ShardId, ShardPlan>(
                            shardIds,
                            func(shardId) {
                                let operations = Buffer.Buffer<ShardOperation>(3);
                                
                                // Add scan operation
                                operations.add(#Scan({
                                    tableName = tableName;
                                    filter = selectQuery.filter;
                                }));
                                
                                // Add projection
                                operations.add(#Project({
                                    columns = Array.map<CommonTypes.ProjectionElement, Text>(
                                        selectQuery.projection,
                                        func(p) { p.expression }
                                    );
                                }));
                                
                                // Add grouping if needed
                                if (selectQuery.groupBy.size() > 0) {
                                    operations.add(#Aggregate({
                                        operations = []; // Would include COUNT, SUM, etc.
                                        groupBy = selectQuery.groupBy;
                                    }));
                                };
                                
                                {
                                    shardId = shardId;
                                    operations = Buffer.toArray(operations);
                                    expectedResultSize = 100; // Placeholder estimate
                                    priority = 1;
                                }
                            }
                        );
                    };
                    case (#Pipeline) {
                        // Not implemented yet
                        return #err(#UnsupportedOperation("Pipeline execution not implemented"));
                    };
                };
                
                // Determine aggregation type based on plan type
                var aggregationType : ?AggregationType = null;
                
                if (planType == #ScatterGather) {
                    if (selectQuery.groupBy.size() > 0) {
                        aggregationType := ?#GroupAggregate;
                    } else {
                        aggregationType := ?#Union;
                    };
                } else if (planType == #Broadcast) {
                    aggregationType := ?#Union;
                };
                
                // Create cost estimate
                let costEstimate : CostEstimate = {
                    estimatedRows = 100 * shardPlans.size(); // Very rough estimate
                    estimatedCycles = cyclesPerShard * Nat64.fromNat(shardPlans.size());
                    estimatedExecutionTime = 1_000_000_000; // 1 second in nanoseconds
                };
                
                // Create final execution plan
                let executionPlan : ExecutionPlan = {
                    queryId = queryId;
                    query = #Select(selectQuery);
                    planType = planType;
                    shardPlans = shardPlans;
                    aggregationType = aggregationType;
                    costEstimate = costEstimate;
                };
                
                #ok(executionPlan)
            };
        };
    };
    
    /// Create execution plan for INSERT query
    private func createInsertPlan(
        getRegistryActor : () -> Registry,
        dbId : DatabaseId,
        queryId : Text,
        insertQuery : CommonTypes.InsertQuery,
        schema : CommonTypes.DatabaseSchema,
        cyclesPerShard : Nat64
    ) : async QueryResult<ExecutionPlan> {
        let tableName = insertQuery.table;
        
        // Find the table schema
        let tableSchema = Array.find<TableSchema>(
            schema.tables,
            func(t) { t.name == tableName }
        );
        
        switch (tableSchema) {
            case (null) {
                return #err(#ValidationError("Table not found: " # tableName));
            };
            case (?table) {
                // Validate column names
                for (colName in insertQuery.columns.vals()) {
                    let colExists = Array.some<ColumnDefinition>(
                        table.columns,
                        func(col) { col.name == colName }
                    );
                    
                    if (not colExists) {
                        return #err(#ValidationError("Column not found: " # colName));
                    };
                };
                
                // Determine target shards based on primary key
                let registry = getRegistryActor();
                
                // Create records from column names and values
                let records = Buffer.Buffer<Record>(insertQuery.values.size());
                
                for (valueRow in insertQuery.values.vals()) {
                    if (valueRow.size() != insertQuery.columns.size()) {
                        return #err(#ValidationError("Values count doesn't match columns count"));
                    };
                    
                    let record = Buffer.Buffer<(Text, Value)>(insertQuery.columns.size());
                    
                    for (i in Iter.range(0, insertQuery.columns.size() - 1)) {
                        record.add((insertQuery.columns[i], valueRow[i]));
                    };
                    
                    records.add(Buffer.toArray(record));
                };
                
                // Determine shards for each record
                let recordsPerShard = HashMap.HashMap<ShardId, Buffer.Buffer<Record>>(10, Text.equal, Text.hash);
                
                for (record in records.vals()) {
                    // Find primary key value if present
                    var pkValue : ?Value = null;
                    
                    if (table.primaryKey.size() > 0) {
                        let pkColName = table.primaryKey[0];
                        for ((colName, value) in record.vals()) {
                            if (colName == pkColName) {
                                pkValue := ?value;
                                break;
                            };
                        };
                    };
                    
                    // Get shard for this record
                    let shardIds = await registry.getShardsForKey(dbId, tableName, pkValue);
                    
                    if (shardIds.size() == 0) {
                        return #err(#ExecutionError("No shards found for table: " # tableName));
                    };
                    
                    let targetShardId = shardIds[0];
                    
                    // Add record to shard's buffer
                    switch (recordsPerShard.get(targetShardId)) {
                        case (null) {
                            let buffer = Buffer.Buffer<Record>(1);
                            buffer.add(record);
                            recordsPerShard.put(targetShardId, buffer);
                        };
                        case (?buffer) {
                            buffer.add(record);
                        };
                    };
                };
                
                // Create shard plans
                let shardPlans = Buffer.Buffer<ShardPlan>(recordsPerShard.size());
                
                for ((shardId, recordsBuffer) in recordsPerShard.entries()) {
                    let records = Buffer.toArray(recordsBuffer);
                    
                    shardPlans.add({
                        shardId = shardId;
                        operations = [
                            #Insert({
                                tableName = tableName;
                                records = records;
                            })
                        ];
                        expectedResultSize = 1;
                        priority = 1;
                    });
                };
                
                // Create cost estimate
                let costEstimate : CostEstimate = {
                    estimatedRows = insertQuery.values.size();
                    estimatedCycles = cyclesPerShard * Nat64.fromNat(shardPlans.size());
                    estimatedExecutionTime = 1_000_000_000; // 1 second in nanoseconds
                };
                
                // Create final execution plan
                let executionPlan : ExecutionPlan = {
                    queryId = queryId;
                    query = #Insert(insertQuery);
                    planType = #SingleShard; // Each shard handles its own set of records
                    shardPlans = Buffer.toArray(shardPlans);
                    aggregationType = ?#Union; // Simple union of affected row counts
                    costEstimate = costEstimate;
                };
                
                #ok(executionPlan)
            };
        };
    };
    
    /// Create execution plan for UPDATE query
    private func createUpdatePlan(
        getRegistryActor : () -> Registry,
        dbId : DatabaseId,
        queryId : Text,
        updateQuery : CommonTypes.UpdateQuery,
        schema : CommonTypes.DatabaseSchema,
        cyclesPerShard : Nat64
    ) : async QueryResult<ExecutionPlan> {
        let tableName = updateQuery.table;
        
        // Find the table schema
        let tableSchema = Array.find<TableSchema>(
            schema.tables,
            func(t) { t.name == tableName }
        );
        
        switch (tableSchema) {
            case (null) {
                return #err(#ValidationError("Table not found: " # tableName));
            };
            case (?table) {
                // Validate column names in updates
                for ((colName, _) in updateQuery.updates.vals()) {
                    let colExists = Array.some<ColumnDefinition>(
                        table.columns,
                        func(col) { col.name == colName }
                    );
                    
                    if (not colExists) {
                        return #err(#ValidationError("Column not found: " # colName));
                    };
                };
                
                // Determine if we need single or multi-shard plan
                let registry = getRegistryActor();
                var targetShardIds : [ShardId] = [];
                
                switch (updateQuery.filter) {
                    case (null) {
                        // No filter means update all rows, so all shards
                        targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                    };
                    case (?filter) {
                        if (isPrimaryKeyFilter(filter, table)) {
                            // If filtering by PK, we can target a specific shard
                            let keyValue = extractKeyValue(filter, table);
                            
                            switch (keyValue) {
                                case (?key) {
                                    targetShardIds := await registry.getShardsForKey(dbId, tableName, ?key);
                                };
                                case (null) {
                                    targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                                };
                            };
                        } else {
                            // For non-PK filters, we need to query all shards
                            targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                        };
                    };
                };
                
                if (targetShardIds.size() == 0) {
                    return #err(#ExecutionError("No shards found for table: " # tableName));
                };
                
                // Create shard plans
                let shardPlans = Array.map<ShardId, ShardPlan>(
                    targetShardIds,
                    func(shardId) {
                        {
                            shardId = shardId;
                            operations = [
                                #Update({
                                    tableName = tableName;
                                    updates = updateQuery.updates;
                                    filter = updateQuery.filter;
                                })
                            ];
                            expectedResultSize = 1;
                            priority = 1;
                        }
                    }
                );
                
                // Determine plan type
                let planType : PlanType = if (shardPlans.size() == 1) {
                    #SingleShard
                } else {
                    #Broadcast
                };
                
                // Create cost estimate
                let costEstimate : CostEstimate = {
                    estimatedRows = 10 * shardPlans.size(); // Very rough estimate
                    estimatedCycles = cyclesPerShard * Nat64.fromNat(shardPlans.size());
                    estimatedExecutionTime = 1_000_000_000; // 1 second in nanoseconds
                };
                
                // Create final execution plan
                let executionPlan : ExecutionPlan = {
                    queryId = queryId;
                    query = #Update(updateQuery);
                    planType = planType;
                    shardPlans = shardPlans;
                    aggregationType = ?#Union; // Simple union of affected row counts
                    costEstimate = costEstimate;
                };
                
                #ok(executionPlan)
            };
        };
    };
    
    /// Create execution plan for DELETE query
    private func createDeletePlan(
        getRegistryActor : () -> Registry,
        dbId : DatabaseId,
        queryId : Text,
        deleteQuery : CommonTypes.DeleteQuery,
        schema : CommonTypes.DatabaseSchema,
        cyclesPerShard : Nat64
    ) : async QueryResult<ExecutionPlan> {
        let tableName = deleteQuery.table;
        
        // Find the table schema
        let tableSchema = Array.find<TableSchema>(
            schema.tables,
            func(t) { t.name == tableName }
        );
        
        switch (tableSchema) {
            case (null) {
                return #err(#ValidationError("Table not found: " # tableName));
            };
            case (?table) {
                // Determine if we need single or multi-shard plan
                let registry = getRegistryActor();
                var targetShardIds : [ShardId] = [];
                
                switch (deleteQuery.filter) {
                    case (null) {
                        // No filter means delete all rows, so all shards
                        targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                    };
                    case (?filter) {
                        if (isPrimaryKeyFilter(filter, table)) {
                            // If filtering by PK, we can target a specific shard
                            let keyValue = extractKeyValue(filter, table);
                            
                            switch (keyValue) {
                                case (?key) {
                                    targetShardIds := await registry.getShardsForKey(dbId, tableName, ?key);
                                };
                                case (null) {
                                    targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                                };
                            };
                        } else {
                            // For non-PK filters, we need to query all shards
                            targetShardIds := await registry.getShardsForKey(dbId, tableName, null);
                        };
                    };
                };
                
                if (targetShardIds.size() == 0) {
                    return #err(#ExecutionError("No shards found for table: " # tableName));
                };
                
                // Create shard plans
                let shardPlans = Array.map<ShardId, ShardPlan>(
                    targetShardIds,
                    func(shardId) {
                        {
                            shardId = shardId;
                            operations = [
                                #Delete({
                                    tableName = tableName;
                                    filter = deleteQuery.filter;
                                })
                            ];
                            expectedResultSize = 1;
                            priority = 1;
                        }
                    }
                );
                
                // Determine plan type
                let planType : PlanType = if (shardPlans.size() == 1) {
                    #SingleShard
                } else {
                    #Broadcast
                };
                
                // Create cost estimate
                let costEstimate : CostEstimate = {
                    estimatedRows = 10 * shardPlans.size(); // Very rough estimate
                    estimatedCycles = cyclesPerShard * Nat64.fromNat(shardPlans.size());
                    estimatedExecutionTime = 1_000_000_000; // 1 second in nanoseconds
                };
                
                // Create final execution plan
                let executionPlan : ExecutionPlan = {
                    queryId = queryId;
                    query = #Delete(deleteQuery);
                    planType = planType;
                    shardPlans = shardPlans;
                    aggregationType = ?#Union; // Simple union of affected row counts
                    costEstimate = costEstimate;
                };
                
                #ok(executionPlan)
            };
        };
    };
    
    // ===== Query Execution Functions =====
    
    /// Execute query plan across shards
    public func executeQueryPlan(
        getShardActor : (ShardId) -> async ShardActor,
        plan : ExecutionPlan,
        dbId : DatabaseId,
        transactionId : ?TransactionId
    ) : async QueryResult<ShardResult> {
        try {
            // Check if we have any shards to query
            if (plan.shardPlans.size() == 0) {
                return #err(#ExecutionError("No shards in execution plan"));
            };
            
            // Single shard or broadcast execution
            if (plan.planType == #SingleShard or plan.planType == #Broadcast) {
                // Execute queries on each shard
                let shardResultsBuffer = Buffer.Buffer<Result.Result<ShardResult, QueryError>>(plan.shardPlans.size());
                
                for (shardPlan in plan.shardPlans.vals()) {
                    let shardQuery : ShardQuery = {
                        queryId = plan.queryId;
                        databaseId = dbId;
                        operations = shardPlan.operations;
                        transactionId = transactionId;
                    };
                    
                    let shardActor = await getShardActor(shardPlan.shardId);
                    let shardResult = await shardActor.executeQuery(shardQuery);
                    shardResultsBuffer.add(shardResult);
                };
                
                // Convert results
                let validResults = Buffer.Buffer<ShardResult>(shardResultsBuffer.size());
                let errors = Buffer.Buffer<Text>(0);
                
                for (result in shardResultsBuffer.vals()) {
                    switch (result) {
                        case (#ok(shardResult)) {
                            validResults.add(shardResult);
                            
                            // Collect warnings
                            for (warning in shardResult.warnings.vals()) {
                                errors.add(warning);
                            };
                        };
                        case (#err(error)) {
                            errors.add(Errors.queryErrorToText(error));
                        };
                    };
                };
                
                if (validResults.size() == 0) {
                    // All shards failed
                    return #err(#ExecutionError("All shards failed: " # Text.join("; ", errors.vals())));
                };
                
                // Merge results based on query type
                let mergedResult = mergeShardResults(validResults.toArray(), plan.aggregationType);
                
                switch (mergedResult) {
                    case (#err(error)) {
                        #err(error)
                    };
                    case (#ok(result)) {
                        #ok({
                            queryId = plan.queryId;
                            resultSet = result;
                            executionTime = Array.foldLeft<ShardResult, Nat64>(
                                validResults.toArray(),
                                0,
                                func(acc, res) { acc + res.executionTime }
                            );
                            cyclesConsumed = Array.foldLeft<ShardResult, Nat64>(
                                validResults.toArray(),
                                0,
                                func(acc, res) { acc + res.cyclesConsumed }
                            );
                            warnings = Buffer.toArray(errors);
                        })
                    };
                };
            } else if (plan.planType == #ScatterGather) {
                // Scatter-gather execution involves:
                // 1. Send queries to all shards (scatter)
                // 2. Collect and aggregate results (gather)
                
                // Execute scatter phase
                let shardResultsBuffer = Buffer.Buffer<Result.Result<ShardResult, QueryError>>(plan.shardPlans.size());
                
                for (shardPlan in plan.shardPlans.vals()) {
                    let shardQuery : ShardQuery = {
                        queryId = plan.queryId;
                        databaseId = dbId;
                        operations = shardPlan.operations;
                        transactionId = transactionId;
                    };
                    
                    let shardActor = await getShardActor(shardPlan.shardId);
                    let shardResult = await shardActor.executeQuery(shardQuery);
                    shardResultsBuffer.add(shardResult);
                };
                
                // Convert results
                let validResults = Buffer.Buffer<ShardResult>(shardResultsBuffer.size());
                let errors = Buffer.Buffer<Text>(0);
                
                for (result in shardResultsBuffer.vals()) {
                    switch (result) {
                        case (#ok(shardResult)) {
                            validResults.add(shardResult);
                            
                            // Collect warnings
                            for (warning in shardResult.warnings.vals()) {
                                errors.add(warning);
                            };
                        };
                        case (#err(error)) {
                            errors.add(Errors.queryErrorToText(error));
                        };
                    };
                };
                
                if (validResults.size() == 0) {
                    // All shards failed
                    return #err(#ExecutionError("All shards failed: " # Text.join("; ", errors.vals())));
                };
                
                // Merge results with appropriate aggregation
                let mergedResult = mergeShardResults(validResults.toArray(), plan.aggregationType);
                
                switch (mergedResult) {
                    case (#err(error)) {
                        #err(error)
                    };
                    case (#ok(result)) {
                        #ok({
                            queryId = plan.queryId;
                            resultSet = result;
                            executionTime = Array.foldLeft<ShardResult, Nat64>(
                                validResults.toArray(),
                                0,
                                func(acc, res) { acc + res.executionTime }
                            );
                            cyclesConsumed = Array.foldLeft<ShardResult, Nat64>(
                                validResults.toArray(),
                                0,
                                func(acc, res) { acc + res.cyclesConsumed }
                            );
                            warnings = Buffer.toArray(errors);
                        })
                    };
                };
            } else {
                // Pipeline execution not implemented yet
                #err(#UnsupportedOperation("Pipeline execution not implemented"))
            };
        } catch (e) {
            #err(#ExecutionError("Error executing query plan: " # Error.message(e)))
        };
    };
    
    // ===== Helper Functions =====
    
    /// Check if a filter is based on the primary key
    private func isPrimaryKeyFilter(filter : FilterExpression, table : TableSchema) : Bool {
        if (table.primaryKey.size() == 0) {
            return false;
        };
        
        let pkColumn = table.primaryKey[0];
        
        switch (filter) {
            case (#Equals(column, _)) { column == pkColumn };
            case (#And(conditions)) {
                // Check if any condition is an equality on the PK
                Array.some<FilterExpression>(conditions, func(cond) {
                    isPrimaryKeyFilter(cond, table)
                })
            };
            case _ { false };
        };
    };
    
    /// Extract key value from a primary key filter
    private func extractKeyValue(filter : FilterExpression, table : TableSchema) : ?Value {
        if (table.primaryKey.size() == 0) {
            return null;
        };
        
        let pkColumn = table.primaryKey[0];
        
        switch (filter) {
            case (#Equals(column, value)) {
                if (column == pkColumn) {
                    ?value
                } else {
                    null
                }
            };
            case (#And(conditions)) {
                // Try to find a condition with PK equality
                for (cond in conditions.vals()) {
                    let keyValue = extractKeyValue(cond, table);
                    if (keyValue != null) {
                        return keyValue;
                    };
                };
                null
            };
            case _ { null };
        };
    };
    
    /// Merge results from multiple shards
    private func mergeShardResults(
        results : [ShardResult],
        aggregationType : ?AggregationType
    ) : QueryResult<ResultSet> {
        if (results.size() == 0) {
            return #err(#ExecutionError("No results to merge"));
        };
        
        if (results.size() == 1) {
            return #ok(results[0].resultSet);
        };
        
        // Choose merge strategy based on aggregation type
        switch (aggregationType) {
            case (?#Union) {
                // Simple union of rows
                let firstResult = results[0].resultSet;
                let columns = firstResult.columns;
                
                let allRows = Buffer.Buffer<[Value]>(0);
                
                for (result in results.vals()) {
                    // Ensure column schemas match
                    if (not Array.equal<Text>(result.resultSet.columns, columns, Text.equal)) {
                        return #err(#ExecutionError("Column mismatch during result merging"));
                    };
                    
                    // Add rows
                    for (row in result.resultSet.rows.vals()) {
                        allRows.add(row);
                    };
                };
                
                #ok({
                    columns = columns;
                    rows = Buffer.toArray(allRows);
                    rowCount = Buffer.size(allRows);
                })
            };
            case (?#Merge) {
                // Merge sorted results (for ORDER BY)
                // This is a simplified implementation that assumes results are already sorted
                let firstResult = results[0].resultSet;
                let columns = firstResult.columns;
                
                let allRows = Buffer.Buffer<[Value]>(0);
                
                for (result in results.vals()) {
                    // Ensure column schemas match
                    if (not Array.equal<Text>(result.resultSet.columns, columns, Text.equal)) {
                        return #err(#ExecutionError("Column mismatch during result merging"));
                    };
                    
                    // Add rows
                    for (row in result.resultSet.rows.vals()) {
                        allRows.add(row);
                    };
                };
                
                // In a real implementation, we'd sort the combined results here
                
                #ok({
                    columns = columns;
                    rows = Buffer.toArray(allRows);
                    rowCount = Buffer.size(allRows);
                })
            };
            case (?#GroupAggregate) {
                // This requires a more sophisticated implementation for GROUP BY aggregation
                // For now, we'll just union the results, assuming the shards did partial aggregation
                
                let firstResult = results[0].resultSet;
                let columns = firstResult.columns;
                
                let allRows = Buffer.Buffer<[Value]>(0);
                
                for (result in results.vals()) {
                    // Ensure column schemas match
                    if (not Array.equal<Text>(result.resultSet.columns, columns, Text.equal)) {
                        return #err(#ExecutionError("Column mismatch during result merging"));
                    };
                    
                    // Add rows
                    for (row in result.resultSet.rows.vals()) {
                        allRows.add(row);
                    };
                };
                
                #ok({
                    columns = columns;
                    rows = Buffer.toArray(allRows);
                    rowCount = Buffer.size(allRows);
                })
            };
            case (?#Join) {
                // Joining results from different shards
                // This is complex and not fully implemented here
                #err(#UnsupportedOperation("Cross-shard joins not fully implemented"))
            };
            case (null) {
                // Default to simple union
                let firstResult = results[0].resultSet;
                let columns = firstResult.columns;
                
                let allRows = Buffer.Buffer<[Value]>(0);
                
                for (result in results.vals()) {
                    // Add rows
                    for (row in result.resultSet.rows.vals()) {
                        allRows.add(row);
                    };
                };
                
                #ok({
                    columns = columns;
                    rows = Buffer.toArray(allRows);
                    rowCount = Buffer.size(allRows);
                })
            };
        };
    };
}