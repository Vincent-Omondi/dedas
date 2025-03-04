/**
 * Registry utilities for DEDaS
 * 
 * This module contains utility functions specific to the Registry canister.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Hash "mo:base/Hash";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";
import RegistryTypes "./types";

module {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type DatabaseSchema = CommonTypes.DatabaseSchema;
    type TableSchema = CommonTypes.TableSchema;
    type ColumnDefinition = CommonTypes.ColumnDefinition;
    type IndexDefinition = CommonTypes.IndexDefinition;
    type Key = CommonTypes.Key;
    type Value = CommonTypes.Value;
    type ShardingStrategy = CommonTypes.ShardingStrategy;
    
    type DatabaseMetadata = RegistryTypes.DatabaseMetadata;
    type ShardMetadata = RegistryTypes.ShardMetadata;
    type RoutingRule = RegistryTypes.RoutingRule;
    type RoutingType = RegistryTypes.RoutingType;
    type ShardInfo = RegistryTypes.ShardInfo;
    type KeyRange = RegistryTypes.KeyRange;
    
    // ===== Schema Validation =====
    
    /// Validate a database schema
    public func validateDatabaseSchema(schema : DatabaseSchema) : Result.Result<(), Text> {
        // Check if database name is valid
        if (not Utils.isValidDatabaseName(schema.name)) {
            return #err("Invalid database name: " # schema.name);
        };
        
        // Check if schema has tables
        if (schema.tables.size() == 0) {
            return #err("Database schema must contain at least one table");
        };
        
        // Validate each table schema
        for (table in schema.tables.vals()) {
            let tableResult = validateTableSchema(table);
            
            switch (tableResult) {
                case (#err(e)) {
                    return #err("Error in table '" # table.name # "': " # e);
                };
                case (#ok()) {
                    // Continue to next table
                };
            };
        };
        
        // Validate table relationships and references
        let tableResult = validateTableRelationships(schema.tables);
        
        switch (tableResult) {
            case (#err(e)) {
                return #err("Error in table relationships: " # e);
            };
            case (#ok()) {
                // All validations passed
            };
        };
        
        #ok()
    };
    
    /// Validate a table schema
    public func validateTableSchema(table : TableSchema) : Result.Result<(), Text> {
        // Check if table name is valid
        if (not Utils.isValidTableName(table.name)) {
            return #err("Invalid table name: " # table.name);
        };
        
        // Check if table has columns
        if (table.columns.size() == 0) {
            return #err("Table must contain at least one column");
        };
        
        // Check for duplicate column names
        let columnNames = Buffer.Buffer<Text>(table.columns.size());
        for (column in table.columns.vals()) {
            if (Buffer.contains<Text>(columnNames, column.name, Text.equal)) {
                return #err("Duplicate column name: " # column.name);
            };
            columnNames.add(column.name);
        };
        
        // Validate each column
        for (column in table.columns.vals()) {
            let columnResult = validateColumnDefinition(column);
            
            switch (columnResult) {
                case (#err(e)) {
                    return #err("Error in column '" # column.name # "': " # e);
                };
                case (#ok()) {
                    // Continue to next column
                };
            };
        };
        
        // Validate primary key
        if (table.primaryKey.size() > 0) {
            for (pkCol in table.primaryKey.vals()) {
                let columnExists = Array.find<ColumnDefinition>(
                    table.columns,
                    func(col) { col.name == pkCol }
                );
                
                switch (columnExists) {
                    case (null) {
                        return #err("Primary key column not found: " # pkCol);
                    };
                    case (?col) {
                        if (col.nullable) {
                            return #err("Primary key column cannot be nullable: " # pkCol);
                        };
                    };
                };
            };
        };
        
        // Validate indexes
        for (index in table.indexes.vals()) {
            let indexResult = validateIndexDefinition(index, table.columns);
            
            switch (indexResult) {
                case (#err(e)) {
                    return #err("Error in index '" # index.name # "': " # e);
                };
                case (#ok()) {
                    // Continue to next index
                };
            };
        };
        
        // Validate foreign keys
        for (fk in table.foreignKeys.vals()) {
            let fkResult = validateForeignKeyDefinition(fk);
            
            switch (fkResult) {
                case (#err(e)) {
                    return #err("Error in foreign key to '" # fk.foreignTable # "': " # e);
                };
                case (#ok()) {
                    // Continue to next foreign key
                };
            };
        };
        
        #ok()
    };
    
    /// Validate a column definition
    public func validateColumnDefinition(column : ColumnDefinition) : Result.Result<(), Text> {
        // Check if column name is valid
        if (not Utils.isValidColumnName(column.name)) {
            return #err("Invalid column name: " # column.name);
        };
        
        // Check for valid default value
        switch (column.defaultValue) {
            case (null) {
                // No default value is always valid
            };
            case (?value) {
                // TODO: Validate that default value matches column type
            };
        };
        
        // Check for conflicting constraints
        let hasNotNull = Array.some<CommonTypes.Constraint>(
            column.constraints,
            func(c) { c == #NotNull }
        );
        
        if (hasNotNull and column.nullable) {
            return #err("Column has contradictory nullable and NOT NULL settings");
        };
        
        #ok()
    };
    
    /// Validate an index definition
    public func validateIndexDefinition(index : IndexDefinition, columns : [ColumnDefinition]) : Result.Result<(), Text> {
        // Check if index name is valid
        if (not Utils.isValidName(index.name)) {
            return #err("Invalid index name: " # index.name);
        };
        
        // Check if index has columns
        if (index.columns.size() == 0) {
            return #err("Index must include at least one column");
        };
        
        // Check if all index columns exist in the table
        for (colName in index.columns.vals()) {
            let colExists = Array.some<ColumnDefinition>(
                columns,
                func(col) { col.name == colName }
            );
            
            if (not colExists) {
                return #err("Column not found: " # colName);
            };
        };
        
        #ok()
    };
    
    /// Validate a foreign key definition
    public func validateForeignKeyDefinition(fk : CommonTypes.ForeignKeyDefinition) : Result.Result<(), Text> {
        // Check if foreign table name is valid
        if (not Utils.isValidTableName(fk.foreignTable)) {
            return #err("Invalid foreign table name: " # fk.foreignTable);
        };
        
        // Check if foreign key has columns
        if (fk.foreignColumns.size() == 0) {
            return #err("Foreign key must include at least one column");
        };
        
        // Note: Full validation would require access to the foreign table schema,
        // which may not be available at this point. More comprehensive checks
        // would be performed at the database level.
        
        #ok()
    };
    
    /// Validate table relationships
    public func validateTableRelationships(tables : [TableSchema]) : Result.Result<(), Text> {
        // Check foreign key references
        for (table in tables.vals()) {
            for (fk in table.foreignKeys.vals()) {
                // Check if referenced table exists
                let foreignTable = Array.find<TableSchema>(
                    tables,
                    func(t) { t.name == fk.foreignTable }
                );
                
                switch (foreignTable) {
                    case (null) {
                        return #err("Foreign table not found: " # fk.foreignTable);
                    };
                    case (?ft) {
                        // Check if referenced columns exist and form a key in the foreign table
                        for (fcol in fk.foreignColumns.vals()) {
                            let colExists = Array.some<ColumnDefinition>(
                                ft.columns,
                                func(col) { col.name == fcol }
                            );
                            
                            if (not colExists) {
                                return #err("Foreign column not found: " # fcol);
                            };
                        };
                        
                        // Simplified check - in a real implementation we'd verify that
                        // the foreign columns form a primary key or unique constraint
                    };
                };
            };
        };
        
        #ok()
    };
    
    // ===== Sharding Functions =====
    
    /// Calculate the number of shards needed for a database
    public func calculateShardCount(
        schema : DatabaseSchema,
        estimatedRowCount : Nat,
        estimatedRowSize : Nat,
        targetShardSize : Nat
    ) : Nat {
        let totalSize = estimatedRowCount * estimatedRowSize;
        
        if (totalSize == 0) {
            return 1; // At least one shard
        };
        
        let shardCount = totalSize / targetShardSize + (if (totalSize % targetShardSize > 0) 1 else 0);
        Nat.max(1, shardCount) // At least one shard
    };
    
    /// Calculate key ranges for range-based sharding
    public func calculateKeyRanges(
        keyType : CommonTypes.DataType,
        shardCount : Nat
    ) : [(Value, Value)] {
        // This is a simplified implementation for demonstration
        // In a real system, we'd use more sophisticated range calculation
        
        switch (keyType) {
            case (#Int) {
                // Create integer ranges
                let ranges = Buffer.Buffer<(Value, Value)>(shardCount);
                
                // Simple approach: divide the integer space into equal parts
                // In reality, we'd analyze data distribution
                let minValue = -1_000_000_000; // Arbitrary range for example
                let maxValue = 1_000_000_000;
                let range = maxValue - minValue;
                let step = range / shardCount;
                
                var start = minValue;
                
                for (i in Iter.range(0, shardCount - 1)) {
                    let end = if (i == shardCount - 1) {
                        maxValue + 1
                    } else {
                        start + step
                    };
                    
                    ranges.add((#Int(start), #Int(end)));
                    start := end;
                };
                
                Buffer.toArray(ranges)
            };
            case (#Text) {
                // Create text ranges
                let ranges = Buffer.Buffer<(Value, Value)>(shardCount);
                
                // Simple approach: divide alphabetically
                let alphabet = ["", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "Z{"]; // "{" comes after "Z" in ASCII
                
                // Ensure we don't try to create more shards than we have boundaries
                let actualShardCount = Nat.min(shardCount, alphabet.size() - 1);
                
                // Calculate step size
                let step = alphabet.size() / actualShardCount;
                
                for (i in Iter.range(0, actualShardCount - 1)) {
                    let startIndex = i * step;
                    let endIndex = if (i == actualShardCount - 1) {
                        alphabet.size() - 1
                    } else {
                        (i + 1) * step
                    };
                    
                    ranges.add((#Text(alphabet[startIndex]), #Text(alphabet[endIndex])));
                };
                
                Buffer.toArray(ranges)
            };
            case _ {
                // Default to a single range for unsupported types
                [(#Null, #Null)]
            };
        };
    };
    
    /// Hash a value for hash-based sharding
    public func hashForSharding(value : Value, shardCount : Nat) : Nat {
        // Use the common hash function for values
        let hashValue = Utils.hashValue(value);
        
        // Convert to shard index
        Nat32.toNat(hashValue) % shardCount
    };
    
    /// Determine shard for a key using hash-based sharding
    public func getShardForKeyHash(
        key : Value,
        shardIds : [ShardId]
    ) : ?ShardId {
        if (shardIds.size() == 0) {
            return null;
        };
        
        let shardIndex = hashForSharding(key, shardIds.size());
        ?shardIds[shardIndex]
    };
    
    /// Determine shard for a key using range-based sharding
    public func getShardForKeyRange(
        key : Value,
        ranges : [(Value, Value, ShardId)]
    ) : ?ShardId {
        for ((startKey, endKey, shardId) in ranges.vals()) {
            let startCompare = Utils.compareValues(key, startKey);
            let endCompare = Utils.compareValues(key, endKey);
            
            if ((startCompare == #EQ or startCompare == #GT) and
                (endCompare == #LT)) {
                return ?shardId;
            };
        };
        
        null
    };
    
    /// Determine shard for a key using directory-based sharding
    public func getShardForKeyDirectory(
        key : Value,
        mappings : [(Key, ShardId)]
    ) : ?ShardId {
        for ((mapKey, shardId) in mappings.vals()) {
            if (Utils.compareValues(key, mapKey) == #EQ) {
                return ?shardId;
            };
        };
        
        null
    };
    
    // ===== Routing Functions =====
    
    /// Create a hash-based routing rule
    public func createHashBasedRoutingRule(
        databaseId : DatabaseId,
        tableName : Text,
        column : Text,
        shardIds : [ShardId],
        priority : Nat
    ) : RoutingRule {
        let routingType : RoutingType = #HashBased({
            column = column;
        });
        
        {
            databaseId = databaseId;
            tableName = tableName;
            routingType = routingType;
            shardIds = shardIds;
            priority = priority;
            created = Time.now();
            lastModified = Time.now();
        }
    };
    
    /// Create a range-based routing rule
    public func createRangeBasedRoutingRule(
        databaseId : DatabaseId,
        tableName : Text,
        column : Text,
        ranges : [(Value, Value, ShardId)],
        priority : Nat
    ) : RoutingRule {
        let routingType : RoutingType = #RangeBased({
            column = column;
            keyRanges = ranges;
        });
        
        {
            databaseId = databaseId;
            tableName = tableName;
            routingType = routingType;
            shardIds = Array.map<(Value, Value, ShardId), ShardId>(
                ranges,
                func((_, _, shardId)) { shardId }
            );
            priority = priority;
            created = Time.now();
            lastModified = Time.now();
        }
    };
    
    /// Create a directory-based routing rule
    public func createDirectoryBasedRoutingRule(
        databaseId : DatabaseId,
        tableName : Text,
        mappings : [(Key, ShardId)],
        priority : Nat
    ) : RoutingRule {
        let routingType : RoutingType = #DirectoryBased({
            mappings = mappings;
        });
        
        let shardIds = Buffer.Buffer<ShardId>(0);
        
        // Collect unique shard IDs
        for ((_, shardId) in mappings.vals()) {
            if (not Buffer.contains<ShardId>(shardIds, shardId, Text.equal)) {
                shardIds.add(shardId);
            };
        };
        
        {
            databaseId = databaseId;
            tableName = tableName;
            routingType = routingType;
            shardIds = Buffer.toArray(shardIds);
            priority = priority;
            created = Time.now();
            lastModified = Time.now();
        }
    };
    
    /// Create a broadcast routing rule
    public func createBroadcastRoutingRule(
        databaseId : DatabaseId,
        tableName : Text,
        shardIds : [ShardId],
        priority : Nat
    ) : RoutingRule {
        let routingType : RoutingType = #Broadcast;
        
        {
            databaseId = databaseId;
            tableName = tableName;
            routingType = routingType;
            shardIds = shardIds;
            priority = priority;
            created = Time.now();
            lastModified = Time.now();
        }
    };
    
    /// Apply sharding strategy to create initial routing rules
    public func createInitialRoutingRules(
        databaseId : DatabaseId,
        schema : DatabaseSchema,
        shardIds : [ShardId],
        strategy : ShardingStrategy
    ) : [RoutingRule] {
        let rules = Buffer.Buffer<RoutingRule>(schema.tables.size());
        
        if (shardIds.size() == 0) {
            return [];
        };
        
        for (table in schema.tables.vals()) {
            let tableName = table.name;
            
            switch (strategy) {
                case (#HashBased({ column })) {
                    // Verify column exists
                    let columnExists = Array.some<ColumnDefinition>(
                        table.columns,
                        func(col) { col.name == column }
                    );
                    
                    let hashColumn = if (columnExists) {
                        column
                    } else if (table.primaryKey.size() > 0) {
                        // Default to first primary key column
                        table.primaryKey[0]
                    } else {
                        // No suitable column found, use first column
                        table.columns[0].name
                    };
                    
                    rules.add(createHashBasedRoutingRule(
                        databaseId,
                        tableName,
                        hashColumn,
                        shardIds,
                        10 // Default priority
                    ));
                };
                case (#RangeBased({ column, ranges })) {
                    // Verify column exists
                    let columnExists = Array.some<ColumnDefinition>(
                        table.columns,
                        func(col) { col.name == column }
                    );
                    
                    if (columnExists) {
                        // Find column type
                        let columnDef = Array.find<ColumnDefinition>(
                            table.columns,
                            func(col) { col.name == column }
                        );
                        
                        switch (columnDef) {
                            case (?col) {
                                // Create ranges based on column type
                                let keyRanges = if (ranges.size() > 0) {
                                    ranges
                                } else {
                                    calculateKeyRanges(col.dataType, shardIds.size())
                                };
                                
                                // Convert to (Value, Value, ShardId) format
                                let rangeTriples = Buffer.Buffer<(Value, Value, ShardId)>(keyRanges.size());
                                
                                for (i in Iter.range(0, keyRanges.size() - 1)) {
                                    let (startKey, endKey) = keyRanges[i];
                                    let shardId = shardIds[i % shardIds.size()];
                                    rangeTriples.add((startKey, endKey, shardId));
                                };
                                
                                rules.add(createRangeBasedRoutingRule(
                                    databaseId,
                                    tableName,
                                    column,
                                    Buffer.toArray(rangeTriples),
                                    10 // Default priority
                                ));
                            };
                            case (null) {
                                // Fallback to hash-based on primary key
                                if (table.primaryKey.size() > 0) {
                                    rules.add(createHashBasedRoutingRule(
                                        databaseId,
                                        tableName,
                                        table.primaryKey[0],
                                        shardIds,
                                        10 // Default priority
                                    ));
                                } else {
                                    // Last resort: broadcast to all shards
                                    rules.add(createBroadcastRoutingRule(
                                        databaseId,
                                        tableName,
                                        shardIds,
                                        10 // Default priority
                                    ));
                                };
                            };
                        };
                    } else {
                        // Column doesn't exist, fallback to broadcast
                        rules.add(createBroadcastRoutingRule(
                            databaseId,
                            tableName,
                            shardIds,
                            10 // Default priority
                        ));
                    };
                };
                case (#DirectoryBased) {
                    // Directory-based requires explicit mappings
                    // For initial setup, we'll default to broadcast
                    rules.add(createBroadcastRoutingRule(
                        databaseId,
                        tableName,
                        shardIds,
                        10 // Default priority
                    ));
                };
                case (#GeoBased({ column })) {
                    // Geo-based not fully implemented,
                    // For initial setup, default to hash-based
                    rules.add(createHashBasedRoutingRule(
                        databaseId,
                        tableName,
                        column,
                        shardIds,
                        10 // Default priority
                    ));
                };
            };
        };
        
        Buffer.toArray(rules)
    };
    
    // ===== Metadata Management =====
    
    /// Create initial metadata for a new database
    public func createDatabaseMetadata(
        dbId : DatabaseId,
        name : Text,
        owner : Principal,
        schema : DatabaseSchema,
        shardingStrategy : ShardingStrategy
    ) : DatabaseMetadata {
        let now = Time.now();
        
        {
            id = dbId;
            name = name;
            owner = owner;
            schema = schema;
            created = now;
            lastModified = now;
            shardingStrategy = shardingStrategy;
            shards = [];
            status = #Initializing;
            version = 1;
        }
    };
    
    /// Create initial metadata for a new shard
    public func createShardMetadata(
        shardId : ShardId,
        canisterId : Principal,
        databaseId : DatabaseId,
        tables : [Text],
        keyRange : ?KeyRange
    ) : ShardMetadata {
        let now = Time.now();
        
        {
            id = shardId;
            canisterId = canisterId;
            databaseId = databaseId;
            tables = tables;
            keyRange = keyRange;
            created = now;
            lastModified = now;
            status = #Provisioning;
            metrics = {
                dataSize = 0;
                recordCount = 0;
                avgQueryLatency = 0;
                queryCount = 0;
                writeCount = 0;
                errorCount = 0;
                cpuCycles = 0;
                memoryUsage = 0;
                lastUpdated = now;
            };
            replicaShards = [];
        }
    };
    
    /// Check if a schema change is compatible
    public func isSchemaChangeCompatible(
        oldSchema : DatabaseSchema,
        newSchema : DatabaseSchema
    ) : Result.Result<(), Text> {
        // Check for table removals (not allowed in basic implementation)
        for (oldTable in oldSchema.tables.vals()) {
            let tableExists = Array.some<TableSchema>(
                newSchema.tables,
                func(t) { t.name == oldTable.name }
            );
            
            if (not tableExists) {
                return #err("Removing tables is not supported: " # oldTable.name);
            };
        };
        
        // Check for compatible table changes
        for (newTable in newSchema.tables.vals()) {
            let oldTable = Array.find<TableSchema>(
                oldSchema.tables,
                func(t) { t.name == newTable.name }
            );
            
            switch (oldTable) {
                case (null) {
                    // New table, always allowed
                };
                case (?table) {
                    // Check column modifications
                    let columnResult = areColumnsCompatible(table.columns, newTable.columns);
                    
                    switch (columnResult) {
                        case (#err(e)) {
                            return #err("Incompatible change in table '" # newTable.name # "': " # e);
                        };
                        case (#ok()) {
                            // Continue to next check
                        };
                    };
                    
                    // Check primary key changes
                    if (not Array.equal<Text>(table.primaryKey, newTable.primaryKey, Text.equal)) {
                        return #err("Changing primary key is not supported");
                    };
                    
                    // Other checks would be implemented here
                };
            };
        };
        
        #ok()
    };
    
    /// Check if column changes are compatible
    public func areColumnsCompatible(
        oldColumns : [ColumnDefinition],
        newColumns : [ColumnDefinition]
    ) : Result.Result<(), Text> {
        // Check for column removals (not allowed in basic implementation)
        for (oldCol in oldColumns.vals()) {
            let colExists = Array.some<ColumnDefinition>(
                newColumns,
                func(c) { c.name == oldCol.name }
            );
            
            if (not colExists) {
                return #err("Removing columns is not supported: " # oldCol.name);
            };
        };
        
        // Check for column type changes
        for (newCol in newColumns.vals()) {
            let oldCol = Array.find<ColumnDefinition>(
                oldColumns,
                func(c) { c.name == newCol.name }
            );
            
            switch (oldCol) {
                case (null) {
                    // New column, check if it's nullable or has default
                    if (not newCol.nullable and Option.isNull(newCol.defaultValue)) {
                        return #err("New column '" # newCol.name # "' must be nullable or have a default value");
                    };
                };
                case (?col) {
                    // Check if type change is compatible
                    if (col.dataType != newCol.dataType) {
                        return #err("Changing column type is not supported: " # newCol.name);
                    };
                    
                    // Check if nullability change is compatible
                    if (col.nullable and not newCol.nullable) {
                        return #err("Cannot change column from nullable to non-nullable: " # newCol.name);
                    };
                };
            };
        };
        
        #ok()
    };
    
    // ===== Rebalancing Functions =====
    
    /// Determine if rebalancing is needed
    public func isRebalancingNeeded(shards : [ShardMetadata], threshold : Nat) : Bool {
        if (shards.size() <= 1) {
            return false;
        };
        
        // Calculate average load
        var totalSize : Nat = 0;
        for (shard in shards.vals()) {
            totalSize += shard.metrics.dataSize;
        };
        
        let avgSize = totalSize / shards.size();
        if (avgSize == 0) {
            return false;
        };
        
        // Check if any shard is above threshold
        for (shard in shards.vals()) {
            let loadPercent = (shard.metrics.dataSize * 100) / avgSize;
            if (loadPercent > threshold) {
                return true;
            };
        };
        
        false
    };
    
    /// Determine which shards should be split
    public func identifyShardsToSplit(
        shards : [ShardMetadata],
        maxSizeBytes : Nat,
        maxRecords : Nat
    ) : [ShardId] {
        let candidates = Buffer.Buffer<ShardId>(0);
        
        for (shard in shards.vals()) {
            if (shard.metrics.dataSize > maxSizeBytes or
                shard.metrics.recordCount > maxRecords) {
                candidates.add(shard.id);
            };
        };
        
        Buffer.toArray(candidates)
    };
    
    /// Create a rebalancing plan
    public func createRebalancePlan(
        databaseId : DatabaseId,
        shardsToSplit : [ShardId],
        existingShards : [ShardMetadata],
        routingRules : [RoutingRule]
    ) : {
        shardsToCreate : Nat;
        splits : [(ShardId, ?KeyRange)];
        updatedRules : [RoutingRule];
    } {
        // In a real implementation, this would be much more sophisticated
        // For now, we'll create a simple plan that splits each shard into two
        
        let splits = Buffer.Buffer<(ShardId, ?KeyRange)>(shardsToSplit.size());
        
        for (shardId in shardsToSplit.vals()) {
            splits.add((shardId, null)); // Simple split without specific key range
        };
        
        {
            shardsToCreate = shardsToSplit.size(); // One new shard per split
            splits = Buffer.toArray(splits);
            updatedRules = []; // Rules would be updated after splits
        }
    };
}