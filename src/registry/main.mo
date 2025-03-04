/**
 * Registry Canister
 * 
 * The Registry Canister is responsible for maintaining metadata about schemas,
 * shards, and routing information in the DEDaS system. It serves as a central
 * coordinator for the distributed database.
 */

import Array "mo:base/Array";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";
import Nat32 "mo:base/Nat32";
import Uuid "mo:base/UUID";

import CommonTypes "../common/types";
import Errors "../common/errors";
import RegistryTypes "./types";

actor Registry {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type DatabaseSchema = CommonTypes.DatabaseSchema;
    type TableSchema = CommonTypes.TableSchema;
    type IndexDefinition = CommonTypes.IndexDefinition;
    type Key = CommonTypes.Key;
    type ShardingStrategy = CommonTypes.ShardingStrategy;
    type RebalanceStrategy = CommonTypes.RebalanceStrategy;
    
    type DatabaseMetadata = RegistryTypes.DatabaseMetadata;
    type ShardMetadata = RegistryTypes.ShardMetadata;
    type RoutingRule = RegistryTypes.RoutingRule;
    type DatabaseRegistrationRequest = RegistryTypes.DatabaseRegistrationRequest;
    type ShardRegistrationRequest = RegistryTypes.ShardRegistrationRequest;
    type RoutingRequest = RegistryTypes.RoutingRequest;
    type ShardInfo = RegistryTypes.ShardInfo;
    type DatabaseInfo = RegistryTypes.DatabaseInfo;
    type TableInfo = RegistryTypes.TableInfo;
    type RoutingInfo = RegistryTypes.RoutingInfo;
    type RebalanceRequest = RegistryTypes.RebalanceRequest;
    type RebalanceResponse = RegistryTypes.RebalanceResponse;
    type KeyRange = RegistryTypes.KeyRange;
    type RoutingType = RegistryTypes.RoutingType;
    type ShardStatus = RegistryTypes.ShardStatus;
    type ShardMetrics = RegistryTypes.ShardMetrics;
    
    type RegistryError = Errors.RegistryError;
    type RegistryResult<T> = Errors.RegistryResult<T>;
    type DatabaseError = Errors.DatabaseError;
    type DatabaseResult<T> = Errors.DatabaseResult<T>;
    type TableError = Errors.TableError;
    type TableResult<T> = Errors.TableResult<T>;
    type AdminError = Errors.AdminError;
    type AdminResult<T> = Errors.AdminResult<T>;
    
    // ===== State =====
    
    // Database registry: DatabaseId -> DatabaseMetadata
    private let databaseRegistry = HashMap.HashMap<DatabaseId, DatabaseMetadata>(10, Text.equal, Text.hash);
    
    // Shard registry: ShardId -> ShardMetadata
    private let shardRegistry = HashMap.HashMap<ShardId, ShardMetadata>(10, Text.equal, Text.hash);
    
    // Routing rules: (DatabaseId, TableName) -> [RoutingRule]
    private let routingRules = HashMap.HashMap<(DatabaseId, Text), [RoutingRule]>(10, 
        func(k1: (DatabaseId, Text), k2: (DatabaseId, Text)) : Bool {
            k1.0 == k2.0 and k1.1 == k2.1
        },
        func(k: (DatabaseId, Text)) : Nat32 {
            Text.hash(k.0 # ":" # k.1)
        }
    );
    
    // Statistics
    private var databaseCount : Nat = 0;
    private var shardCount : Nat = 0;
    private var tableCount : Nat = 0;
    private var indexCount : Nat = 0;
    private var lastUpdateTime : Time.Time = Time.now();
    
    // ===== Helper Functions =====
    
    /// Generate a unique ID using UUID
    private func generateId() : Text {
        // This is a simplified ID generation, in production we'd use a more robust UUID generation
        let now = Int.abs(Time.now());
        let id = Nat32.toText(Nat32.fromIntWrap(now)) # "-" # Nat32.toText(Nat32.fromIntWrap(now / 1000000));
        return id;
    };
    
    /// Find tables in a database schema
    private func findTablesInSchema(schema : DatabaseSchema) : [Text] {
        Array.map<TableSchema, Text>(schema.tables, func(t : TableSchema) : Text {
            t.name
        });
    };
    
    /// Count indexes in a database schema
    private func countIndexesInSchema(schema : DatabaseSchema) : Nat {
        var count = 0;
        for (table in schema.tables.vals()) {
            count += table.indexes.size();
        };
        return count;
    };
    
    /// Validate database name
    private func validateDatabaseName(name : Text) : Bool {
        // Check if name is not empty
        if (Text.size(name) == 0) {
            return false;
        };
        
        // Check if name contains only valid characters
        for (char in Text.toIter(name)) {
            if (not (
                (char >= 'a' and char <= 'z') or
                (char >= 'A' and char <= 'Z') or
                (char >= '0' and char <= '9') or
                char == '_' or char == '-'
            )) {
                return false;
            };
        };
        
        return true;
    };
    
    /// Validate table schema
    private func validateTableSchema(table : TableSchema) : Bool {
        // Check if table name is valid
        if (Text.size(table.name) == 0) {
            return false;
        };
        
        // Check if table has at least one column
        if (table.columns.size() == 0) {
            return false;
        };
        
        // Check if primary key columns exist in table columns
        for (pkCol in table.primaryKey.vals()) {
            let columnExists = Array.some<CommonTypes.ColumnDefinition>(table.columns, func(col) {
                col.name == pkCol
            });
            if (not columnExists) {
                return false;
            };
        };
        
        // Check if index columns exist in table columns
        for (index in table.indexes.vals()) {
            for (indexCol in index.columns.vals()) {
                let columnExists = Array.some<CommonTypes.ColumnDefinition>(table.columns, func(col) {
                    col.name == indexCol
                });
                if (not columnExists) {
                    return false;
                };
            };
        };
        
        return true;
    };
    
    /// Validate database schema
    private func validateDatabaseSchema(schema : DatabaseSchema) : Bool {
        // Check if database name is valid
        if (not validateDatabaseName(schema.name)) {
            return false;
        };
        
        // Check if database has at least one table
        if (schema.tables.size() == 0) {
            return false;
        };
        
        // Validate each table in the schema
        for (table in schema.tables.vals()) {
            if (not validateTableSchema(table)) {
                return false;
            };
        };
        
        return true;
    };
    
    // ===== Database Management =====
    
    /// Register a new database
    public shared(msg) func registerDatabase(request : DatabaseRegistrationRequest) : async DatabaseResult<DatabaseId> {
        // Validate request
        if (not validateDatabaseName(request.name)) {
            return #err(#InvalidDatabaseName("Database name must be non-empty and contain only alphanumeric characters, hyphens, and underscores"));
        };
        
        if (not validateDatabaseSchema(request.schema)) {
            return #err(#InvalidDatabaseName("Invalid schema definition"));
        };
        
        // Check if database with the same name already exists
        let existingDb = Array.find<DatabaseMetadata>(Iter.toArray(databaseRegistry.vals()), func(db) {
            db.name == request.name
        });
        
        switch (existingDb) {
            case (?db) {
                return #err(#DatabaseAlreadyExists("Database with name '" # request.name # "' already exists"));
            };
            case (null) {
                // Generate database ID
                let dbId = generateId();
                
                // Create database metadata
                let now = Time.now();
                let dbMetadata : DatabaseMetadata = {
                    id = dbId;
                    name = request.name;
                    owner = msg.caller;
                    schema = request.schema;
                    created = now;
                    lastModified = now;
                    shardingStrategy = request.shardingStrategy;
                    shards = [];
                    status = #Initializing;
                    version = 1;
                };
                
                // Store database metadata
                databaseRegistry.put(dbId, dbMetadata);
                
                // Update statistics
                databaseCount += 1;
                tableCount += request.schema.tables.size();
                indexCount += countIndexesInSchema(request.schema);
                lastUpdateTime := now;
                
                return #ok(dbId);
            };
        };
    };
    
    /// Get database by ID
    public query func getDatabase(dbId : DatabaseId) : async DatabaseResult<DatabaseMetadata> {
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                return #ok(db);
            };
            case (null) {
                return #err(#DatabaseNotFound("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    /// Get database schema
    public query func getDatabaseSchema(dbId : DatabaseId) : async DatabaseResult<DatabaseSchema> {
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                return #ok(db.schema);
            };
            case (null) {
                return #err(#DatabaseNotFound("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    /// List all databases
    public query func listDatabases() : async [DatabaseInfo] {
        let databases = Iter.toArray(databaseRegistry.vals());
        Array.map<DatabaseMetadata, DatabaseInfo>(databases, func(db) {
            {
                id = db.id;
                name = db.name;
                tableCount = db.schema.tables.size();
                shardCount = db.shards.size();
                created = db.created;
                lastModified = db.lastModified;
                status = db.status;
            }
        });
    };
    
    /// Update database schema
    public shared(msg) func updateDatabaseSchema(dbId : DatabaseId, schema : DatabaseSchema) : async DatabaseResult<()> {
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                // Check if caller is the owner
                if (msg.caller != db.owner) {
                    return #err(#OperationNotPermitted("Only the database owner can update the schema"));
                };
                
                // Validate schema
                if (not validateDatabaseSchema(schema)) {
                    return #err(#InvalidDatabaseName("Invalid schema definition"));
                };
                
                // Create updated metadata
                let updatedDb : DatabaseMetadata = {
                    id = db.id;
                    name = db.name;
                    owner = db.owner;
                    schema = schema;
                    created = db.created;
                    lastModified = Time.now();
                    shardingStrategy = db.shardingStrategy;
                    shards = db.shards;
                    status = db.status;
                    version = db.version + 1;
                };
                
                // Update database registry
                databaseRegistry.put(dbId, updatedDb);
                
                // Update statistics
                tableCount := tableCount - db.schema.tables.size() + schema.tables.size();
                indexCount := indexCount - countIndexesInSchema(db.schema) + countIndexesInSchema(schema);
                lastUpdateTime := Time.now();
                
                return #ok();
            };
            case (null) {
                return #err(#DatabaseNotFound("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    /// Get table schema from a database
    public query func getTableSchema(dbId : DatabaseId, tableName : Text) : async TableResult<TableSchema> {
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                // Find table in schema
                let tableOpt = Array.find<TableSchema>(db.schema.tables, func(t) {
                    t.name == tableName
                });
                
                switch (tableOpt) {
                    case (?table) {
                        return #ok(table);
                    };
                    case (null) {
                        return #err(#TableNotFound("Table '" # tableName # "' not found in database '" # db.name # "'"));
                    };
                };
            };
            case (null) {
                return #err(#TableNotFound("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    /// List tables in a database
    public query func listTables(dbId : DatabaseId) : async DatabaseResult<[TableInfo]> {
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                let tables = Array.map<TableSchema, TableInfo>(db.schema.tables, func(t) {
                    {
                        name = t.name;
                        columnCount = t.columns.size();
                        indexCount = t.indexes.size();
                        shardCount = db.shards.size(); // This is a simplification, in reality we'd need to count shards per table
                        primaryKey = t.primaryKey;
                    }
                });
                return #ok(tables);
            };
            case (null) {
                return #err(#DatabaseNotFound("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    // ===== Shard Management =====
    
    /// Register a new shard
    public shared(msg) func registerShard(request : ShardRegistrationRequest) : async RegistryResult<ShardId> {
        // Check if database exists
        switch (databaseRegistry.get(request.databaseId)) {
            case (?db) {
                // Generate shard ID
                let shardId = generateId();
                
                // Create shard metadata
                let now = Time.now();
                let shardMetadata : ShardMetadata = {
                    id = shardId;
                    canisterId = request.canisterId;
                    databaseId = request.databaseId;
                    tables = request.tables;
                    keyRange = request.keyRange;
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
                };
                
                // Store shard metadata
                shardRegistry.put(shardId, shardMetadata);
                
                // Update database's shard list
                let updatedDb : DatabaseMetadata = {
                    id = db.id;
                    name = db.name;
                    owner = db.owner;
                    schema = db.schema;
                    created = db.created;
                    lastModified = now;
                    shardingStrategy = db.shardingStrategy;
                    shards = Array.append<ShardId>(db.shards, [shardId]);
                    status = db.status;
                    version = db.version;
                };
                databaseRegistry.put(db.id, updatedDb);
                
                // Update statistics
                shardCount += 1;
                lastUpdateTime := now;
                
                return #ok(shardId);
            };
            case (null) {
                return #err(#RegistryOperationFailed("Database with ID '" # request.databaseId # "' not found"));
            };
        };
    };
    
    /// Get shard information
    public query func getShardInfo(shardId : ShardId) : async RegistryResult<ShardInfo> {
        switch (shardRegistry.get(shardId)) {
            case (?shard) {
                return #ok({
                    id = shard.id;
                    canisterId = shard.canisterId;
                    databaseId = shard.databaseId;
                    tables = shard.tables;
                    keyRange = shard.keyRange;
                    status = shard.status;
                    metrics = shard.metrics;
                });
            };
            case (null) {
                return #err(#RecordNotFound("Shard with ID '" # shardId # "' not found"));
            };
        };
    };
    
    /// Update shard information
    public shared(msg) func updateShardInfo(shardId : ShardId, status : ?ShardStatus, metrics : ?ShardMetrics) : async RegistryResult<()> {
        switch (shardRegistry.get(shardId)) {
            case (?shard) {
                // In production, we'd verify that the caller is authorized to update this shard
                
                // Create updated metadata
                let updatedShard : ShardMetadata = {
                    id = shard.id;
                    canisterId = shard.canisterId;
                    databaseId = shard.databaseId;
                    tables = shard.tables;
                    keyRange = shard.keyRange;
                    created = shard.created;
                    lastModified = Time.now();
                    status = switch (status) {
                        case (?s) { s };
                        case (null) { shard.status };
                    };
                    metrics = switch (metrics) {
                        case (?m) { m };
                        case (null) { shard.metrics };
                    };
                    replicaShards = shard.replicaShards;
                };
                
                // Update shard registry
                shardRegistry.put(shardId, updatedShard);
                lastUpdateTime := Time.now();
                
                return #ok();
            };
            case (null) {
                return #err(#RecordNotFound("Shard with ID '" # shardId # "' not found"));
            };
        };
    };
    
    /// List all shards for a database
    public query func getShardsByDatabase(dbId : DatabaseId) : async [ShardInfo] {
        let allShards = Iter.toArray(shardRegistry.vals());
        let dbShards = Array.filter<ShardMetadata>(allShards, func(s) {
            s.databaseId == dbId
        });
        
        Array.map<ShardMetadata, ShardInfo>(dbShards, func(shard) {
            {
                id = shard.id;
                canisterId = shard.canisterId;
                databaseId = shard.databaseId;
                tables = shard.tables;
                keyRange = shard.keyRange;
                status = shard.status;
                metrics = shard.metrics;
            }
        });
    };
    
    // ===== Routing Information =====
    
    /// Get routing information for a table
    public query func getRoutingInfo(request : RoutingRequest) : async RegistryResult<RoutingInfo> {
        let key = (request.databaseId, request.tableName);
        
        switch (routingRules.get(key)) {
            case (?rules) {
                if (rules.size() == 0) {
                    return #err(#RecordNotFound("No routing rules found for table '" # request.tableName # "' in database '" # request.databaseId # "'"));
                };
                
                // Find the highest priority rule
                let sortedRules = Array.sort<RoutingRule>(rules, func(a, b) {
                    a.priority > b.priority // Higher priority first
                });
                
                let rule = sortedRules[0];
                
                return #ok({
                    databaseId = rule.databaseId;
                    tableName = rule.tableName;
                    routingType = rule.routingType;
                    shardIds = rule.shardIds;
                });
            };
            case (null) {
                return #err(#RecordNotFound("No routing rules found for table '" # request.tableName # "' in database '" # request.databaseId # "'"));
            };
        };
    };
    
    /// Get shards for a table and optional key
    public query func getShardsForKey(dbId : DatabaseId, tableName : Text, key : ?Key) : async [ShardId] {
        // Find routing rules for this table
        let routingKey = (dbId, tableName);
        
        switch (routingRules.get(routingKey)) {
            case (?rules) {
                if (rules.size() == 0) {
                    // If no rules exist, return all shards for this database
                    let db = databaseRegistry.get(dbId);
                    switch (db) {
                        case (?database) {
                            return database.shards;
                        };
                        case (null) {
                            return [];
                        };
                    };
                };
                
                // Find the highest priority rule
                let sortedRules = Array.sort<RoutingRule>(rules, func(a, b) {
                    a.priority > b.priority // Higher priority first
                });
                
                let rule = sortedRules[0];
                
                // Apply routing logic based on the rule type
                switch (rule.routingType) {
                    case (#HashBased(_)) {
                        // For hash-based routing, if a key is provided, find the appropriate shard
                        // Otherwise, return all shards
                        switch (key) {
                            case (?k) {
                                // This is a simplified hash-based routing
                                // In production, we'd use a proper consistent hashing algorithm
                                let hash = Text.hash(debug_show(k));
                                let index = hash % rule.shardIds.size();
                                return [rule.shardIds[index]];
                            };
                            case (null) {
                                return rule.shardIds;
                            };
                        };
                    };
                    case (#RangeBased(rb)) {
                        // For range-based routing, if a key is provided, find the appropriate range
                        // Otherwise, return all shards
                        switch (key) {
                            case (?k) {
                                switch (rb.keyRanges) {
                                    case (keyRanges) {
                                        // Find the range that contains this key
                                        // This is a simplified implementation
                                        for ((startKey, endKey, shardId) in keyRanges.vals()) {
                                            // Implement proper key comparison based on the actual type
                                            // This is a placeholder implementation
                                            let keyStr = debug_show(k);
                                            let startStr = debug_show(startKey);
                                            let endStr = debug_show(endKey);
                                            
                                            if (keyStr >= startStr and keyStr < endStr) {
                                                return [shardId];
                                            };
                                        };
                                    };
                                };
                                
                                // If no range matches, return all shards
                                return rule.shardIds;
                            };
                            case (null) {
                                return rule.shardIds;
                            };
                        };
                    };
                    case (#DirectoryBased(db)) {
                        // For directory-based routing, if a key is provided, look up the shard
                        // Otherwise, return all shards
                        switch (key) {
                            case (?k) {
                                switch (db.mappings) {
                                    case (mappings) {
                                        // Find the direct mapping for this key
                                        for ((mappingKey, shardId) in mappings.vals()) {
                                            // Implement proper key comparison based on the actual type
                                            // This is a placeholder implementation
                                            if (debug_show(k) == debug_show(mappingKey)) {
                                                return [shardId];
                                            };
                                        };
                                    };
                                };
                                
                                // If no mapping found, return all shards
                                return rule.shardIds;
                            };
                            case (null) {
                                return rule.shardIds;
                            };
                        };
                    };
                    case (#Broadcast) {
                        // For broadcast tables, always return all shards
                        return rule.shardIds;
                    };
                };
            };
            case (null) {
                // If no rules exist, return all shards for this database
                let db = databaseRegistry.get(dbId);
                switch (db) {
                    case (?database) {
                        return database.shards;
                    };
                    case (null) {
                        return [];
                    };
                };
            };
        };
    };
    
    /// Register a routing rule
    public shared(msg) func registerRoutingRule(
        dbId : DatabaseId,
        tableName : Text,
        routingType : RoutingType,
        shardIds : [ShardId],
        priority : Nat
    ) : async RegistryResult<()> {
        // Check if database exists
        switch (databaseRegistry.get(dbId)) {
            case (?db) {
                // Check if table exists
                let tableExists = Array.some<TableSchema>(db.schema.tables, func(t) {
                    t.name == tableName
                });
                
                if (not tableExists) {
                    return #err(#InvalidRegistration("Table '" # tableName # "' does not exist in database '" # db.name # "'"));
                };
                
                // Check if all shards exist and belong to this database
                for (shardId in shardIds.vals()) {
                    switch (shardRegistry.get(shardId)) {
                        case (?shard) {
                            if (shard.databaseId != dbId) {
                                return #err(#InvalidRegistration("Shard '" # shardId # "' does not belong to database '" # db.name # "'"));
                            };
                        };
                        case (null) {
                            return #err(#InvalidRegistration("Shard '" # shardId # "' does not exist"));
                        };
                    };
                };
                
                // Create routing rule
                let now = Time.now();
                let rule : RoutingRule = {
                    databaseId = dbId;
                    tableName = tableName;
                    routingType = routingType;
                    shardIds = shardIds;
                    priority = priority;
                    created = now;
                    lastModified = now;
                };
                
                // Get existing rules for this table
                let key = (dbId, tableName);
                let existingRules = switch (routingRules.get(key)) {
                    case (?rules) { rules };
                    case (null) { [] };
                };
                
                // Add new rule
                let updatedRules = Array.append<RoutingRule>(existingRules, [rule]);
                routingRules.put(key, updatedRules);
                
                lastUpdateTime := now;
                
                return #ok();
            };
            case (null) {
                return #err(#InvalidRegistration("Database with ID '" # dbId # "' not found"));
            };
        };
    };
    
    // ===== Administration =====
    
    /// Get system statistics
    public query func getStats() : async {
        databaseCount : Nat;
        shardCount : Nat;
        tableCount : Nat;
        indexCount : Nat;
        lastUpdateTime : Time.Time;
    } {
        {
            databaseCount = databaseCount;
            shardCount = shardCount;
            tableCount = tableCount;
            indexCount = indexCount;
            lastUpdateTime = lastUpdateTime;
        }
    };
    
    /// Initiate rebalancing of shards
    public shared(msg) func initiateRebalance(request : RebalanceRequest) : async AdminResult<RebalanceResponse> {
        // In a real implementation, this would trigger a complex rebalancing process
        // For this initial version, we'll simply return a placeholder response
        
        let rebalanceId = generateId();
        
        return #ok({
            requestId = rebalanceId;
            status = #Started;
            affectedShards = []; // In a real implementation, this would be the list of shards that will be rebalanced
        });
    };
}