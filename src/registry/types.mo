/**
 * Registry canister type definitions
 * 
 * This module contains type definitions specific to the Registry Canister,
 * which is responsible for maintaining metadata about schemas, shards,
 * and routing information.
 */

import Time "mo:base/Time";
import Principal "mo:base/Principal";
import CommonTypes "../common/types";

module {
    // Import common types
    public type DatabaseId = CommonTypes.DatabaseId;
    public type ShardId = CommonTypes.ShardId;
    public type DatabaseSchema = CommonTypes.DatabaseSchema;
    public type TableSchema = CommonTypes.TableSchema;
    public type IndexDefinition = CommonTypes.IndexDefinition;
    public type Key = CommonTypes.Key;
    public type ShardingStrategy = CommonTypes.ShardingStrategy;
    public type RebalanceStrategy = CommonTypes.RebalanceStrategy;

    // ===== Registry State Types =====

    /// Represents the registry state
    public type RegistryState = {
        var databaseRegistry : DatabaseRegistry;
        var shardRegistry : ShardRegistry;
        var routingRegistry : RoutingRegistry;
        var statistics : RegistryStatistics;
    };

    /// Represents the database registry
    public type DatabaseRegistry = {
        var databases : [DatabaseMetadata];
    };

    /// Represents the shard registry
    public type ShardRegistry = {
        var shards : [ShardMetadata];
    };

    /// Represents the routing registry
    public type RoutingRegistry = {
        var routes : [RoutingRule];
    };

    /// Represents registry statistics
    public type RegistryStatistics = {
        var databaseCount : Nat;
        var shardCount : Nat;
        var tableCount : Nat;
        var indexCount : Nat;
        var lastUpdateTime : Time.Time;
    };

    // ===== Database Metadata Types =====

    /// Represents database metadata
    public type DatabaseMetadata = {
        id : DatabaseId;
        name : Text;
        owner : Principal;
        schema : DatabaseSchema;
        created : Time.Time;
        lastModified : Time.Time;
        shardingStrategy : ShardingStrategy;
        shards : [ShardId];
        status : DatabaseStatus;
        version : Nat;
    };

    /// Represents database status
    public type DatabaseStatus = {
        #Active;
        #Initializing;
        #Migrating;
        #Suspended;
        #Archived;
    };

    // ===== Shard Metadata Types =====

    /// Represents shard metadata
    public type ShardMetadata = {
        id : ShardId;
        canisterId : Principal;
        databaseId : DatabaseId;
        tables : [Text];
        keyRange : ?KeyRange;
        created : Time.Time;
        lastModified : Time.Time;
        status : ShardStatus;
        metrics : ShardMetrics;
        replicaShards : [ShardId];
    };

    /// Represents a key range for range-based sharding
    public type KeyRange = {
        startKey : ?Key;
        endKey : ?Key;
    };

    /// Represents shard status
    public type ShardStatus = {
        #Active;
        #Provisioning;
        #Rebalancing;
        #Migrating;
        #Archived;
        #Unavailable;
    };

    /// Represents shard metrics
    public type ShardMetrics = {
        dataSize : Nat;
        recordCount : Nat;
        avgQueryLatency : Nat64;
        queryCount : Nat;
        writeCount : Nat;
        errorCount : Nat;
        cpuCycles : Nat64;
        memoryUsage : Nat;
        lastUpdated : Time.Time;
    };

    // ===== Routing Types =====

    /// Represents a routing rule
    public type RoutingRule = {
        databaseId : DatabaseId;
        tableName : Text;
        routingType : RoutingType;
        shardIds : [ShardId];
        priority : Nat;
        created : Time.Time;
        lastModified : Time.Time;
    };

    /// Represents a routing type
    public type RoutingType = {
        #HashBased : {
            column : Text;
        };
        #RangeBased : {
            column : Text;
            keyRanges : [(Key, Key, ShardId)]; // (startKey, endKey, shardId)
        };
        #DirectoryBased : {
            mappings : [(Key, ShardId)]; // (key, shardId)
        };
        #Broadcast; // For small tables that are replicated across all shards
    };

    // ===== Operation Types =====

    /// Represents a database registration request
    public type DatabaseRegistrationRequest = {
        name : Text;
        schema : DatabaseSchema;
        shardingStrategy : ShardingStrategy;
    };

    /// Represents a shard registration request
    public type ShardRegistrationRequest = {
        canisterId : Principal;
        databaseId : DatabaseId;
        tables : [Text];
        keyRange : ?KeyRange;
    };

    /// Represents a routing information request
    public type RoutingRequest = {
        databaseId : DatabaseId;
        tableName : Text;
        key : ?Key;
    };

    /// Represents information about a shard
    public type ShardInfo = {
        id : ShardId;
        canisterId : Principal;
        databaseId : DatabaseId;
        tables : [Text];
        keyRange : ?KeyRange;
        status : ShardStatus;
        metrics : ShardMetrics;
    };

    /// Represents a database information response
    public type DatabaseInfo = {
        id : DatabaseId;
        name : Text;
        tableCount : Nat;
        shardCount : Nat;
        created : Time.Time;
        lastModified : Time.Time;
        status : DatabaseStatus;
    };

    /// Represents a table information response
    public type TableInfo = {
        name : Text;
        columnCount : Nat;
        indexCount : Nat;
        shardCount : Nat;
        primaryKey : [Text];
    };

    /// Represents a routing information response
    public type RoutingInfo = {
        databaseId : DatabaseId;
        tableName : Text;
        routingType : RoutingType;
        shardIds : [ShardId];
    };

    /// Represents a rebalance request
    public type RebalanceRequest = {
        strategy : RebalanceStrategy;
        databaseId : ?DatabaseId;
        tableName : ?Text;
        priority : {
            #High;
            #Normal;
            #Low;
        };
    };

    /// Represents a rebalance response
    public type RebalanceResponse = {
        requestId : Text;
        status : {
            #Started;
            #Completed;
            #Failed : Text;
        };
        affectedShards : [ShardId];
    };
}