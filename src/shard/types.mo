/**
 * Shard Canister type definitions
 * 
 * This module contains type definitions specific to the Shard Canister,
 * which is responsible for storing and manipulating data partitions.
 */

import Time "mo:base/Time";
import Principal "mo:base/Principal";
import CommonTypes "../common/types";
import CoordinatorTypes "../query_coordinator/types";

module {
    // Import common types
    public type DatabaseId = CommonTypes.DatabaseId;
    public type ShardId = CommonTypes.ShardId;
    public type TransactionId = CommonTypes.TransactionId;
    public type Key = CommonTypes.Key;
    public type Value = CommonTypes.Value;
    public type Record = CommonTypes.Record;
    public type TableSchema = CommonTypes.TableSchema;
    public type ResultSet = CommonTypes.ResultSet;
    public type FilterExpression = CommonTypes.FilterExpression;
    
    // Import coordinator types
    public type ShardOperation = CoordinatorTypes.ShardOperation;
    public type ShardQuery = CoordinatorTypes.ShardQuery;
    public type ShardResult = CoordinatorTypes.ShardResult;
    public type PrepareResult = CoordinatorTypes.PrepareResult;
    public type CommitResult = CoordinatorTypes.CommitResult;
    public type TransactionOperation = CoordinatorTypes.TransactionOperation;
    
    // ===== Shard State Types =====
    
    /// Represents the shard state
    public type ShardState = {
        var id : ShardId;
        var databaseId : DatabaseId;
        var tables : [TableStorage];
        var transactions : [TransactionState];
        var statistics : ShardStatistics;
    };
    
    /// Represents a table's storage
    public type TableStorage = {
        name : Text;
        schema : TableSchema;
        var records : [Record];
        var indexes : [IndexStorage];
    };
    
    /// Represents an index storage
    public type IndexStorage = {
        name : Text;
        indexType : {
            #BTree;
            #Hash;
            #Inverted;
            #Spatial;
        };
        columns : [Text];
        unique : Bool;
        var entries : [(Key, [Nat])]; // Key -> [row indices]
    };
    
    /// Represents shard statistics
    public type ShardStatistics = {
        var recordCount : Nat;
        var dataSize : Nat;
        var queryCount : Nat;
        var writeCount : Nat;
        var errorCount : Nat;
        var lastAccessTime : Time.Time;
        var creationTime : Time.Time;
    };
    
    // ===== Transaction Types =====
    
    /// Represents a transaction state
    public type TransactionState = {
        id : TransactionId;
        status : TransactionStatus;
        operations : [TransactionOperation];
        createdAt : Time.Time;
        updatedAt : Time.Time;
        coordinator : Principal;
        var prepareLog : ?PrepareLog;
    };
    
    /// Represents a transaction status in the shard
    public type TransactionStatus = {
        #Active;
        #Prepared;
        #Committed;
        #Aborted;
    };
    
    /// Represents a prepare log for two-phase commit
    public type PrepareLog = {
        preparedAt : Time.Time;
        operations : [PreparedOperation];
        undo : [UndoOperation];
    };
    
    /// Represents a prepared operation
    public type PreparedOperation = {
        #Insert : {
            tableName : Text;
            records : [Record];
            rowIndices : [Nat]; // Position in the table where records will be inserted
        };
        #Update : {
            tableName : Text;
            updates : [(Text, Value)];
            affectedRows : [Nat]; // Indices of rows to be updated
            oldValues : [[(Text, Value)]]; // Original values for rollback
        };
        #Delete : {
            tableName : Text;
            affectedRows : [Nat]; // Indices of rows to be deleted
            oldRecords : [Record]; // Original records for rollback
        };
    };
    
    /// Represents an undo operation for rollback
    public type UndoOperation = {
        #UndoInsert : {
            tableName : Text;
            rowIndices : [Nat]; // Positions to remove
        };
        #UndoUpdate : {
            tableName : Text;
            affectedRows : [Nat]; // Indices of rows to restore
            oldValues : [[(Text, Value)]]; // Original values to restore
        };
        #UndoDelete : {
            tableName : Text;
            rowIndices : [Nat]; // Positions to restore at
            records : [Record]; // Records to restore
        };
    };
    
    // ===== Query Processing Types =====
    
    /// Represents a filter result for query execution
    public type FilterResult = {
        #Match : [Nat]; // Indices of matching rows
        #NoMatch;
        #Error : Text;
    };
    
    /// Represents an index lookup result
    public type IndexLookupResult = {
        #Match : [Nat]; // Indices of matching rows
        #NoMatch;
        #NotIndexed;
        #Error : Text;
    };
    
    /// Represents a row predicate for filtering
    public type RowPredicate = (Record) -> Bool;
    
    // ===== Operation Result Types =====
    
    /// Represents the result of a record insertion
    public type InsertResult = {
        tableName : Text;
        insertedCount : Nat;
        newRecords : [Record];
    };
    
    /// Represents the result of a record update
    public type UpdateResult = {
        tableName : Text;
        updatedCount : Nat;
        updatedIndices : [Nat];
    };
    
    /// Represents the result of a record deletion
    public type DeleteResult = {
        tableName : Text;
        deletedCount : Nat;
        deletedIndices : [Nat];
    };
    
    // ===== Shard Management Types =====
    
    /// Represents a shard initialization request
    public type ShardInitRequest = {
        shardId : ShardId;
        databaseId : DatabaseId;
        tables : [TableSchema];
    };
    
    /// Represents a shard info response
    public type ShardInfo = {
        id : ShardId;
        databaseId : DatabaseId;
        tableCount : Nat;
        recordCount : Nat;
        dataSize : Nat;
        status : ShardStatus;
    };
    
    /// Represents the shard status
    public type ShardStatus = {
        #Active;
        #Initializing;
        #Migrating;
        #Rebalancing;
        #ReadOnly;
        #Error : Text;
    };
    
    /// Represents a schema update request
    public type SchemaUpdateRequest = {
        tables : [TableSchema];
    };
    
    /// Represents metrics of the shard
    public type ShardMetrics = {
        recordCount : Nat;
        dataSize : Nat;
        queryCount : Nat;
        writeCount : Nat;
        errorCount : Nat;
        averageQueryTime : Nat64;
        averageWriteTime : Nat64;
        memoryUsage : Nat;
        cyclesBalance : Nat;
    };
}