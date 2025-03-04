/**
 * Common type definitions for DEDaS
 * 
 * This module contains shared type definitions used across all canisters
 * in the DEDaS system.
 */

import Nat "mo:base/Nat";
import Nat8 "mo:base/Nat8";
import Nat64 "mo:base/Nat64";
import Int "mo:base/Int";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Hash "mo:base/Hash";
import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Principal "mo:base/Principal";

module {
    // ===== Identifier Types =====

    /// Unique identifier for a database within DEDaS
    public type DatabaseId = Text;
    
    /// Unique identifier for a shard canister
    public type ShardId = Text;
    
    /// Unique identifier for a transaction
    public type TransactionId = Text;
    
    /// Unique identifier for a connection
    public type ConnectionId = Text;
    
    /// Unique identifier for an index
    public type IndexId = Text;

    // ===== Data Type Definitions =====

    /// Represents the data types supported in DEDaS
    public type DataType = {
        #Int;
        #Float;
        #Text;
        #Bool;
        #Blob;
        #Null;
        #Array : DataType;
        #Optional : DataType;
        #Record : [FieldType];
        #Variant : [VariantType];
    };
    
    /// Represents a field in a record type
    public type FieldType = {
        name : Text;
        fieldType : DataType;
    };
    
    /// Represents a variant in a variant type
    public type VariantType = {
        name : Text;
        variantType : ?DataType;
    };

    /// Represents a value of any supported data type
    public type Value = {
        #Int : Int;
        #Float : Float;
        #Text : Text;
        #Bool : Bool;
        #Blob : Blob;
        #Null;
        #Array : [Value];
        #Record : [(Text, Value)];
        #Variant : (Text, ?Value);
    };

    // ===== Schema Definitions =====
    
    /// Represents a constraint on a column
    public type Constraint = {
        #PrimaryKey;
        #Unique;
        #NotNull;
        #ForeignKey : ForeignKeyDefinition;
        #Check : Text; // Predicate expression
    };
    
    /// Represents a foreign key relationship
    public type ForeignKeyDefinition = {
        foreignTable : Text;
        foreignColumns : [Text];
        onDelete : ForeignKeyAction;
        onUpdate : ForeignKeyAction;
    };
    
    /// Represents a foreign key action
    public type ForeignKeyAction = {
        #NoAction;
        #Restrict;
        #Cascade;
        #SetNull;
        #SetDefault;
    };
    
    /// Represents a column in a table
    public type ColumnDefinition = {
        name : Text;
        dataType : DataType;
        nullable : Bool;
        defaultValue : ?Value;
        constraints : [Constraint];
    };
    
    /// Represents an index on a table
    public type IndexDefinition = {
        name : Text;
        table : Text;
        columns : [Text];
        unique : Bool;
        indexType : {
            #BTree;
            #Hash;
            #Inverted;
            #Spatial;
        };
    };
    
    /// Represents a table schema
    public type TableSchema = {
        name : Text;
        columns : [ColumnDefinition];
        primaryKey : [Text];
        indexes : [IndexDefinition];
        foreignKeys : [ForeignKeyDefinition];
    };
    
    /// Represents a database schema
    public type DatabaseSchema = {
        name : Text;
        tables : [TableSchema];
    };

    // ===== Record and Query Types =====
    
    /// Represents a record (row) in a table
    public type Record = {
        [Text] : Value;
    };
    
    /// Represents a record key
    public type Key = Value;
    
    /// Represents a filter expression for queries
    public type FilterExpression = {
        #Equals : (Text, Value);
        #NotEquals : (Text, Value);
        #GreaterThan : (Text, Value);
        #LessThan : (Text, Value);
        #GreaterThanOrEqual : (Text, Value);
        #LessThanOrEqual : (Text, Value);
        #Like : (Text, Text);
        #Between : (Text, Value, Value);
        #In : (Text, [Value]);
        #IsNull : Text;
        #IsNotNull : Text;
        #And : [FilterExpression];
        #Or : [FilterExpression];
        #Not : FilterExpression;
    };
    
    /// Represents a query projection element
    public type ProjectionElement = {
        expression : Text;
        alias : ?Text;
    };
    
    /// Represents a table reference in a query
    public type TableReference = {
        table : Text;
        alias : ?Text;
        join : ?JoinSpecification;
    };
    
    /// Represents a join specification
    public type JoinSpecification = {
        joinType : JoinType;
        table : Text;
        alias : ?Text;
        condition : FilterExpression;
    };
    
    /// Represents a join type
    public type JoinType = {
        #Inner;
        #Left;
        #Right;
        #Full;
    };
    
    /// Represents an order element in a query
    public type OrderElement = {
        expression : Text;
        direction : {
            #Ascending;
            #Descending;
        };
    };
    
    // ===== Query Types =====
    
    /// Represents a select query
    public type SelectQuery = {
        tables : [TableReference];
        projection : [ProjectionElement];
        filter : ?FilterExpression;
        groupBy : [Text];
        having : ?FilterExpression;
        orderBy : [OrderElement];
        limit : ?Nat;
        offset : ?Nat;
    };
    
    /// Represents an insert query
    public type InsertQuery = {
        table : Text;
        columns : [Text];
        values : [[Value]];
        returning : [Text];
    };
    
    /// Represents an update query
    public type UpdateQuery = {
        table : Text;
        updates : [(Text, Value)];
        filter : ?FilterExpression;
        returning : [Text];
    };
    
    /// Represents a delete query
    public type DeleteQuery = {
        table : Text;
        filter : ?FilterExpression;
        returning : [Text];
    };
    
    /// Represents a query
    public type Query = {
        #Select : SelectQuery;
        #Insert : InsertQuery;
        #Update : UpdateQuery;
        #Delete : DeleteQuery;
    };
    
    // ===== Result Types =====
    
    /// Represents a query result set
    public type ResultSet = {
        columns : [Text];
        rows : [[Value]];
        rowCount : Nat;
    };
    
    /// Represents a query response
    public type QueryResponse = {
        resultSet : ResultSet;
        executionTime : Nat64;
    };

    // ===== Configuration Types =====
    
    /// Represents a connection configuration
    public type ConnectionConfig = {
        registryCanisterId : Principal;
        applicationPrincipal : Principal;
        databaseName : Text;
    };
    
    /// Represents sharding strategy
    public type ShardingStrategy = {
        #HashBased : {
            column : Text;
        };
        #RangeBased : {
            column : Text;
            ranges : [(Value, Value)];
        };
        #DirectoryBased;
        #GeoBased : {
            column : Text;
        };
    };
    
    /// Represents rebalance strategy
    public type RebalanceStrategy = {
        #EvenDistribution;
        #HotspotMitigation;
        #DataLocality;
        #Custom : Text;
    };
    
    /// Represents a scaling action
    public type ScalingAction = {
        #Split : Key;
        #Merge : ShardId;
        #Replicate;
        #NoAction;
    };
}