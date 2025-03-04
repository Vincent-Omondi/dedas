/**
 * Error definitions for DEDaS
 * 
 * This module contains error definitions and error handling utilities
 * for the DEDaS system.
 */

import Text "mo:base/Text";
import Result "mo:base/Result";

module {
    // ===== Error Types =====

    /// Database related errors
    public type DatabaseError = {
        #DatabaseNotFound : Text;
        #DatabaseAlreadyExists : Text;
        #InvalidDatabaseName : Text;
        #DatabaseCreationFailed : Text;
        #OperationNotPermitted : Text;
        #InternalError : Text;
    };

    /// Schema related errors
    public type SchemaError = {
        #TableNotFound : Text;
        #TableAlreadyExists : Text;
        #InvalidTableName : Text;
        #InvalidSchemaDefinition : Text;
        #ColumnNotFound : Text;
        #InvalidColumnDefinition : Text;
        #IncompatibleSchemaChange : Text;
        #MigrationFailed : Text;
        #InternalError : Text;
    };

    /// Query related errors
    public type QueryError = {
        #ParseError : Text;
        #ValidationError : Text;
        #ExecutionError : Text;
        #UnsupportedOperation : Text;
        #ConstraintViolation : Text;
        #TransactionError : Text;
        #TimeoutError : Text;
        #AccessDenied : Text;
        #ResourceExhausted : Text;
        #InternalError : Text;
    };

    /// Shard related errors
    public type ShardError = {
        #ShardNotFound : Text;
        #ShardCreationFailed : Text;
        #ShardCapacityExceeded : Text;
        #ShardUnavailable : Text;
        #DataCorruption : Text;
        #InternalError : Text;
    };

    /// Registry related errors
    public type RegistryError = {
        #RegistryOperationFailed : Text;
        #InvalidRegistration : Text;
        #RecordNotFound : Text;
        #DuplicateRecord : Text;
        #VersionMismatch : Text;
        #InternalError : Text;
    };

    /// Transaction related errors
    public type TransactionError = {
        #TransactionNotFound : Text;
        #InvalidTransactionState : Text;
        #PrepareError : Text;
        #CommitError : Text;
        #AbortError : Text;
        #ConcurrencyConflict : Text;
        #Timeout : Text;
        #InternalError : Text;
    };

    /// Authorization related errors
    public type AuthError = {
        #NotAuthenticated : Text;
        #NotAuthorized : Text;
        #InvalidCredentials : Text;
        #SessionExpired : Text;
        #AccessDenied : Text;
        #InternalError : Text;
    };

    /// Administration related errors
    public type AdminError = {
        #OperationFailed : Text;
        #InvalidConfiguration : Text;
        #ResourceUnavailable : Text;
        #InternalError : Text;
    };

    /// Index related errors
    public type IndexError = {
        #IndexNotFound : Text;
        #IndexAlreadyExists : Text;
        #InvalidIndexDefinition : Text;
        #IndexCreationFailed : Text;
        #IndexUpdateFailed : Text;
        #InternalError : Text;
    };

    /// Table alteration related errors
    public type TableError = {
        #TableNotFound : Text;
        #TableAlreadyExists : Text;
        #InvalidTableDefinition : Text;
        #ColumnNotFound : Text;
        #ColumnAlreadyExists : Text;
        #InvalidColumnDefinition : Text;
        #AlterationFailed : Text;
        #InternalError : Text;
    };

    // ===== Error Conversion Function =====

    /// Convert an error to a text representation
    public func errorToText<E>(error : E, getTextFn : E -> Text) : Text {
        getTextFn(error)
    };

    /// Convert DatabaseError to Text
    public func databaseErrorToText(error : DatabaseError) : Text {
        switch (error) {
            case (#DatabaseNotFound(msg)) { "Database not found: " # msg };
            case (#DatabaseAlreadyExists(msg)) { "Database already exists: " # msg };
            case (#InvalidDatabaseName(msg)) { "Invalid database name: " # msg };
            case (#DatabaseCreationFailed(msg)) { "Database creation failed: " # msg };
            case (#OperationNotPermitted(msg)) { "Operation not permitted: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    /// Convert SchemaError to Text
    public func schemaErrorToText(error : SchemaError) : Text {
        switch (error) {
            case (#TableNotFound(msg)) { "Table not found: " # msg };
            case (#TableAlreadyExists(msg)) { "Table already exists: " # msg };
            case (#InvalidTableName(msg)) { "Invalid table name: " # msg };
            case (#InvalidSchemaDefinition(msg)) { "Invalid schema definition: " # msg };
            case (#ColumnNotFound(msg)) { "Column not found: " # msg };
            case (#InvalidColumnDefinition(msg)) { "Invalid column definition: " # msg };
            case (#IncompatibleSchemaChange(msg)) { "Incompatible schema change: " # msg };
            case (#MigrationFailed(msg)) { "Migration failed: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    /// Convert QueryError to Text
    public func queryErrorToText(error : QueryError) : Text {
        switch (error) {
            case (#ParseError(msg)) { "Parse error: " # msg };
            case (#ValidationError(msg)) { "Validation error: " # msg };
            case (#ExecutionError(msg)) { "Execution error: " # msg };
            case (#UnsupportedOperation(msg)) { "Unsupported operation: " # msg };
            case (#ConstraintViolation(msg)) { "Constraint violation: " # msg };
            case (#TransactionError(msg)) { "Transaction error: " # msg };
            case (#TimeoutError(msg)) { "Timeout error: " # msg };
            case (#AccessDenied(msg)) { "Access denied: " # msg };
            case (#ResourceExhausted(msg)) { "Resource exhausted: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    /// Convert ShardError to Text
    public func shardErrorToText(error : ShardError) : Text {
        switch (error) {
            case (#ShardNotFound(msg)) { "Shard not found: " # msg };
            case (#ShardCreationFailed(msg)) { "Shard creation failed: " # msg };
            case (#ShardCapacityExceeded(msg)) { "Shard capacity exceeded: " # msg };
            case (#ShardUnavailable(msg)) { "Shard unavailable: " # msg };
            case (#DataCorruption(msg)) { "Data corruption: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    /// Convert RegistryError to Text
    public func registryErrorToText(error : RegistryError) : Text {
        switch (error) {
            case (#RegistryOperationFailed(msg)) { "Registry operation failed: " # msg };
            case (#InvalidRegistration(msg)) { "Invalid registration: " # msg };
            case (#RecordNotFound(msg)) { "Record not found: " # msg };
            case (#DuplicateRecord(msg)) { "Duplicate record: " # msg };
            case (#VersionMismatch(msg)) { "Version mismatch: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    /// Convert TransactionError to Text
    public func transactionErrorToText(error : TransactionError) : Text {
        switch (error) {
            case (#TransactionNotFound(msg)) { "Transaction not found: " # msg };
            case (#InvalidTransactionState(msg)) { "Invalid transaction state: " # msg };
            case (#PrepareError(msg)) { "Prepare error: " # msg };
            case (#CommitError(msg)) { "Commit error: " # msg };
            case (#AbortError(msg)) { "Abort error: " # msg };
            case (#ConcurrencyConflict(msg)) { "Concurrency conflict: " # msg };
            case (#Timeout(msg)) { "Timeout: " # msg };
            case (#InternalError(msg)) { "Internal error: " # msg };
        }
    };

    // ===== Result Type Aliases =====

    /// Result type for database operations
    public type DatabaseResult<T> = Result.Result<T, DatabaseError>;
    
    /// Result type for schema operations
    public type SchemaResult<T> = Result.Result<T, SchemaError>;
    
    /// Result type for query operations
    public type QueryResult<T> = Result.Result<T, QueryError>;
    
    /// Result type for shard operations
    public type ShardResult<T> = Result.Result<T, ShardError>;
    
    /// Result type for registry operations
    public type RegistryResult<T> = Result.Result<T, RegistryError>;
    
    /// Result type for transaction operations
    public type TransactionResult<T> = Result.Result<T, TransactionError>;
    
    /// Result type for authorization operations
    public type AuthResult<T> = Result.Result<T, AuthError>;
    
    /// Result type for administration operations
    public type AdminResult<T> = Result.Result<T, AdminError>;
    
    /// Result type for index operations
    public type IndexResult<T> = Result.Result<T, IndexError>;
    
    /// Result type for table operations
    public type TableResult<T> = Result.Result<T, TableError>;

    // ===== Error Constants =====

    /// Error codes
    public let ERROR_DB_001 = "ERR-DB-001"; // Database not found
    public let ERROR_SCHEMA_002 = "ERR-SCHEMA-002"; // Table schema violation
    public let ERROR_QUERY_003 = "ERR-QUERY-003"; // Query syntax error
    public let ERROR_SHARD_004 = "ERR-SHARD-004"; // Shard not reachable
    public let ERROR_AUTH_005 = "ERR-AUTH-005"; // Authorization failed
    public let ERROR_TXN_006 = "ERR-TXN-006"; // Transaction conflict
}