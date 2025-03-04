/**
 * DEDaS Backend Main Entry Point
 * 
 * This canister serves as the primary entry point for the Decentralized Elastic
 * Database Service (DEDaS). It provides a unified API for database operations,
 * delegates requests to appropriate canisters, and manages the lifecycle of
 * database components.
 */

import Array "mo:base/Array";
import Debug "mo:base/Debug";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";
import IC "mo:base/ExperimentalInternetComputer";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";
import Auth "../common/auth";

actor DEDaS {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type QueryResponse = CommonTypes.QueryResponse;
    type ConnectionId = CommonTypes.ConnectionId;
    type ConnectionConfig = CommonTypes.ConnectionConfig;
    type TableSchema = CommonTypes.TableSchema;
    type DatabaseSchema = CommonTypes.DatabaseSchema;
    type ShardingStrategy = CommonTypes.ShardingStrategy;
    
    type DatabaseResult<T> = Errors.DatabaseResult<T>;
    type TableResult<T> = Errors.TableResult<T>;
    type QueryResult<T> = Errors.QueryResult<T>;
    
    type Session = Auth.Session;
    type AccessPolicy = Auth.AccessPolicy;
    
    // External actor interfaces
    
    type Registry = actor {
        registerDatabase : (request : {
            name : Text;
            schema : CommonTypes.DatabaseSchema;
            shardingStrategy : CommonTypes.ShardingStrategy;
        }) -> async Result.Result<DatabaseId, Errors.DatabaseError>;
        
        getDatabase : (dbId : DatabaseId) -> async Result.Result<{
            id : DatabaseId;
            name : Text;
            owner : Principal;
            schema : CommonTypes.DatabaseSchema;
            created : Time.Time;
            lastModified : Time.Time;
            shardingStrategy : CommonTypes.ShardingStrategy;
            shards : [ShardId];
            status : {#Active; #Initializing; #Migrating; #Suspended; #Archived};
            version : Nat;
        }, Errors.DatabaseError>;
        
        listDatabases : () -> async [{
            id : DatabaseId;
            name : Text;
            tableCount : Nat;
            shardCount : Nat;
            created : Time.Time;
            lastModified : Time.Time;
            status : {#Active; #Initializing; #Migrating; #Suspended; #Archived};
        }];
        
        updateDatabaseSchema : (dbId : DatabaseId, schema : CommonTypes.DatabaseSchema) -> 
            async Result.Result<(), Errors.DatabaseError>;
        
        listTables : (dbId : DatabaseId) -> async Result.Result<[{
            name : Text;
            columnCount : Nat;
            indexCount : Nat;
            shardCount : Nat;
            primaryKey : [Text];
        }], Errors.DatabaseError>;
    };
    
    type QueryCoordinator = actor {
        executeQuery : (request : {
            databaseId : DatabaseId;
            query : Text;
            timeout : ?Nat64;
            consistency : ?{#Strong; #ReadCommitted; #ReadUncommitted; #Eventual};
        }) -> async Result.Result<CommonTypes.QueryResponse, Errors.QueryError>;
        
        beginTransaction : (dbId : DatabaseId) -> 
            async Result.Result<Text, Errors.TransactionError>;
        
        addTransactionOperation : (txId : Text, operation : {
            #Insert : {
                tableName : Text;
                records : [CommonTypes.Record];
            };
            #Update : {
                tableName : Text;
                updates : [(Text, CommonTypes.Value)];
                filter : ?CommonTypes.FilterExpression;
            };
            #Delete : {
                tableName : Text;
                filter : ?CommonTypes.FilterExpression;
            };
        }) -> async Result.Result<(), Errors.TransactionError>;
        
        commitTransaction : (txId : Text) -> async Result.Result<{
            transactionId : Text;
            status : {#Active; #Preparing; #Prepared; #Committing; #Committed; #Aborting; #Aborted};
            affectedRows : Nat;
        }, Errors.TransactionError>;
        
        rollbackTransaction : (txId : Text) -> async Result.Result<(), Errors.TransactionError>;
    };
    
    // ===== State =====
    
    // Cache of registry and query coordinator canisters
    private var registryCanisterId : ?Principal = null;
    private var queryCoordinatorCanisterId : ?Principal = null;
    
    // Active connections
    private let connections = HashMap.HashMap<ConnectionId, ConnectionState>(10, Text.equal, Text.hash);
    
    // Permissions and policies
    private let accessPolicies = HashMap.HashMap<DatabaseId, [AccessPolicy]>(10, Text.equal, Text.hash);
    
    // Session management
    private let activeSessions = HashMap.HashMap<Text, Session>(50, Text.equal, Text.hash);
    
    // Connection state
    private type ConnectionState = {
        id : ConnectionId;
        config : ConnectionConfig;
        created : Time.Time;
        lastActivity : Time.Time;
        principal : Principal;
        currentDatabase : ?DatabaseId;
    };
    
    // ===== Initialization =====
    
    /// Initialize DEDaS with registry and query coordinator canisters
    public shared(msg) func initialize(
        registry : Principal,
        queryCoordinator : Principal
    ) : async Result.Result<(), Text> {
        // Only allow initialization if not already initialized
        if (registryCanisterId != null or queryCoordinatorCanisterId != null) {
            return #err("DEDaS is already initialized");
        };
        
        // Store canister IDs
        registryCanisterId := ?registry;
        queryCoordinatorCanisterId := ?queryCoordinator;
        
        #ok()
    };
    
    // ===== Helper Functions =====
    
    /// Get the registry actor
    private func getRegistryActor() : Registry {
        switch (registryCanisterId) {
            case (?id) {
                actor(Principal.toText(id)) : Registry
            };
            case (null) {
                Debug.trap("Registry canister ID not initialized");
            };
        };
    };
    
    /// Get the query coordinator actor
    private func getQueryCoordinatorActor() : QueryCoordinator {
        switch (queryCoordinatorCanisterId) {
            case (?id) {
                actor(Principal.toText(id)) : QueryCoordinator
            };
            case (null) {
                Debug.trap("Query coordinator canister ID not initialized");
            };
        };
    };
    
    /// Check if a connection exists and is valid
    private func validateConnection(connectionId : ConnectionId) : Bool {
        switch (connections.get(connectionId)) {
            case (?conn) {
                // Check if connection is recent (within 1 hour)
                let now = Time.now();
                let hourInNanos : Int = 3_600_000_000_000;
                
                if (now - conn.lastActivity > hourInNanos) {
                    connections.delete(connectionId);
                    return false;
                };
                
                // Update last activity time
                let updatedConn = {
                    id = conn.id;
                    config = conn.config;
                    created = conn.created;
                    lastActivity = now;
                    principal = conn.principal;
                    currentDatabase = conn.currentDatabase;
                };
                
                connections.put(connectionId, updatedConn);
                true
            };
            case (null) {
                false
            };
        };
    };
    
    /// Get a connection's current database
    private func getCurrentDatabase(connectionId : ConnectionId) : ?DatabaseId {
        switch (connections.get(connectionId)) {
            case (?conn) {
                conn.currentDatabase
            };
            case (null) {
                null
            };
        };
    };
    
    /// Update a connection's current database
    private func setCurrentDatabase(connectionId : ConnectionId, dbId : DatabaseId) : Bool {
        switch (connections.get(connectionId)) {
            case (?conn) {
                let updatedConn = {
                    id = conn.id;
                    config = conn.config;
                    created = conn.created;
                    lastActivity = Time.now();
                    principal = conn.principal;
                    currentDatabase = ?dbId;
                };
                
                connections.put(connectionId, updatedConn);
                true
            };
            case (null) {
                false
            };
        };
    };
    
    // ===== Public API =====
    
    /// Connect to DEDaS
    public shared(msg) func connect(config : ConnectionConfig) : async Result.Result<ConnectionId, Text> {
        let connectionId = Utils.generateId("conn");
        let now = Time.now();
        
        let connection : ConnectionState = {
            id = connectionId;
            config = config;
            created = now;
            lastActivity = now;
            principal = msg.caller;
            currentDatabase = null;
        };
        
        connections.put(connectionId, connection);
        
        #ok(connectionId)
    };
    
    /// Disconnect from DEDaS
    public shared(msg) func disconnect(connectionId : ConnectionId) : async () {
        // Remove connection
        switch (connections.get(connectionId)) {
            case (?conn) {
                if (Principal.equal(msg.caller, conn.principal)) {
                    connections.delete(connectionId);
                };
            };
            case (null) {
                // Connection already gone, nothing to do
            };
        };
    };
    
    /// Create a new database
    public shared(msg) func createDatabase(
        connectionId : ConnectionId,
        name : Text,
        schema : DatabaseSchema
    ) : async DatabaseResult<DatabaseId> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#OperationNotPermitted("Invalid or expired connection"));
        };
        
        // Validate database name
        if (not Utils.isValidDatabaseName(name)) {
            return #err(#InvalidDatabaseName("Database name must be alphanumeric with optional underscores and hyphens"));
        };
        
        // Default sharding strategy
        let shardingStrategy : ShardingStrategy = #HashBased({
            column = "id" // Assumes a primary key named "id"
        });
        
        // Register database with registry
        let registry = getRegistryActor();
        let result = await registry.registerDatabase({
            name = name;
            schema = schema;
            shardingStrategy = shardingStrategy;
        });
        
        switch (result) {
            case (#ok(dbId)) {
                // Set as current database for the connection
                ignore setCurrentDatabase(connectionId, dbId);
                
                // Initialize access policies for the new database
                // Grant full access to the creator
                let policies : [AccessPolicy] = [{
                    principal = msg.caller;
                    resource = {
                        resourceType = #Database;
                        resourceName = dbId;
                    };
                    permissions = [#Read, #Write, #Admin, #Schema];
                    conditions = null;
                }];
                
                accessPolicies.put(dbId, policies);
                
                #ok(dbId)
            };
            case (#err(e)) {
                #err(e)
            };
        };
    };
    
    /// Use a specific database
    public shared(msg) func useDatabase(
        connectionId : ConnectionId,
        dbId : DatabaseId
    ) : async DatabaseResult<()> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#OperationNotPermitted("Invalid or expired connection"));
        };
        
        // Check if database exists
        let registry = getRegistryActor();
        let result = await registry.getDatabase(dbId);
        
        switch (result) {
            case (#ok(_)) {
                // Set as current database for the connection
                if (setCurrentDatabase(connectionId, dbId)) {
                    #ok()
                } else {
                    #err(#OperationNotPermitted("Failed to set current database"))
                }
            };
            case (#err(e)) {
                #err(e)
            };
        };
    };
    
    /// List available databases
    public query(msg) func listDatabases(
        connectionId : ConnectionId
    ) : async DatabaseResult<[{
        id : DatabaseId;
        name : Text;
        tableCount : Nat;
        shardCount : Nat;
    }]> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#OperationNotPermitted("Invalid or expired connection"));
        };
        
        // We need to make an update call to the registry
        // Since query methods can't call other canisters
        // In a real implementation, we'd cache this data
        #err(#OperationNotPermitted("Operation not supported in query method"))
    };
    
    /// Execute an update call to list databases
    public shared(msg) func listDatabasesUpdate(
        connectionId : ConnectionId
    ) : async DatabaseResult<[{
        id : DatabaseId;
        name : Text;
        tableCount : Nat;
        shardCount : Nat;
    }]> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#OperationNotPermitted("Invalid or expired connection"));
        };
        
        // List databases from registry
        let registry = getRegistryActor();
        let databases = await registry.listDatabases();
        
        let result = Array.map<{
            id : DatabaseId;
            name : Text;
            tableCount : Nat;
            shardCount : Nat;
            created : Time.Time;
            lastModified : Time.Time;
            status : {#Active; #Initializing; #Migrating; #Suspended; #Archived};
        }, {
            id : DatabaseId;
            name : Text;
            tableCount : Nat;
            shardCount : Nat;
        }>(databases, func(db) {
            {
                id = db.id;
                name = db.name;
                tableCount = db.tableCount;
                shardCount = db.shardCount;
            }
        });
        
        #ok(result)
    };
    
    /// Drop a database
    public shared(msg) func dropDatabase(
        connectionId : ConnectionId,
        dbId : DatabaseId
    ) : async DatabaseResult<()> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#OperationNotPermitted("Invalid or expired connection"));
        };
        
        // Check if the user has permission to drop the database
        // In a real implementation, we'd check policies
        
        // This is a placeholder error since drop functionality
        // needs to be implemented in the registry canister
        #err(#OperationNotPermitted("Database deletion not yet implemented"))
    };
    
    /// Create a table in the current database
    public shared(msg) func createTable(
        connectionId : ConnectionId,
        tableSchema : TableSchema
    ) : async TableResult<()> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#InvalidTableDefinition("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Get current schema
                let registry = getRegistryActor();
                let dbResult = await registry.getDatabase(dbId);
                
                switch (dbResult) {
                    case (#ok(database)) {
                        // Check if table already exists
                        let tableExists = Array.some<TableSchema>(
                            database.schema.tables,
                            func(t) { t.name == tableSchema.name }
                        );
                        
                        if (tableExists) {
                            return #err(#TableAlreadyExists("Table " # tableSchema.name # " already exists"));
                        };
                        
                        // Add the new table to the schema
                        let updatedTables = Array.append<TableSchema>(
                            database.schema.tables,
                            [tableSchema]
                        );
                        
                        let updatedSchema : DatabaseSchema = {
                            name = database.schema.name;
                            tables = updatedTables;
                        };
                        
                        // Update the database schema
                        let updateResult = await registry.updateDatabaseSchema(dbId, updatedSchema);
                        
                        switch (updateResult) {
                            case (#ok()) {
                                #ok()
                            };
                            case (#err(e)) {
                                #err(#AlterationFailed("Failed to update schema: " # debug_show(e)))
                            };
                        };
                    };
                    case (#err(e)) {
                        #err(#TableNotFound("Database error: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#TableNotFound("No database selected"))
            };
        };
    };
    
    /// List tables in the current database
    public shared(msg) func listTables(
        connectionId : ConnectionId
    ) : async TableResult<[{
        name : Text;
        columnCount : Nat;
        primaryKey : [Text];
    }]> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#TableNotFound("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Get tables from registry
                let registry = getRegistryActor();
                let tablesResult = await registry.listTables(dbId);
                
                switch (tablesResult) {
                    case (#ok(tables)) {
                        let result = Array.map<{
                            name : Text;
                            columnCount : Nat;
                            indexCount : Nat;
                            shardCount : Nat;
                            primaryKey : [Text];
                        }, {
                            name : Text;
                            columnCount : Nat;
                            primaryKey : [Text];
                        }>(tables, func(t) {
                            {
                                name = t.name;
                                columnCount = t.columnCount;
                                primaryKey = t.primaryKey;
                            }
                        });
                        
                        #ok(result)
                    };
                    case (#err(e)) {
                        #err(#TableNotFound("Database error: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#TableNotFound("No database selected"))
            };
        };
    };
    
    /// Execute a query on the current database
    public shared(msg) func executeQuery(
        connectionId : ConnectionId,
        query : Text
    ) : async QueryResult<QueryResponse> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#ValidationError("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Forward to query coordinator
                let coordinator = getQueryCoordinatorActor();
                let result = await coordinator.executeQuery({
                    databaseId = dbId;
                    query = query;
                    timeout = null;
                    consistency = ?#Strong;
                });
                
                result
            };
            case (null) {
                #err(#ValidationError("No database selected"))
            };
        };
    };
    
    /// Insert records into a table
    public shared(msg) func insertRecords(
        connectionId : ConnectionId,
        tableName : Text,
        records : [CommonTypes.Record]
    ) : async QueryResult<{affectedRows : Nat}> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#ValidationError("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Start a transaction
                let coordinator = getQueryCoordinatorActor();
                let txResult = await coordinator.beginTransaction(dbId);
                
                switch (txResult) {
                    case (#ok(txId)) {
                        // Add insert operation to transaction
                        let opResult = await coordinator.addTransactionOperation(
                            txId,
                            #Insert({
                                tableName = tableName;
                                records = records;
                            })
                        );
                        
                        switch (opResult) {
                            case (#ok()) {
                                // Commit transaction
                                let commitResult = await coordinator.commitTransaction(txId);
                                
                                switch (commitResult) {
                                    case (#ok(txResult)) {
                                        #ok({affectedRows = txResult.affectedRows})
                                    };
                                    case (#err(e)) {
                                        // Try to roll back
                                        ignore await coordinator.rollbackTransaction(txId);
                                        #err(#TransactionError("Commit failed: " # debug_show(e)))
                                    };
                                };
                            };
                            case (#err(e)) {
                                // Try to roll back
                                ignore await coordinator.rollbackTransaction(txId);
                                #err(#TransactionError("Operation failed: " # debug_show(e)))
                            };
                        };
                    };
                    case (#err(e)) {
                        #err(#TransactionError("Failed to start transaction: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("No database selected"))
            };
        };
    };
    
    /// Update records in a table
    public shared(msg) func updateRecords(
        connectionId : ConnectionId,
        tableName : Text,
        filter : ?CommonTypes.FilterExpression,
        updates : [(Text, CommonTypes.Value)]
    ) : async QueryResult<{affectedRows : Nat}> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#ValidationError("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Start a transaction
                let coordinator = getQueryCoordinatorActor();
                let txResult = await coordinator.beginTransaction(dbId);
                
                switch (txResult) {
                    case (#ok(txId)) {
                        // Add update operation to transaction
                        let opResult = await coordinator.addTransactionOperation(
                            txId,
                            #Update({
                                tableName = tableName;
                                updates = updates;
                                filter = filter;
                            })
                        );
                        
                        switch (opResult) {
                            case (#ok()) {
                                // Commit transaction
                                let commitResult = await coordinator.commitTransaction(txId);
                                
                                switch (commitResult) {
                                    case (#ok(txResult)) {
                                        #ok({affectedRows = txResult.affectedRows})
                                    };
                                    case (#err(e)) {
                                        // Try to roll back
                                        ignore await coordinator.rollbackTransaction(txId);
                                        #err(#TransactionError("Commit failed: " # debug_show(e)))
                                    };
                                };
                            };
                            case (#err(e)) {
                                // Try to roll back
                                ignore await coordinator.rollbackTransaction(txId);
                                #err(#TransactionError("Operation failed: " # debug_show(e)))
                            };
                        };
                    };
                    case (#err(e)) {
                        #err(#TransactionError("Failed to start transaction: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("No database selected"))
            };
        };
    };
    
    /// Delete records from a table
    public shared(msg) func deleteRecords(
        connectionId : ConnectionId,
        tableName : Text,
        filter : ?CommonTypes.FilterExpression
    ) : async QueryResult<{affectedRows : Nat}> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#ValidationError("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Start a transaction
                let coordinator = getQueryCoordinatorActor();
                let txResult = await coordinator.beginTransaction(dbId);
                
                switch (txResult) {
                    case (#ok(txId)) {
                        // Add delete operation to transaction
                        let opResult = await coordinator.addTransactionOperation(
                            txId,
                            #Delete({
                                tableName = tableName;
                                filter = filter;
                            })
                        );
                        
                        switch (opResult) {
                            case (#ok()) {
                                // Commit transaction
                                let commitResult = await coordinator.commitTransaction(txId);
                                
                                switch (commitResult) {
                                    case (#ok(txResult)) {
                                        #ok({affectedRows = txResult.affectedRows})
                                    };
                                    case (#err(e)) {
                                        // Try to roll back
                                        ignore await coordinator.rollbackTransaction(txId);
                                        #err(#TransactionError("Commit failed: " # debug_show(e)))
                                    };
                                };
                            };
                            case (#err(e)) {
                                // Try to roll back
                                ignore await coordinator.rollbackTransaction(txId);
                                #err(#TransactionError("Operation failed: " # debug_show(e)))
                            };
                        };
                    };
                    case (#err(e)) {
                        #err(#TransactionError("Failed to start transaction: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("No database selected"))
            };
        };
    };
    
    // ===== System Management =====
    
    /// Get system status
    public query func getStatus() : async {
        initialized : Bool;
        connectionCount : Nat;
    } {
        {
            initialized = registryCanisterId != null and queryCoordinatorCanisterId != null;
            connectionCount = connections.size();
        }
    };
    
    /// Get canister status
    public shared(msg) func getCanisterStatus() : async {
        cycles : Nat;
        memory : Nat;
    } {
        {
            cycles = IC.canisterBalance();
            memory = 0; // In a real implementation, we'd track this
        }
    };
    
    /// Create the necessary canisters for a new database instance
    public shared(msg) func createDatabaseCanisters() : async Result.Result<{
        registry : Principal;
        queryCoordinator : Principal;
        shards : [Principal];
    }, Text> {
        // Check caller's permissions
        // In a real implementation, we'd have proper authorization
        
        // This is a placeholder that would create necessary canisters:
        // - Registry Canister
        // - Query Coordinator Canister
        // - Initial Shard Canisters
        
        #err("Canister creation not implemented in this version")
    };

    // ===== Transaction Management =====
    
    /// Begin a transaction
    public shared(msg) func beginTransaction(
        connectionId : ConnectionId
    ) : async Result.Result<TransactionId, Errors.TransactionError> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#InvalidTransactionState("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Start transaction with query coordinator
                let coordinator = getQueryCoordinatorActor();
                let result = await coordinator.beginTransaction(dbId);
                
                switch (result) {
                    case (#ok(txId)) {
                        #ok(txId)
                    };
                    case (#err(e)) {
                        #err(e)
                    };
                };
            };
            case (null) {
                #err(#InvalidTransactionState("No database selected"))
            };
        };
    };
    
    /// Commit a transaction
    public shared(msg) func commitTransaction(
        connectionId : ConnectionId,
        txId : TransactionId
    ) : async Result.Result<{
        transactionId : TransactionId;
        status : {#Active; #Preparing; #Prepared; #Committing; #Committed; #Aborting; #Aborted};
        affectedRows : Nat;
    }, Errors.TransactionError> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#InvalidTransactionState("Invalid or expired connection"));
        };
        
        // Commit transaction with query coordinator
        let coordinator = getQueryCoordinatorActor();
        let result = await coordinator.commitTransaction(txId);
        
        result
    };
    
    /// Rollback a transaction
    public shared(msg) func rollbackTransaction(
        connectionId : ConnectionId,
        txId : TransactionId
    ) : async Result.Result<(), Errors.TransactionError> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#InvalidTransactionState("Invalid or expired connection"));
        };
        
        // Rollback transaction with query coordinator
        let coordinator = getQueryCoordinatorActor();
        let result = await coordinator.rollbackTransaction(txId);
        
        result
    };
    
    // ===== Authentication and Authorization =====
    
    /// Create session for a principal
    public shared(msg) func createSession() : async Result.Result<Session, Text> {
        let sessionId = Utils.generateId("session");
        let now = Time.now();
        let sessionDuration = 24 * 60 * 60 * 1000000000; // 24 hours in nanoseconds
        
        let session : Session = {
            id = sessionId;
            principal = msg.caller;
            created = now;
            expires = now + sessionDuration;
            permissions = []; // Default permissions
        };
        
        activeSessions.put(sessionId, session);
        
        #ok(session)
    };
    
    /// Validate session
    public query(msg) func validateSession(sessionId : Text) : async Bool {
        switch (activeSessions.get(sessionId)) {
            case (?session) {
                // Check if session belongs to caller
                if (not Principal.equal(msg.caller, session.principal)) {
                    return false;
                };
                
                // Check if session is expired
                if (Time.now() > session.expires) {
                    activeSessions.delete(sessionId);
                    return false;
                };
                
                true
            };
            case (null) {
                false
            };
        };
    };
    
    /// Get permissions for a database
    public query(msg) func getDatabasePermissions(
        dbId : DatabaseId
    ) : async [Text] {
        // Get policies for this database
        let policies = Option.get(accessPolicies.get(dbId), []);
        
        // Get permissions for caller
        let userPermissions = Array.mapFilter<AccessPolicy, Text>(
            policies,
            func(policy) {
                if (Principal.equal(policy.principal, msg.caller)) {
                    ?Auth.permissionToText(Option.get(Array.get(policy.permissions, 0), #Read))
                } else {
                    null
                }
            }
        );
        
        userPermissions
    };
    
    /// Grant database permissions to a principal
    public shared(msg) func grantDatabasePermission(
        dbId : DatabaseId,
        principal : Principal,
        permission : Text
    ) : async Result.Result<(), Text> {
        // Check if caller has admin permission on this database
        let policies = Option.get(accessPolicies.get(dbId), []);
        
        let hasAdmin = Array.some<AccessPolicy>(
            policies,
            func(policy) {
                Principal.equal(policy.principal, msg.caller) and
                Array.some<Auth.Permission>(
                    policy.permissions,
                    func(p) { p == #Admin }
                )
            }
        );
        
        if (not hasAdmin) {
            return #err("Caller does not have admin permission on this database");
        };
        
        // Parse permission text
        var permissionType : ?Auth.Permission = null;
        
        switch (Text.toUppercase(permission)) {
            case ("READ") { permissionType := ?#Read };
            case ("WRITE") { permissionType := ?#Write };
            case ("ADMIN") { permissionType := ?#Admin };
            case ("SCHEMA") { permissionType := ?#Schema };
            case (_) { return #err("Invalid permission type: " # permission) };
        };
        
        // Create new policy
        let newPolicy : AccessPolicy = {
            principal = principal;
            resource = {
                resourceType = #Database;
                resourceName = dbId;
            };
            permissions = Option.get(Array.get([permissionType], 0), [#Read]);
            conditions = null;
        };
        
        // Add to existing policies
        let updatedPolicies = Array.append(policies, [newPolicy]);
        accessPolicies.put(dbId, updatedPolicies);
        
        #ok()
    };
    
    // ===== Schema Migration =====
    
    /// Alter a table in the current database
    public shared(msg) func alterTable(
        connectionId : ConnectionId,
        tableName : Text,
        alterations : [{
            #AddColumn : CommonTypes.ColumnDefinition;
            #DropColumn : Text;
            #ModifyColumn : CommonTypes.ColumnDefinition;
            #AddIndex : CommonTypes.IndexDefinition;
            #DropIndex : Text;
        }]
    ) : async TableResult<()> {
        // Validate connection
        if (not validateConnection(connectionId)) {
            return #err(#InvalidTableDefinition("Invalid or expired connection"));
        };
        
        // Get current database
        let dbIdOpt = getCurrentDatabase(connectionId);
        
        switch (dbIdOpt) {
            case (?dbId) {
                // Get current schema
                let registry = getRegistryActor();
                let dbResult = await registry.getDatabase(dbId);
                
                switch (dbResult) {
                    case (#ok(database)) {
                        // Find the table to alter
                        let tableIndex = Array.indexOf<TableSchema>(
                            database.schema.tables,
                            {
                                name = tableName;
                                columns = [];
                                primaryKey = [];
                                indexes = [];
                                foreignKeys = [];
                            },
                            func(a, b) { a.name == b.name }
                        );
                        
                        switch (tableIndex) {
                            case (?idx) {
                                let table = database.schema.tables[idx];
                                var updatedTable = table;
                                
                                // Apply alterations
                                for (alteration in alterations.vals()) {
                                    switch (alteration) {
                                        case (#AddColumn(column)) {
                                            updatedTable := {
                                                name = updatedTable.name;
                                                columns = Array.append(updatedTable.columns, [column]);
                                                primaryKey = updatedTable.primaryKey;
                                                indexes = updatedTable.indexes;
                                                foreignKeys = updatedTable.foreignKeys;
                                            };
                                        };
                                        case (#DropColumn(columnName)) {
                                            updatedTable := {
                                                name = updatedTable.name;
                                                columns = Array.filter<CommonTypes.ColumnDefinition>(
                                                    updatedTable.columns,
                                                    func(col) { col.name != columnName }
                                                );
                                                primaryKey = Array.filter<Text>(
                                                    updatedTable.primaryKey,
                                                    func(pk) { pk != columnName }
                                                );
                                                indexes = updatedTable.indexes;
                                                foreignKeys = updatedTable.foreignKeys;
                                            };
                                        };
                                        case (#ModifyColumn(column)) {
                                            let colIdx = Array.indexOf<CommonTypes.ColumnDefinition>(
                                                updatedTable.columns,
                                                {
                                                    name = column.name;
                                                    dataType = #Null;
                                                    nullable = true;
                                                    defaultValue = null;
                                                    constraints = [];
                                                },
                                                func(a, b) { a.name == b.name }
                                            );
                                            
                                            switch (colIdx) {
                                                case (?cIdx) {
                                                    let updatedColumns = Array.tabulate<CommonTypes.ColumnDefinition>(
                                                        updatedTable.columns.size(),
                                                        func(i) {
                                                            if (i == cIdx) { column } else { updatedTable.columns[i] }
                                                        }
                                                    );
                                                    
                                                    updatedTable := {
                                                        name = updatedTable.name;
                                                        columns = updatedColumns;
                                                        primaryKey = updatedTable.primaryKey;
                                                        indexes = updatedTable.indexes;
                                                        foreignKeys = updatedTable.foreignKeys;
                                                    };
                                                };
                                                case (null) {
                                                    return #err(#ColumnNotFound("Column not found: " # column.name));
                                                };
                                            };
                                        };
                                        case (#AddIndex(index)) {
                                            updatedTable := {
                                                name = updatedTable.name;
                                                columns = updatedTable.columns;
                                                primaryKey = updatedTable.primaryKey;
                                                indexes = Array.append(updatedTable.indexes, [index]);
                                                foreignKeys = updatedTable.foreignKeys;
                                            };
                                        };
                                        case (#DropIndex(indexName)) {
                                            updatedTable := {
                                                name = updatedTable.name;
                                                columns = updatedTable.columns;
                                                primaryKey = updatedTable.primaryKey;
                                                indexes = Array.filter<CommonTypes.IndexDefinition>(
                                                    updatedTable.indexes,
                                                    func(idx) { idx.name != indexName }
                                                );
                                                foreignKeys = updatedTable.foreignKeys;
                                            };
                                        };
                                    };
                                };
                                
                                // Create updated schema
                                let updatedTables = Array.tabulate<TableSchema>(
                                    database.schema.tables.size(),
                                    func(i) {
                                        if (i == idx) { updatedTable } else { database.schema.tables[i] }
                                    }
                                );
                                
                                let updatedSchema : DatabaseSchema = {
                                    name = database.schema.name;
                                    tables = updatedTables;
                                };
                                
                                // Update the database schema
                                let updateResult = await registry.updateDatabaseSchema(dbId, updatedSchema);
                                
                                switch (updateResult) {
                                    case (#ok()) {
                                        #ok()
                                    };
                                    case (#err(e)) {
                                        #err(#AlterationFailed("Failed to update schema: " # debug_show(e)))
                                    };
                                };
                            };
                            case (null) {
                                #err(#TableNotFound("Table not found: " # tableName))
                            };
                        };
                    };
                    case (#err(e)) {
                        #err(#TableNotFound("Database error: " # debug_show(e)))
                    };
                };
            };
            case (null) {
                #err(#TableNotFound("No database selected"))
            };
        };
    };
    
    // ===== System Management =====
    
    /// Get system status
    public query func getStatus() : async {
        initialized : Bool;
        connectionCount : Nat;
        activeSessions : Nat;
    } {
        {
            initialized = registryCanisterId != null and queryCoordinatorCanisterId != null;
            connectionCount = connections.size();
            activeSessions = activeSessions.size();
        }
    };
    
    /// Get canister status
    public shared(msg) func getCanisterStatus() : async {
        cycles : Nat;
        memory : Nat;
        version : Text;
    } {
        {
            cycles = IC.canisterBalance();
            memory = 0; // In a real implementation, we'd track this
            version = "0.1.0"; // Version of the DEDaS implementation
        }
    };
    
    /// Create the necessary canisters for a new database instance
    public shared(msg) func createDatabaseCanisters() : async Result.Result<{
        registry : Principal;
        queryCoordinator : Principal;
        shards : [Principal];
    }, Text> {
        // Check caller's permissions
        // In a real implementation, we'd have proper authorization
        
        // This is a placeholder that would create necessary canisters:
        // - Registry Canister
        // - Query Coordinator Canister
        // - Initial Shard Canisters
        
        #err("Canister creation not implemented in this version")
    };
    
    /// Deploy code to system canisters
    public shared(msg) func deployCanisterCode(
        canisterId : Principal,
        wasmModule : Blob,
        canisterType : {#Registry; #QueryCoordinator; #Shard}
    ) : async Result.Result<(), Text> {
        // Check caller's permissions
        // In a real implementation, we'd have proper authorization
        
        // This is a placeholder that would install/upgrade code on canisters
        #err("Code deployment not implemented in this version")
    };
}