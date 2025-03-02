# Decentralized Elastic Database Service (DEDaS)

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
   - [Shard Management](#shard-management)
   - [Query Engine](#query-engine)
   - [Schema Management](#schema-management)
   - [Consistency Model](#consistency-model)
4. [API Reference](#api-reference)
   - [Database Operations](#database-operations)
   - [Query Language](#query-language)
   - [Administration](#administration)
5. [Implementation Guide](#implementation-guide)
   - [Canister Structure](#canister-structure)
   - [Data Types and Schemas](#data-types-and-schemas)
   - [Inter-Canister Communication](#inter-canister-communication)
6. [Performance Optimization](#performance-optimization)
   - [Query Planning](#query-planning)
   - [Indexing Strategies](#indexing-strategies)
   - [Caching Layer](#caching-layer)
7. [Deployment Patterns](#deployment-patterns)
   - [Single Application](#single-application)
   - [Multi-Tenant Service](#multi-tenant-service)
8. [Advanced Features](#advanced-features)
   - [Geospatial Sharding](#geospatial-sharding)
   - [Analytics Support](#analytics-support)
   - [Time-Series Optimization](#time-series-optimization)
9. [Security Considerations](#security-considerations)
   - [Access Control](#access-control)
   - [Data Encryption](#data-encryption)
   - [Audit Logging](#audit-logging)
10. [Ecosystem Integration](#ecosystem-integration)
    - [Frontend Libraries](#frontend-libraries)
    - [ORM Support](#orm-support)
11. [Troubleshooting](#troubleshooting)
12. [Roadmap and Future Development](#roadmap-and-future-development)
13. [Contributing](#contributing)

## Introduction

The Decentralized Elastic Database Service (DEDaS) is a scalable, on-chain database engine built specifically for the Internet Computer (IC) platform using Motoko. DEDaS addresses one of the most significant challenges in decentralized application development: efficient storage and retrieval of large datasets without compromising on the security and decentralization benefits of the blockchain.

### Key Features

- **Fully On-Chain**: All data and operations remain on the Internet Computer blockchain
- **Dynamic Sharding**: Automatic data partitioning across multiple canisters for horizontal scalability
- **SQL-like Query Interface**: Familiar query syntax for developers
- **ACID Compliance**: Transactional integrity for data operations
- **Auto-Scaling**: Intelligent resource management that responds to usage patterns
- **Cross-Shard Queries**: Ability to execute queries that span multiple data partitions

### Use Cases

- **DeFi Applications**: Store transaction histories, user balances, and market data
- **Social Networks**: Manage user profiles, content, and interaction data
- **Gaming**: Track game states, player inventories, and leaderboards
- **DAOs**: Store governance proposals, voting records, and treasury management
- **Enterprise dApps**: Manage business logic and data for decentralized organizations

## Architecture Overview

DEDaS employs a distributed architecture composed of several specialized canister types working together:

![DEDaS Architecture](https://placeholder-image.com/dedas-architecture.png)

The system comprises:

1. **Registry Canister**: Central coordinator that maintains metadata about shards, schemas, and routing information
2. **Query Coordinator**: Handles incoming queries, determines execution plans, and aggregates results
3. **Shard Canisters**: Store actual data partitions and execute portion of queries relevant to their data
4. **Index Canisters**: Maintain specialized data structures for accelerating queries
5. **Monitoring Canister**: Collects metrics and performance data for the entire system

Data flows through the system as follows:

1. Applications submit queries through the public API
2. The Query Coordinator parses and optimizes the query
3. The query is routed to relevant shards based on data distribution
4. Shards execute their portion of the query and return results
5. Results are aggregated and returned to the client

## Core Components

### Shard Management

Shards are individual canisters that store partitions of data. The sharding strategy is a critical aspect of DEDaS that directly impacts performance and resource utilization.

#### Sharding Strategies

- **Hash-Based Sharding**: Distributes data based on the hash of a primary key
- **Range-Based Sharding**: Partitions data based on value ranges of a specified column
- **Directory-Based Sharding**: Uses a mapping table to track which data is stored in which shard
- **Geo-Based Sharding**: Distributes data based on geographic relevance (requires subnet awareness)

#### Shard Lifecycle

```
┌────────────┐      ┌────────────┐      ┌────────────┐
│            │      │            │      │            │
│ Provision  ├─────►│  Active    ├─────►│  Archive   │
│            │      │            │      │            │
└────────────┘      └──────┬─────┘      └────────────┘
                           │
                           ▼
                    ┌────────────┐
                    │            │
                    │  Rebalance │
                    │            │
                    └────────────┘
```

- **Provisioning**: Creating new shards when needed
- **Activation**: Making shards available for read/write operations
- **Rebalancing**: Redistributing data across shards for optimal performance
- **Archiving**: Moving cold data to archival shards with different optimization parameters

#### Auto-Scaling Logic

Shards are created, merged, or scaled based on several metrics:

- Data volume per shard (approaching memory limits)
- Query latency (exceeding threshold)
- Query frequency (hot vs. cold data)
- Write frequency (update-intensive vs. read-intensive)

```motoko
// Example shard scaling decision logic
public func evaluateShardScaling(shardId: ShardId) : async ScalingAction {
  let metrics = await getShardMetrics(shardId);
  
  if (metrics.dataSize > MAX_SHARD_SIZE * 0.8) {
    return #Split(determineOptimalSplitKey(shardId));
  };
  
  if (metrics.avgQueryLatency > LATENCY_THRESHOLD && 
      metrics.queryCount > HIGH_QUERY_THRESHOLD) {
    return #Replicate;
  };
  
  if (metrics.dataSize < MAX_SHARD_SIZE * 0.2 && 
      shardCount > MIN_SHARD_COUNT) {
    return #Merge(findMergeCandidate(shardId));
  };
  
  return #NoAction;
};
```

### Query Engine

The query engine is responsible for parsing, planning, optimizing, and executing queries across distributed shards.

#### Query Processing Pipeline

1. **Parsing**: Convert query string into an abstract syntax tree (AST)
2. **Validation**: Check query against schema and access controls
3. **Planning**: Determine which shards to involve and execution strategy
4. **Optimization**: Rewrite query for efficient execution
5. **Execution**: Run query components on relevant shards
6. **Aggregation**: Combine results from multiple shards when needed
7. **Response**: Format and return results to the client

#### Query Types

- **Point Queries**: Direct lookup by primary key
- **Range Queries**: Selection based on value ranges
- **Join Queries**: Combining data from multiple tables
- **Aggregate Queries**: Computing summaries (COUNT, SUM, AVG, etc.)
- **Update Operations**: INSERT, UPDATE, DELETE
- **Schema Operations**: CREATE, ALTER, DROP

#### Distributed Query Execution

For queries spanning multiple shards:

1. The Query Coordinator breaks down the query into sub-queries
2. Each sub-query is sent to relevant shards
3. Shards execute their portion and return intermediate results
4. The Coordinator performs necessary joins, aggregations, or sorting
5. Final results are assembled and returned

```motoko
// Example of a distributed query execution
public func executeDistributedQuery(query: Query) : async Result<ResultSet, QueryError> {
  try {
    // Parse and validate
    let ast = parseQuery(query);
    validateQuery(ast);
    
    // Create execution plan
    let executionPlan = createQueryPlan(ast);
    
    // Identify target shards
    let targetShards = identifyTargetShards(executionPlan);
    
    // Execute on each shard
    let intermediateResults = await* Array.mapAsync(
      targetShards, 
      func(shardId: ShardId) : async IntermediateResult {
        await executeOnShard(shardId, executionPlan.forShard(shardId))
      }
    );
    
    // Merge results according to plan
    let finalResult = mergeSuery(executionPlan, intermediateResults);
    
    return #ok(finalResult);
  } catch (e) {
    return #err(#QueryExecutionError(e));
  }
};
```

### Schema Management

DEDaS provides a robust schema management system that supports:

- Table definitions with typed columns
- Primary and secondary indexes
- Foreign key relationships
- Constraints (unique, not null, check)
- Schema evolution and migrations

#### Schema Definition

Schemas are defined using a DDL-like syntax:

```sql
CREATE TABLE users (
  user_id: Text PRIMARY KEY,
  username: Text NOT NULL UNIQUE,
  email: Text,
  created_at: Timestamp,
  profile: { 
    bio: Text,
    avatar_url: Text
  }
);

CREATE INDEX users_by_created ON users (created_at);
```

Internally, schemas are represented as Motoko types:

```motoko
type UserSchema = {
  user_id: Text;
  username: Text;
  email: ?Text;
  created_at: Int;
  profile: {
    bio: ?Text;
    avatar_url: ?Text;
  };
};

type IndexDefinition = {
  name: Text;
  table: Text;
  columns: [Text];
  unique: Bool;
};
```

#### Schema Distribution

Schema information is stored in the Registry Canister and replicated to all relevant shards. When schema changes occur:

1. The change is validated against existing data
2. It's recorded in the Registry Canister
3. Updates are propagated to all affected shards
4. Existing data is migrated if necessary

#### Migrations

For schema changes that require data transformation:

1. A migration plan is created specifying the changes
2. The plan is executed in phases to minimize disruption
3. A rollback plan is prepared in case of failures
4. Once complete, the new schema becomes the active version

### Consistency Model

DEDaS implements a hybrid consistency model to balance performance and correctness:

#### Transaction Types

- **Single-Shard Transactions**: Provide strong consistency (atomic)
- **Cross-Shard Transactions**: Use a two-phase commit protocol for consistency
- **Analytics Queries**: Can opt for eventual consistency for better performance

#### Isolation Levels

- **Read Uncommitted**: Lowest isolation, highest performance
- **Read Committed**: Prevents dirty reads
- **Repeatable Read**: Prevents non-repeatable reads
- **Serializable**: Highest isolation, may impact performance

#### Concurrency Control

- **Optimistic Concurrency Control (OCC)**: For read-heavy workloads
- **Multi-Version Concurrency Control (MVCC)**: For mixed workloads
- **Timestamp-based Ordering**: For globally ordered operations

```motoko
// Example transaction execution with two-phase commit
public func executeCrosssardTransaction(transaction: Transaction) : async Result<TransactionReceipt, TransactionError> {
  // Prepare phase
  let prepareResults = await* Array.mapAsync(
    transaction.involvedShards, 
    func(shardId: ShardId) : async PrepareResult {
      await shardCanisterActor(shardId).prepare(transaction.id, transaction.operations)
    }
  );
  
  // Check if all prepared successfully
  if (Array.all(prepareResults, func(r) { r == #Ready })) {
    // Commit phase
    await* Array.mapAsync(
      transaction.involvedShards,
      func(shardId: ShardId) : async () {
        await shardCanisterActor(shardId).commit(transaction.id)
      }
    );
    return #ok({ transactionId = transaction.id, status = #Committed });
  } else {
    // Abort if any failed
    await* Array.mapAsync(
      transaction.involvedShards,
      func(shardId: ShardId) : async () {
        await shardCanisterActor(shardId).abort(transaction.id)
      }
    );
    return #err(#PreparationFailed);
  };
};
```

## API Reference

### Database Operations

#### Connection Management

```motoko
// Initialize a connection to the DEDaS service
public func connect(config: ConnectionConfig) : async ConnectionResult;

// Close a connection
public func disconnect(connectionId: ConnectionId) : async ();
```

#### Database Administration

```motoko
// Create a new database
public func createDatabase(name: Text) : async Result<DatabaseId, DatabaseError>;

// Drop an existing database
public func dropDatabase(id: DatabaseId) : async Result<(), DatabaseError>;

// List available databases
public func listDatabases() : async [DatabaseInfo];
```

#### Table Operations

```motoko
// Create a new table
public func createTable(dbId: DatabaseId, schema: TableSchema) : async Result<(), TableError>;

// Modify an existing table
public func alterTable(dbId: DatabaseId, tableName: Text, alterations: [TableAlteration]) : async Result<(), TableError>;

// Remove a table
public func dropTable(dbId: DatabaseId, tableName: Text) : async Result<(), TableError>;

// List tables in a database
public func listTables(dbId: DatabaseId) : async Result<[TableInfo], DatabaseError>;
```

#### Data Manipulation

```motoko
// Insert records
public func insert(dbId: DatabaseId, tableName: Text, records: [Record]) : async Result<InsertResponse, QueryError>;

// Update records
public func update(dbId: DatabaseId, tableName: Text, filter: Filter, updates: [Update]) : async Result<UpdateResponse, QueryError>;

// Delete records
public func delete(dbId: DatabaseId, tableName: Text, filter: Filter) : async Result<DeleteResponse, QueryError>;

// Query records with optional filtering and projection
public func query(dbId: DatabaseId, query: QueryStatement) : async Result<QueryResponse, QueryError>;
```

### Query Language

DEDaS supports a SQL-like query language that is transpiled to internal query representations.

#### Basic Queries

```sql
-- Simple selection
SELECT * FROM users WHERE age >= 18;

-- Projection
SELECT username, email FROM users;

-- Aggregation
SELECT count(*) FROM posts WHERE user_id = "user123";
```

#### Joins

```sql
-- Inner join
SELECT u.username, p.title 
FROM users u 
JOIN posts p ON u.user_id = p.author_id
WHERE p.created_at > 1634567890;
```

#### Nested Queries

```sql
-- Query with subquery
SELECT * FROM users 
WHERE user_id IN (
  SELECT author_id FROM posts 
  WHERE likes > 100
);
```

#### Advanced Features

```sql
-- Pagination
SELECT * FROM transactions 
ORDER BY timestamp DESC 
LIMIT 10 OFFSET 20;

-- Pattern matching
SELECT * FROM products 
WHERE name LIKE "%phone%";
```

### Administration

```motoko
// Get system statistics
public func getStats() : async SystemStats;

// Get shard information
public func getShardInfo(shardId: ?ShardId) : async [ShardInfo];

// Force rebalancing
public func rebalanceShards(strategy: RebalanceStrategy) : async Result<(), AdminError>;

// Set configuration parameters
public func configure(params: ConfigParams) : async Result<(), AdminError>;
```

## Implementation Guide

### Canister Structure

DEDaS is composed of several canister types, each with specific responsibilities:

#### Registry Canister

```motoko
actor Registry {
  // Schema and metadata storage
  private var schemas : HashMap<DatabaseId, DatabaseSchema>;
  private var shardRegistry : HashMap<ShardId, ShardInfo>;
  
  // Schema management
  public func registerSchema(db: DatabaseId, schema: DatabaseSchema) : async Result<(), RegistryError>;
  public func getSchema(db: DatabaseId) : async Result<DatabaseSchema, RegistryError>;
  
  // Shard management
  public func registerShard(info: ShardInfo) : async ShardId;
  public func updateShardInfo(id: ShardId, info: ShardInfo) : async Result<(), RegistryError>;
  public func getShardInfo(id: ?ShardId) : async [ShardInfo];
  
  // Routing information
  public func getRouting(db: DatabaseId, table: Text, key: Key) : async [ShardId];
  
  // System operations
  public func initiateRebalance(strategy: RebalanceStrategy) : async Result<(), RegistryError>;
}
```

#### Query Coordinator Canister

```motoko
actor QueryCoordinator {
  // Query processing
  public func executeQuery(db: DatabaseId, query: Query) : async Result<ResultSet, QueryError>;
  
  // Transaction management
  public func beginTransaction(db: DatabaseId) : async Result<TransactionId, TransactionError>;
  public func commit(txId: TransactionId) : async Result<(), TransactionError>;
  public func rollback(txId: TransactionId) : async Result<(), TransactionError>;
  
  // Internal query planning
  private func createExecutionPlan(query: Query) : ExecutionPlan;
  private func optimizeQuery(ast: QueryAST) : QueryAST;
  private func routeQuery(plan: ExecutionPlan) : ShardQueryMap;
}
```

#### Shard Canister

```motoko
actor Shard {
  // Data storage
  private var data : HashMap<Text, Blob>;
  private var indexes : HashMap<Text, Index>;
  
  // Schema information
  private var schema : DatabaseSchema;
  
  // Query execution
  public func executeQuery(query: ShardQuery) : async Result<ShardResult, ShardError>;
  
  // Transaction support
  public func prepare(txId: TransactionId, operations: [Operation]) : async PrepareResult;
  public func commit(txId: TransactionId) : async CommitResult;
  public func abort(txId: TransactionId) : async ();
  
  // Maintenance operations
  public func compact() : async ();
  public func splitShard(splitKey: Key) : async Result<ShardId, ShardError>;
  public func mergeShard(targetShardId: ShardId) : async Result<(), ShardError>;
}
```

#### Index Canister

```motoko
actor Index {
  // Index storage
  private var indexData : TrieMap<Key, [ShardId]>;
  
  // Index definition
  private var definition : IndexDefinition;
  
  // Index operations
  public func lookupKey(key: Key) : async [ShardId];
  public func lookupRange(start: ?Key, end: ?Key) : async [(Key, [ShardId])];
  
  // Maintenance
  public func rebuild() : async ();
  public func updateEntry(key: Key, shards: [ShardId]) : async ();
}
```

### Data Types and Schemas

#### Basic Data Types

```motoko
// Primitive types
type Int = Int;
type Float = Float;
type Text = Text;
type Bool = Bool;
type Blob = Blob;
type Null = Null;

// Complex types
type Array<T> = [T];
type Optional<T> = ?T;
type Record = {[Text]: Value};

// Value can be any of the above types
type Value = {
  #Int: Int;
  #Float: Float;
  #Text: Text;
  #Bool: Bool;
  #Blob: Blob;
  #Null;
  #Array: [Value];
  #Record: {[Text]: Value};
};
```

#### Schema Definition

```motoko
type ColumnDefinition = {
  name: Text;
  dataType: DataType;
  nullable: Bool;
  defaultValue: ?Value;
  constraints: [Constraint];
};

type TableSchema = {
  name: Text;
  columns: [ColumnDefinition];
  primaryKey: [Text];
  indexes: [IndexDefinition];
  foreignKeys: [ForeignKeyDefinition];
};

type DatabaseSchema = {
  name: Text;
  tables: [TableSchema];
};

type Constraint = {
  #PrimaryKey;
  #Unique;
  #NotNull;
  #ForeignKey: ForeignKeyDefinition;
  #Check: Text; // Predicate expression
};
```

#### Query Representation

```motoko
type QueryAST = {
  #Select: SelectQuery;
  #Insert: InsertQuery;
  #Update: UpdateQuery;
  #Delete: DeleteQuery;
  #Create: CreateQuery;
  #Alter: AlterQuery;
  #Drop: DropQuery;
};

type SelectQuery = {
  tables: [TableReference];
  projection: [ProjectionElement];
  filter: ?FilterExpression;
  groupBy: [Text];
  having: ?FilterExpression;
  orderBy: [OrderElement];
  limit: ?Nat;
  offset: ?Nat;
};

// Additional query structure types...
```

### Inter-Canister Communication

DEDaS relies heavily on inter-canister communication for distributed query execution:

#### Actor References

```motoko
// Get actor references for remote canister calls
func getRegistryActor() : Registry {
  actor(Principal.toText(registryCanisterId)) : Registry
};

func getShardActor(shardId: ShardId) : Shard {
  let shardPrincipal = shardIdToPrincipal(shardId);
  actor(Principal.toText(shardPrincipal)) : Shard
};
```

#### Asynchronous Processing

```motoko
// Execute query across multiple shards
public func executeDistributedQuery(query: Query) : async Result<ResultSet, QueryError> {
  try {
    // Identify target shards
    let shardIds = await getRegistry().getShardIdsForQuery(query);
    
    // Execute in parallel
    let results = await* Array.mapAsync<ShardId, Result<ShardResult, ShardError>>(
      shardIds,
      func(shardId: ShardId) : async Result<ShardResult, ShardError> {
        let shardActor = getShardActor(shardId);
        await shardActor.executeQuery(queryForShard(query, shardId))
      }
    );
    
    // Aggregate results
    return aggregateResults(results);
  } catch (e) {
    return #err(#ExecutionError(Error.message(e)));
  }
};
```

#### Error Handling

```motoko
// Handle distributed transaction errors
func handleTransactionError(results: [Result<(), TransactionError>]) : Result<(), TransactionError> {
  // Count errors
  let errors = Array.filter<Result<(), TransactionError>, Bool>(
    results,
    func(r) { _ = r; false }
  );
  
  if (errors.size() > 0) {
    // Attempt recovery
    if (canRecover(errors)) {
      return recoverTransaction(errors);
    };
    
    // Return first error
    for (result in errors.vals()) {
      switch (result) {
        case (#err(e)) { return #err(e) };
        case (_) {};
      };
    };
  };
  
  return #ok();
};
```

## Performance Optimization

### Query Planning

The query planner is responsible for determining the most efficient way to execute a query:

#### Planning Process

1. **Parse the query** into an abstract syntax tree (AST)
2. **Analyze data distribution** across shards
3. **Determine access patterns** (sequential scan, index lookup, etc.)
4. **Estimate costs** of different execution strategies
5. **Select execution plan** with the lowest estimated cost

#### Plan Types

- **Single-Shard Plan**: Execute query on one shard only
- **Broadcast Plan**: Execute same query on multiple shards
- **Scatter-Gather Plan**: Distribute query and aggregate results
- **Pipeline Plan**: Execute stages of the query in sequence

```motoko
// Example query plan for a join operation
type JoinPlan = {
  leftTable: TableAccess;
  rightTable: TableAccess;
  joinType: JoinType;
  joinCondition: FilterExpression;
  postJoinOperations: [Operation];
};

type TableAccess = {
  #DirectAccess: {
    shardId: ShardId;
    accessMethod: AccessMethod;
  };
  #DistributedAccess: {
    shardIds: [ShardId];
    accessMethod: AccessMethod;
    aggregateMethod: AggregateMethod;
  };
};

type AccessMethod = {
  #FullScan;
  #IndexScan: { indexName: Text };
  #PointLookup: { key: Value };
};
```

### Indexing Strategies

Indices are crucial for query performance, especially in a distributed environment:

#### Index Types

- **Primary Index**: Maps primary keys to shard locations
- **Secondary Index**: Improves query performance for non-key columns
- **Composite Index**: Covers multiple columns for complex queries
- **Global Index**: Spans multiple shards for cross-shard queries
- **Local Index**: Maintained within a single shard for local operations

#### Index Implementation

Indices are implemented using optimized data structures:

- **B-Trees**: For ordered range scans
- **Hash Tables**: For equality lookups
- **Inverted Indices**: For text search
- **R-Trees**: For spatial data

```motoko
// Index type definitions
type IndexType = {
  #BTree;
  #Hash;
  #Inverted;
  #Spatial;
};

type IndexDefinition = {
  name: Text;
  table: Text;
  columns: [Text];
  unique: Bool;
  indexType: IndexType;
};

// Example index operation
func createIndex(db: DatabaseId, indexDef: IndexDefinition) : async Result<(), IndexError> {
  // Register index in schema
  let result = await getRegistry().registerIndex(db, indexDef);
  
  // Create index on relevant shards
  let shardIds = await getRegistry().getShardsForTable(db, indexDef.table);
  
  // Kick off index building
  await* Array.mapAsync<ShardId, Result<(), IndexError>>(
    shardIds,
    func(shardId: ShardId) : async Result<(), IndexError> {
      let shardActor = getShardActor(shardId);
      await shardActor.buildIndex(indexDef)
    }
  );
  
  return #ok();
};
```

### Caching Layer

DEDaS incorporates multiple caching strategies to improve performance:

#### Cache Types

- **Schema Cache**: Caches table and index definitions
- **Query Result Cache**: Caches results of frequent queries
- **Index Cache**: Caches frequently accessed index pages
- **Routing Cache**: Caches shard location information

#### Invalidation Strategies

- **Time-Based**: Expires cache entries after a set period
- **Write-Through**: Updates cache when data changes
- **Lazy Invalidation**: Marks entries as stale and updates on next access

```motoko
// Result cache implementation
type CacheEntry<K, V> = {
  key: K;
  value: V;
  timestamp: Int;
  accessCount: Nat;
};

class ResultCache<K, V> {
  private let maxSize: Nat;
  private let ttl: Nat; // Time-to-live in nanoseconds
  private var entries: HashMap<K, CacheEntry<K, V>>;
  
  public func get(key: K) : ?V {
    switch (entries.get(key)) {
      case (?entry) {
        // Check if entry is still valid
        if (Time.now() - entry.timestamp > ttl) {
          entries.delete(key);
          return null;
        };
        
        // Update access count and return value
        let updatedEntry = {
          key = entry.key;
          value = entry.value;
          timestamp = entry.timestamp;
          accessCount = entry.accessCount + 1;
        };
        entries.put(key, updatedEntry);
        return ?entry.value;
      };
      case (null) { return null };
    }
  };
  
  public func put(key: K, value: V) : () {
    // Evict if needed
    if (entries.size() >= maxSize) {
      evictLeastUsed();
    };
    
    // Add new entry
    let entry = {
      key = key;
      value = value;
      timestamp = Time.now();
      accessCount = 1;
    };
    entries.put(key, entry);
  };
  
  private func evictLeastUsed() : () {
    // Find and remove least recently used entry
    // Implementation details...
  };
}
```

## Deployment Patterns

### Single Application

For dApps that need a dedicated database:

1. Deploy Registry and Query Coordinator canisters
2. Create initial shard canisters (typically 1-3)
3. Initialize schemas and indices
4. Configure scaling parameters
5. Integrate with application canister using provided API

```motoko
// Example of application integration
actor MyDApp {
  // DEDaS connection
  private let dbConnection : DEDaSConnection = null;
  
  // Initialize database
  public shared(msg) func initialize() : async () {
    let connectionConfig = {
      registryCanisterId = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
      applicationPrincipal = msg.caller;
      databaseName = "myapp_db";
    };
    
    dbConnection := await DEDaS.connect(connectionConfig);
    
    // Create schema
    let userSchema = {
      name = "users";
      columns = [
        { name = "user_id"; dataType = #Text; nullable = false; defaultValue = null; constraints = [#PrimaryKey] },
        { name = "username"; dataType = #Text; nullable = false; defaultValue = null; constraints = [#Unique] },
        // more columns...
      ];
      // more schema details...
    };
    
    await dbConnection.createTable(userSchema);
  };
  
  // Application logic using DEDaS
  public shared(msg) func createUser(username: Text) : async Result<Text, Error> {
    let userId = generateUserId();
    
    let insertResult = await dbConnection.insert(
      "users",
      [{ user_id = userId; username = username }]
    );
    
    switch (insertResult) {
      case (#ok(_)) { return #ok(userId) };
      case (#err(e)) { return #err(#DatabaseError(e)) };
    };
  };
}
```

### Multi-Tenant Service

For providing database-as-a-service to multiple applications:

1. Deploy central tenant management canister
2. For each tenant:
   - Create isolated database resources
   - Manage permissions and rate limits
   - Track usage for billing
3. Provide tenant-specific connection credentials

```motoko
// Multi-tenant management example
actor DEDaSService {
  // Tenant registry
  private var tenants : HashMap<TenantId, TenantInfo>;
  
  // Create a new tenant
  public shared(msg) func createTenant(name: Text, plan: ServicePlan) : async Result<TenantId, ServiceError> {
    let tenantId = generateTenantId();
    
    // Create tenant-specific database resources
    let dbId = await createTenantDatabase(tenantId);
    
    // Generate connection credentials
    let credentials = generateCredentials(tenantId);
    
    // Store tenant information
    let tenantInfo = {
      id = tenantId;
      name = name;
      plan = plan;
      created = Time.now();
      owner = msg.caller;
      databaseId = dbId;
      credentials = credentials;
    };
    
    tenants.put(tenantId, tenantInfo);
    return #ok(tenantId);
  };

// Get connection information for a tenant
public shared(msg) func getConnectionInfo(tenantId: TenantId) : async Result<ConnectionInfo, ServiceError> {
  // Verify the caller is authorized
  if (not isAuthorized(msg.caller, tenantId)) {
    return #err(#NotAuthorized);
  };
  
  switch (tenants.get(tenantId)) {
    case (?tenant) {
      return #ok({
        registryId = tenant.registryId;
        credentials = tenant.credentials;
        endpoints = {
          query = tenant.endpoints.query;
          admin = tenant.endpoints.admin;
        };
      });
    };
    case (null) {
      return #err(#TenantNotFound);
    };
  };
};

// Upgrade tenant plan
public shared(msg) func upgradeTenantPlan(tenantId: TenantId, newPlan: ServicePlan) : async Result<(), ServiceError> {
  // Implementation for upgrading a tenant's service plan
  // Includes updating resource limits, scaling parameters, etc.
  // ...
};

// Monitor tenant usage
public func getTenantMetrics(tenantId: TenantId) : async Result<TenantMetrics, ServiceError> {
  // Collect and return usage metrics
  // ...
};
```

## Security Considerations

Security is a critical aspect of any database system, especially in a decentralized context where data is stored on a public blockchain.

### Access Control

DEDaS implements a layered access control mechanism:

#### Authentication 

```motoko
// Principal-based authentication
public shared(msg) func authenticate() : async Result<Session, AuthError> {
  let caller = msg.caller;
  
  // Check if principal is authorized
  if (not isAuthorizedPrincipal(caller)) {
    return #err(#NotAuthorized("Principal not authorized"));
  };
  
  // Create a new session
  let sessionId = generateSessionId();
  let session = {
    id = sessionId;
    principal = caller;
    created = Time.now();
    expires = Time.now() + SESSION_EXPIRY;
    permissions = getPermissionsForPrincipal(caller);
  };
  
  sessions.put(sessionId, session);
  return #ok(session);
};
```

#### Authorization

DEDaS supports fine-grained, role-based access control:

```motoko
type Permission = {
  #Read;
  #Write;
  #Admin;
  #Schema;
};

type ResourceType = {
  #Database;
  #Table;
  #Row;
  #Column;
};

type Resource = {
  resourceType: ResourceType;
  resourceName: Text;
};

type AccessPolicy = {
  principal: Principal;
  resource: Resource;
  permissions: [Permission];
  conditions: ?FilterExpression;
};
```

Access control is enforced at multiple levels:

1. **Database Level**: Controls who can access the database
2. **Table Level**: Controls who can access specific tables
3. **Row Level**: Controls which rows a user can see or modify
4. **Column Level**: Controls which columns a user can see or modify

```motoko
// Row-level security example
func applyRowLevelSecurity(
  query: Query, 
  principal: Principal
) : Query {
  // Get the security policies for this principal
  let policies = getPoliciesForPrincipal(principal);
  
  // Extract tables referenced in the query
  let referencedTables = getReferencedTables(query);
  
  // Build combined filter from all applicable policies
  var securityFilter : ?FilterExpression = null;
  
  for (table in referencedTables.vals()) {
    let tableCondition = buildTableCondition(table, policies);
    securityFilter := combineFilters(securityFilter, tableCondition);
  };
  
  // Inject security filter into the query
  return injectSecurityFilter(query, securityFilter);
};
```

#### Delegation and Proxy Access

For applications that need to perform operations on behalf of users:

```motoko
// Create a delegated access token
public shared(msg) func createDelegation(
  delegate: Principal,
  permissions: [Permission],
  expiration: Int
) : async Result<DelegationToken, AuthError> {
  // Verify the caller can delegate these permissions
  if (not canDelegate(msg.caller, permissions)) {
    return #err(#InsufficientPermissions);
  };
  
  // Create delegation token
  let token = {
    id = generateTokenId();
    delegator = msg.caller;
    delegate = delegate;
    permissions = permissions;
    created = Time.now();
    expires = Time.now() + expiration;
    signature = signDelegation(msg.caller, delegate, permissions, expiration);
  };
  
  delegations.put(token.id, token);
  return #ok(token);
};
```

### Data Encryption

DEDaS provides encryption capabilities to protect sensitive data:

#### Column-Level Encryption

For sensitive fields that require encryption:

```motoko
type EncryptionConfig = {
  algorithm: EncryptionAlgorithm;
  keyManagement: KeyManagementStrategy;
  accessControl: [Principal];
};

// Define encrypted columns in schema
public func defineEncryptedColumn(
  dbId: DatabaseId,
  tableName: Text,
  columnName: Text,
  config: EncryptionConfig
) : async Result<(), SchemaError> {
  // Implementation for setting up encrypted columns
  // ...
};
```

#### Client-Side Encryption

For end-to-end encryption scenarios:

```javascript
// Client-side example (JavaScript)
async function insertEncryptedRecord(connection, tableName, record) {
  // Generate encryption key if needed
  const encryptionKey = await getOrCreateEncryptionKey();
  
  // Encrypt sensitive fields
  const encryptedRecord = { ...record };
  for (const field of sensitiveFields) {
    if (record[field]) {
      encryptedRecord[field] = await encrypt(record[field], encryptionKey);
    }
  }
  
  // Store encrypted record
  return connection.insert(tableName, encryptedRecord);
}
```

#### Homomorphic Encryption Support

For specialized use cases requiring computation on encrypted data:

```motoko
// Register a homomorphic encryption scheme
public func registerHomomorphicScheme(
  name: Text,
  implementation: HomomorphicSchemeImplementation
) : async Result<(), RegistrationError> {
  // Register the scheme for use in queries
  // ...
};

// Configure a column to use homomorphic encryption
public func configureHomomorphicColumn(
  dbId: DatabaseId,
  tableName: Text,
  columnName: Text,
  schemeName: Text,
  config: HomomorphicConfig
) : async Result<(), ConfigError> {
  // Implementation for setting up homomorphically encrypted columns
  // ...
};
```

### Audit Logging

Comprehensive audit logging is essential for security monitoring and compliance:

```motoko
type AuditLogEntry = {
  timestamp: Int;
  principal: Principal;
  action: AuditAction;
  resource: Resource;
  status: AuditStatus;
  details: Text;
};

type AuditAction = {
  #SchemaModification;
  #DataModification;
  #DataAccess;
  #AuthenticationAttempt;
  #ConfigurationChange;
  #SystemOperation;
};

type AuditStatus = {
  #Success;
  #Failure: Text;
};
```

The audit logging system:

1. Captures all significant actions (schema changes, data modifications, etc.)
2. Records both successful and failed operations
3. Stores logs in tamper-evident structures
4. Provides query capabilities for compliance and forensics

```motoko
// Record an audit log entry
func recordAuditLog(entry: AuditLogEntry) : async () {
  // Store log entry
  await auditLogCanister.addEntry(entry);
  
  // Check for security alerts
  if (shouldTriggerAlert(entry)) {
    await triggerSecurityAlert(entry);
  };
};

// Query audit logs
public shared(msg) func queryAuditLogs(
  filter: AuditLogFilter,
  options: QueryOptions
) : async Result<[AuditLogEntry], QueryError> {
  // Verify the caller has permission to access audit logs
  if (not hasAuditLogAccess(msg.caller)) {
    return #err(#AccessDenied);
  };
  
  // Query logs based on filter
  return await auditLogCanister.queryLogs(filter, options);
};
```

## Ecosystem Integration

DEDaS is designed to integrate seamlessly with the broader Internet Computer ecosystem.

### Frontend Libraries

Client libraries are available for various platforms to simplify DEDaS integration:

#### JavaScript/TypeScript SDK

```typescript
// TypeScript SDK example
import { DEDaSClient, QueryBuilder } from '@dedas/client';

// Initialize client
const client = new DEDaSClient({
  registryCanisterId: 'rrkah-fqaaa-aaaaa-aaaaq-cai',
  credentials: {
    identity: identity, // Internet Identity or other authentication
  },
});

// Connect to database
const db = await client.connectToDatabase('myapp_db');

// Build and execute a query
const usersQuery = new QueryBuilder('users')
  .select(['user_id', 'username', 'email'])
  .where('age > ?', [18])
  .orderBy('created_at', 'DESC')
  .limit(10);

const results = await db.execute(usersQuery);
```

#### Rust Client

```rust
// Rust client example
use dedas_client::{DEDaSClient, QueryBuilder, Identity};

async fn fetch_users() -> Result<Vec<User>, DEDaSError> {
    // Initialize client
    let identity = Identity::from_pem_file("identity.pem")?;
    let client = DEDaSClient::new()
        .with_registry_id("rrkah-fqaaa-aaaaa-aaaaq-cai")
        .with_identity(identity)
        .build()?;
    
    // Connect to database
    let db = client.connect_database("myapp_db").await?;
    
    // Build and execute query
    let users = db.query::<User>()
        .select(["user_id", "username", "email"])
        .where_clause("age > ?", &[18])
        .order_by("created_at", "DESC")
        .limit(10)
        .execute()
        .await?;
    
    Ok(users)
}
```

#### React Integration

```jsx
// React Hook example
import { useDEDaSQuery, DEDaSProvider } from '@dedas/react';

function UserList() {
  const { data, isLoading, error } = useDEDaSQuery(
    'users',
    {
      select: ['user_id', 'username', 'profile.bio'],
      where: { age: { $gt: 18 } },
      orderBy: { created_at: 'DESC' },
      limit: 10
    },
    {
      // Optional configuration
      refetchInterval: 30000,
      queryKey: ['users', 'active'],
    }
  );

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;
  
  return (
    <ul>
      {data.map(user => (
        <li key={user.user_id}>{user.username}</li>
      ))}
    </ul>
  );
}

// Wrap application with provider
function App() {
  return (
    <DEDaSProvider config={{
      registryCanisterId: 'rrkah-fqaaa-aaaaa-aaaaq-cai',
      databaseId: 'myapp_db'
    }}>
      <UserList />
    </DEDaSProvider>
  );
}
```

### ORM Support

DEDaS offers ORM (Object-Relational Mapping) capabilities for more elegant data interactions:

```motoko
// Motoko ORM example
import DEDaS "mo:dedas/orm";

class User {
  public let id: Text;
  public var username: Text;
  public var email: ?Text;
  public var profile: UserProfile;
  
  // Constructor for creating new users
  public func new(username: Text, email: ?Text, profile: UserProfile) : User {
    return {
      id = generateId();
      username = username;
      email = email;
      profile = profile;
    };
  };
}

// Configure ORM
let userModel = DEDaS.createModel<User>("users", {
  primaryKey = #field("id"),
  fields = [
    { name = "id"; type = #Text; nullable = false },
    { name = "username"; type = #Text; nullable = false },
    { name = "email"; type = #Text; nullable = true },
    { name = "profile"; type = #Record(UserProfileSchema); nullable = false }
  ],
  indices = [
    { name = "username_idx"; fields = ["username"]; unique = true }
  ]
});

// Use the ORM
public func createUser(username: Text, email: ?Text) : async Result<User, Error> {
  let user = User.new(
    username,
    email,
    { bio = null; avatar_url = null }
  );
  
  let result = await userModel.save(user);
  switch (result) {
    case (#ok()) { return #ok(user) };
    case (#err(e)) { return #err(e) };
  };
}

public func findUserByUsername(username: Text) : async Result<User, Error> {
  return await userModel.findOne({ username = username });
}
```

## Troubleshooting

### Common Issues and Solutions

#### Query Performance Problems

```
Problem: Queries are taking longer than expected to execute.

Potential causes:
1. Missing or inefficient indexes
2. Data skew causing hot shards
3. Resource constraints on specific canisters
4. Inefficient query patterns

Solutions:
1. Analyze query execution plans
   await queryCoordinator.explainQuery(query);
   
2. Check shard data distribution
   await registry.getShardDistributionStats(dbId, tableName);
   
3. Add appropriate indexes
   await schema.createIndex({ table: "users", columns: ["email"], ...});
   
4. Adjust sharding strategy
   await registry.updateShardingConfig({
     strategy: #RangeSharding,
     keyColumn: "user_id",
     rangePoints: ["A", "F", "M", "S", "Z"]
   });
```

#### Transaction Failures

```
Problem: Distributed transactions are failing intermittently.

Potential causes:
1. Cycle limit exceeded during transaction phases
2. Network partitioning or canister unavailability
3. Deadlocks in concurrent operations
4. Validation or constraint violations

Solutions:
1. Enable transaction logging for debugging
   await config.setTransactionLogging(true);
   
2. Analyze transaction logs
   let logs = await admin.getTransactionLogs(transactionId);
   
3. Optimize transaction scope and size
   - Reduce the number of operations per transaction
   - Split large transactions into smaller units
   
4. Implement retry logic with backoff in client applications
   async function executeWithRetry(operation, maxRetries = 3) {
     for (let i = 0; i < maxRetries; i++) {
       try {
         return await operation();
       } catch (e) {
         if (i === maxRetries - 1 || !isRetryableError(e)) throw e;
         await new Promise(r => setTimeout(r, Math.pow(2, i) * 100));
       }
     }
   }
```

#### Scaling and Resource Management

```
Problem: System fails to scale properly under load.

Potential causes:
1. Inefficient shard management parameters
2. Memory limits reached in canisters
3. Poor data partitioning decisions
4. Hot spots in data access patterns

Solutions:
1. Tune scaling parameters
   await admin.updateScalingParameters({
     maxShardSizeBytes: 1_000_000_000, // 1GB
     shardingThresholdPercent: 75,
     minShardCount: 3,
     autoScalingEnabled: true
   });
   
2. Force rebalancing when needed
   await admin.rebalanceShards({
     strategy: #EvenDistribution,
     priority: #High
   });
   
3. Monitor and adjust
   - Regularly check shard utilization 
   - Identify and fix hot spots
   - Consider data-specific optimizations
```

### Diagnostic Tools

DEDaS provides several diagnostic tools to help identify and resolve issues:

```motoko
// Get detailed system status
public func getSystemDiagnostics() : async SystemDiagnostics {
  // Collect diagnostics from all components
  let registryStatus = await registry.getStatus();
  let shardStatuses = await* Array.mapAsync(
    allShardIds,
    func(id: ShardId) : async ShardStatus {
      try {
        let shard = getShardActor(id);
        await shard.getStatus()
      } catch (e) {
        {
          shardId = id;
          status = #Unreachable;
          error = ?Error.message(e);
          metrics = null;
        }
      }
    }
  );
  
  // Analyze for issues
  let issues = detectSystemIssues(registryStatus, shardStatuses);
  
  return {
    timestamp = Time.now();
    registryStatus = registryStatus;
    shardStatuses = shardStatuses;
    queryCoordinatorStatus = await queryCoordinator.getStatus();
    detectedIssues = issues;
    recommendations = generateRecommendations(issues);
  };
};

// Trace a specific query execution
public func traceQuery(query: Query) : async QueryTrace {
  let queryId = generateQueryId();
  
  // Enable tracing
  await queryCoordinator.enableTracing(queryId);
  
  // Execute query
  let result = await queryCoordinator.executeQuery(query);
  
  // Collect and return trace
  return await queryCoordinator.getQueryTrace(queryId);
};
```

### Error Reference

Common error codes and their resolution:

```
ERR-DB-001: Database not found
- Verify the database ID is correct
- Check that the database has been created

ERR-SCHEMA-002: Table schema violation
- Ensure data matches the defined schema
- Check for missing required fields
- Verify data types are correct

ERR-QUERY-003: Query syntax error
- Check query syntax
- Verify column names and table references
- Ensure operators are used correctly

ERR-SHARD-004: Shard not reachable
- Verify the shard canister is running
- Check for subnet issues
- May require system administrator attention

ERR-AUTH-005: Authorization failed
- Verify credentials
- Check principal has required permissions
- Ensure delegation token is valid and not expired

ERR-TXN-006: Transaction conflict
- Transaction conflicts with concurrent operations
- Retry the transaction
- Consider restructuring to avoid hot spots
```

## Roadmap and Future Development

The DEDaS project is actively being developed with the following roadmap:

### Short-term Goals (Next 3-6 Months)

1. **Performance Optimizations**
   - Reduce inter-canister call overhead
   - Implement adaptive caching strategies
   - Optimize serialization and deserialization

2. **Enhanced Query Capabilities**
   - Advanced analytics functions
   - Window functions and hierarchical queries
   - Full-text search integration

3. **Developer Experience**
   - Improved client libraries
   - Visual query explorer and administration console
   - Enhanced debugging and monitoring tools

4. **Ecosystem Integrations**
   - Direct integration with popular IC frameworks
   - Automated backup and restore capabilities
   - Support for standard data import/export formats

### Medium-term Goals (6-12 Months)

1. **Advanced Database Features**
   - Stored procedures and user-defined functions
   - Event triggers and notifications
   - Materialized views and computed columns

2. **Multi-model Support**
   - Document database capabilities
   - Graph data model and queries
   - Time-series optimizations

3. **Enterprise Features**
   - Multi-region data distribution
   - Enhanced compliance and governance tools
   - SLA monitoring and enforcement

4. **Performance at Scale**
   - Billion-record benchmark suite
   - Adaptations for IC subnet improvements
   - Dynamic optimization based on usage patterns

### Long-term Vision

1. **Autonomous Operation**
   - Self-tuning query optimization
   - Predictive scaling and resource allocation
   - AI-assisted schema design and optimization

2. **Ecosystem Position**
   - Reference implementation for IC database patterns
   - Integration with standardized data protocols
   - Cross-chain data bridging capabilities

3. **Sovereignty and Governance**
   - Community-driven feature prioritization
   - Open governance model for protocol evolution
   - Sustainable economic model for resource allocation

### Experimental Features in Development

The following features are currently in research and experimental phases:

```motoko
// Zero-knowledge proof queries (experimental)
public func zkQuery(
  proofedStatement: ZkQueryStatement,
  proof: ZkProof
) : async Result<Blob, ZkQueryError> {
  // Verify the proof
  if (not verifyZkProof(proofedStatement, proof)) {
    return #err(#InvalidProof);
  };
  
  // Execute the query with privacy guarantees
  // ...
};

// Federated queries across services (experimental)
public func federatedQuery(
  query: FederatedQuery,
  dataSourceRefs: [DataSourceRef]
) : async Result<FederatedResult, FederatedQueryError> {
  // Implementation for cross-service data queries
  // ...
};
```

## Contributing

DEDaS is an open-source project that welcomes contributions from the community.

### Getting Started

To set up your development environment:

1. Fork the DEDaS repository
2. Clone your fork locally
3. Install the development dependencies
4. Set up the local development environment

```bash
# Clone the repository
git clone https://github.com/yourusername/dedas.git
cd dedas

# Install development dependencies
npm install

# Set up local Internet Computer replica
dfx start --background

# Deploy development canisters
dfx deploy
```

### Development Workflow

The recommended development workflow is:

1. Create a new branch for your feature or bugfix
2. Make your changes with appropriate tests
3. Run the test suite locally
4. Submit a pull request for review

```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes and run tests
npm test

# Run integration tests
npm run test:integration

# Submit changes
git push origin feature/my-feature
```

### Code Style and Guidelines

DEDaS follows specific coding standards:

- Use meaningful variable and function names
- Document public functions and types with comments
- Write unit tests for all new functionality
- Follow the Motoko style guide for code formatting
- Keep functions small and focused on a single responsibility

### Documentation

When adding new features, please update the documentation:

- Add inline code comments for complex logic
- Update relevant sections of the documentation
- Provide examples of how to use the new feature
- Consider adding entries to the troubleshooting section

### Review Process

All contributions go through a review process:

1. Automated checks verify formatting, tests, and build
2. Core team members review the code for quality and design
3. Changes may be requested before merging
4. Once approved, changes are merged into the main branch

### Reporting Issues

If you find a bug or have a feature request:

1. Check the existing issues to avoid duplicates
2. Create a new issue with a clear description
3. Include steps to reproduce bugs
4. Label the issue appropriately

### Community

Join the DEDaS community:

- Discord: [DEDaS Developers](https://discord.gg/example)
- Forum: [DEDaS Discussion](https://forum.example.org/dedas)
- Twitter: [@DEDaS_Dev](https://twitter.com/example)

---

This documentation provides comprehensive guidance for deploying, using, and contributing to the Decentralized Elastic Database Service (DEDaS). As the project evolves, this documentation will be updated to reflect the latest features and best practices.

For the latest updates, please refer to the official repository and release notes.