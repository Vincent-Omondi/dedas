/**
 * Shard Canister
 * 
 * The Shard Canister is responsible for storing and manipulating data partitions
 * in the DEDaS system. It receives queries from the Query Coordinator, executes them
 * on its local data, and returns results.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Cycles "mo:base/ExperimentalCycles";
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

import CommonTypes "../common/types";
import Errors "../common/errors";
import ShardTypes "./types";
import CoordinatorTypes "../query_coordinator/types";

actor Shard {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    type ShardId = CommonTypes.ShardId;
    type TransactionId = CommonTypes.TransactionId;
    type Key = CommonTypes.Key;
    type Value = CommonTypes.Value;
    type Record = CommonTypes.Record;
    type TableSchema = CommonTypes.TableSchema;
    type ResultSet = CommonTypes.ResultSet;
    type FilterExpression = CommonTypes.FilterExpression;
    
    type ShardOperation = CoordinatorTypes.ShardOperation;
    type ShardQuery = CoordinatorTypes.ShardQuery;
    type ShardResult = CoordinatorTypes.ShardResult;
    type PrepareResult = CoordinatorTypes.PrepareResult;
    type CommitResult = CoordinatorTypes.CommitResult;
    type TransactionOperation = CoordinatorTypes.TransactionOperation;
    
    type TableStorage = ShardTypes.TableStorage;
    type IndexStorage = ShardTypes.IndexStorage;
    type TransactionState = ShardTypes.TransactionState;
    type ShardInitRequest = ShardTypes.ShardInitRequest;
    type ShardInfo = ShardTypes.ShardInfo;
    type ShardStatus = ShardTypes.ShardStatus;
    type ShardMetrics = ShardTypes.ShardMetrics;
    type FilterResult = ShardTypes.FilterResult;
    type IndexLookupResult = ShardTypes.IndexLookupResult;
    type RowPredicate = ShardTypes.RowPredicate;
    
    type QueryError = Errors.QueryError;
    type QueryResult<T> = Errors.QueryResult<T>;
    type ShardError = Errors.ShardError;
    type ShardResult<T> = Errors.ShardResult<T>;
    type SchemaError = Errors.SchemaError;
    type SchemaResult<T> = Errors.SchemaResult<T>;
    
    // ===== State =====
    
    // Shard identity
    private var shardId : ShardId = "";
    private var databaseId : DatabaseId = "";
    
    // Tables
    private let tables = HashMap.HashMap<Text, TableStorage>(10, Text.equal, Text.hash);
    
    // Transactions
    private let transactions = HashMap.HashMap<TransactionId, TransactionState>(10, Text.equal, Text.hash);
    
    // Statistics
    private var recordCount : Nat = 0;
    private var dataSize : Nat = 0;
    private var queryCount : Nat = 0;
    private var writeCount : Nat = 0;
    private var errorCount : Nat = 0;
    private var totalQueryTime : Nat64 = 0;
    private var totalWriteTime : Nat64 = 0;
    private var creationTime : Time.Time = Time.now();
    private var status : ShardStatus = #Initializing;
    
    // ===== Initialization =====
    
    /// Initialize the shard
    public shared(msg) func initialize(request : ShardInitRequest) : async ShardResult<()> {
        // Verify that the shard is not already initialized
        if (Text.size(shardId) > 0) {
            return #err(#ShardCreationFailed("Shard already initialized"));
        };
        
        // Set shard identity
        shardId := request.shardId;
        databaseId := request.databaseId;
        
        // Initialize tables
        for (tableSchema in request.tables.vals()) {
            let tableStorage : TableStorage = {
                name = tableSchema.name;
                schema = tableSchema;
                var records = [];
                var indexes = [];
            };
            
            tables.put(tableSchema.name, tableStorage);
            
            // Initialize indexes
            for (indexDef in tableSchema.indexes.vals()) {
                let indexStorage : IndexStorage = {
                    name = indexDef.name;
                    indexType = indexDef.indexType;
                    columns = indexDef.columns;
                    unique = indexDef.unique;
                    var entries = [];
                };
                
                // Update table storage with the new index
                let updatedTable = Option.get(tables.get(tableSchema.name), tableStorage);
                updatedTable.indexes := Array.append(updatedTable.indexes, [indexStorage]);
                tables.put(tableSchema.name, updatedTable);
            };
        };
        
        // Update status
        status := #Active;
        
        return #ok();
    };
    
    // ===== Helper Functions =====
    
    /// Get a value from a record by key
    private func getRecordValue(record : Record, key : Text) : ?Value {
        let valueIter = record.vals();
        let keyIter = record.keys();
        
        loop {
            switch (keyIter.next()) {
                case (?k) {
                    let v = Option.get(valueIter.next(), #Null);
                    if (k == key) {
                        return ?v;
                    };
                };
                case (null) return null;
            };
        };
    };
    
    /// Compare values for filtering
    private func compareValues(v1 : Value, v2 : Value) : ?{#LT; #EQ; #GT} {
        switch (v1, v2) {
            case (#Int(i1), #Int(i2)) {
                if (i1 < i2) return ?#LT;
                if (i1 == i2) return ?#EQ;
                if (i1 > i2) return ?#GT;
            };
            case (#Float(f1), #Float(f2)) {
                if (f1 < f2) return ?#LT;
                if (f1 == f2) return ?#EQ;
                if (f1 > f2) return ?#GT;
            };
            case (#Text(t1), #Text(t2)) {
                if (t1 < t2) return ?#LT;
                if (t1 == t2) return ?#EQ;
                if (t1 > t2) return ?#GT;
            };
            case (#Bool(b1), #Bool(b2)) {
                if (not b1 and b2) return ?#LT;
                if (b1 == b2) return ?#EQ;
                if (b1 and not b2) return ?#GT;
            };
            case _ return null; // Cannot compare different types
        };
        
        null
    };
    
    /// Evaluate a filter expression against a record
    private func evaluateFilter(record : Record, filter : FilterExpression) : Bool {
        switch (filter) {
            case (#Equals(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#EQ) true;
                            case _ false;
                        };
                    };
                    case (null) false;
                };
            };
            case (#NotEquals(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#EQ) false;
                            case _ true;
                        };
                    };
                    case (null) true;
                };
            };
            case (#GreaterThan(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#GT) true;
                            case _ false;
                        };
                    };
                    case (null) false;
                };
            };
            case (#LessThan(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#LT) true;
                            case _ false;
                        };
                    };
                    case (null) false;
                };
            };
            case (#GreaterThanOrEqual(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#GT) true;
                            case (?#EQ) true;
                            case _ false;
                        };
                    };
                    case (null) false;
                };
            };
            case (#LessThanOrEqual(column, value)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        switch (compareValues(recordValue, value)) {
                            case (?#LT) true;
                            case (?#EQ) true;
                            case _ false;
                        };
                    };
                    case (null) false;
                };
            };
            case (#Like(column, pattern)) {
                switch (getRecordValue(record, column)) {
                    case (?#Text(text)) {
                        // Simple pattern matching with % wildcard
                        // This is a very simplified implementation
                        if (pattern == "%") return true;
                        
                        let startsWithWildcard = Text.startsWith(pattern, #text "%");
                        let endsWithWildcard = Text.endsWith(pattern, #text "%");
                        
                        if (startsWithWildcard and endsWithWildcard) {
                            let substr = Text.trim(pattern, #text "%");
                            return Text.contains(text, #text substr);
                        } else if (startsWithWildcard) {
                            let suffix = Text.trimStart(pattern, #text "%");
                            return Text.endsWith(text, #text suffix);
                        } else if (endsWithWildcard) {
                            let prefix = Text.trimEnd(pattern, #text "%");
                            return Text.startsWith(text, #text prefix);
                        } else {
                            return text == pattern;
                        };
                    };
                    case _ false;
                };
            };
            case (#Between(column, lower, upper)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        let geqLower = switch (compareValues(recordValue, lower)) {
                            case (?#GT) true;
                            case (?#EQ) true;
                            case _ false;
                        };
                        
                        let leqUpper = switch (compareValues(recordValue, upper)) {
                            case (?#LT) true;
                            case (?#EQ) true;
                            case _ false;
                        };
                        
                        geqLower and leqUpper
                    };
                    case (null) false;
                };
            };
            case (#In(column, values)) {
                switch (getRecordValue(record, column)) {
                    case (?recordValue) {
                        Array.some<Value>(values, func(v) {
                            switch (compareValues(recordValue, v)) {
                                case (?#EQ) true;
                                case _ false;
                            };
                        });
                    };
                    case (null) false;
                };
            };
            case (#IsNull(column)) {
                switch (getRecordValue(record, column)) {
                    case (?#Null) true;
                    case (null) true;
                    case _ false;
                };
            };
            case (#IsNotNull(column)) {
                switch (getRecordValue(record, column)) {
                    case (?#Null) false;
                    case (null) false;
                    case _ true;
                };
            };
            case (#And(conditions)) {
                Array.all<FilterExpression>(conditions, func(c) {
                    evaluateFilter(record, c)
                });
            };
            case (#Or(conditions)) {
                Array.some<FilterExpression>(conditions, func(c) {
                    evaluateFilter(record, c)
                });
            };
            case (#Not(condition)) {
                not evaluateFilter(record, condition);
            };
        };
    };
    
    /// Apply a filter to a table
    private func applyFilter(tableName : Text, filter : ?FilterExpression) : FilterResult {
        switch (tables.get(tableName)) {
            case (?table) {
                switch (filter) {
                    case (null) {
                        // No filter, match all rows
                        let indices = Array.tabulate<Nat>(table.records.size(), func(i) { i });
                        #Match(indices)
                    };
                    case (?f) {
                        // Check if we can use an index for this filter
                        let indexLookup = lookupIndexForFilter(tableName, f);
                        
                        switch (indexLookup) {
                            case (#Match(indices)) {
                                #Match(indices)
                            };
                            case (#NoMatch) {
                                #NoMatch
                            };
                            case (#NotIndexed) {
                                // Fall back to full table scan
                                let matchingIndices = Buffer.Buffer<Nat>(0);
                                
                                for (i in Iter.range(0, table.records.size() - 1)) {
                                    if (evaluateFilter(table.records[i], f)) {
                                        matchingIndices.add(i);
                                    };
                                };
                                
                                if (matchingIndices.size() > 0) {
                                    #Match(Buffer.toArray(matchingIndices))
                                } else {
                                    #NoMatch
                                };
                            };
                            case (#Error(e)) {
                                #Error(e)
                            };
                        };
                    };
                };
            };
            case (null) {
                #Error("Table not found: " # tableName)
            };
        };
    };
    
    /// Lookup an index for a given filter
    private func lookupIndexForFilter(tableName : Text, filter : FilterExpression) : IndexLookupResult {
        switch (tables.get(tableName)) {
            case (?table) {
                switch (filter) {
                    case (#Equals(column, value)) {
                        // Find an index on this column
                        let indexOpt = Array.find<IndexStorage>(table.indexes, func(idx) {
                            idx.columns.size() == 1 and idx.columns[0] == column
                        });
                        
                        switch (indexOpt) {
                            case (?index) {
                                // Find entries for this value
                                let key = value;
                                let entriesOpt = Array.find<(Key, [Nat])>(index.entries, func(entry) {
                                    let (entryKey, _) = entry;
                                    switch (compareValues(entryKey, key)) {
                                        case (?#EQ) true;
                                        case _ false;
                                    };
                                });
                                
                                switch (entriesOpt) {
                                    case (?entry) {
                                        let (_, indices) = entry;
                                        #Match(indices)
                                    };
                                    case (null) {
                                        #NoMatch
                                    };
                                };
                            };
                            case (null) {
                                #NotIndexed
                            };
                        };
                    };
                    // For other filter types, we'd implement similar logic
                    // Here I'm simplifying by just returning #NotIndexed
                    case _ {
                        #NotIndexed
                    };
                };
            };
            case (null) {
                #Error("Table not found: " # tableName)
            };
        };
    };
    
    /// Get the value of a column from a record
    private func getColumn(record : Record, column : Text) : Value {
        switch (getRecordValue(record, column)) {
            case (?value) value;
            case (null) #Null;
        };
    };
    
    /// Project columns from a record
    private func projectColumns(record : Record, columns : [Text]) : [Value] {
        if (columns.size() == 1 and columns[0] == "*") {
            // Return all columns
            let values = Buffer.Buffer<Value>(0);
            for ((_, value) in record.vals()) {
                values.add(value);
            };
            return Buffer.toArray(values);
        } else {
            Array.map<Text, Value>(columns, func(col) {
                getColumn(record, col)
            })
        };
    };
    
    /// Update a record with new values
    private func updateRecord(record : Record, updates : [(Text, Value)]) : Record {
        var updatedRecord = record;
        
        for ((column, value) in updates.vals()) {
            updatedRecord := HashMap.fromIter<Text, Value>(
                Iter.chain(
                    HashMap.fromIter<Text, Value>(
                        updatedRecord.entries(), 
                        10, 
                        Text.equal, 
                        Text.hash
                    ).entries(),
                    Iter.fromArray([(column, value)])
                ), 
                10, 
                Text.equal, 
                Text.hash
            );
        };
        
        updatedRecord
    };
    
    /// Check if a record satisfies unique constraints
    private func checkUniqueConstraints(tableName : Text, record : Record, excludeIndex : ?Nat) : Result.Result<(), Text> {
        switch (tables.get(tableName)) {
            case (?table) {
                // Find unique indexes
                for (index in table.indexes.vals()) {
                    if (index.unique) {
                        // Build the key for this record
                        let keyColumns = Array.map<Text, Value>(index.columns, func(col) {
                            getColumn(record, col)
                        });
                        
                        // This is a very simplified key construction - in production we'd have
                        // proper composite key handling
                        let key = if (keyColumns.size() == 1) {
                            keyColumns[0]
                        } else {
                            #Record(Array.mapEntries<Text, Value, (Text, Value)>(
                                index.columns,
                                keyColumns,
                                func(col, val) { (col, val) }
                            ))
                        };
                        
                        // Check for existing records with this key
                        for (entry in index.entries.vals()) {
                            let (entryKey, rowIndices) = entry;
                            
                            if (compareValues(entryKey, key) == ?#EQ) {
                                // Found a match, check if it's our excluded index
                                switch (excludeIndex) {
                                    case (?excludeIdx) {
                                        if (not Array.contains<Nat>(rowIndices, excludeIdx, Nat.equal)) {
                                            return #err("Unique constraint violation on " # index.name);
                                        };
                                    };
                                    case (null) {
                                        return #err("Unique constraint violation on " # index.name);
                                    };
                                };
                            };
                        };
                    };
                };
                
                return #ok();
            };
            case (null) {
                return #err("Table not found: " # tableName);
            };
        };
    };
    
    /// Update indexes after inserting a record
    private func updateIndexesAfterInsert(tableName : Text, record : Record, rowIndex : Nat) : Result.Result<(), Text> {
        switch (tables.get(tableName)) {
            case (?table) {
                for (i in Iter.range(0, table.indexes.size() - 1)) {
                    let index = table.indexes[i];
                    
                    // Build the key for this record
                    let keyColumns = Array.map<Text, Value>(index.columns, func(col) {
                        getColumn(record, col)
                    });
                    
                    // This is a very simplified key construction - in production we'd have
                    // proper composite key handling
                    let key = if (keyColumns.size() == 1) {
                        keyColumns[0]
                    } else {
                        #Record(Array.mapEntries<Text, Value, (Text, Value)>(
                            index.columns,
                            keyColumns,
                            func(col, val) { (col, val) }
                        ))
                    };
                    
                    // Find existing entry for this key
                    var found = false;
                    let updatedEntries = Array.map<(Key, [Nat]), (Key, [Nat])>(
                        index.entries,
                        func(entry) {
                            let (entryKey, rowIndices) = entry;
                            
                            if (compareValues(entryKey, key) == ?#EQ) {
                                found := true;
                                (entryKey, Array.append(rowIndices, [rowIndex]))
                            } else {
                                entry
                            };
                        }
                    );
                    
                    // If no existing entry, add a new one
                    let finalEntries = if (not found) {
                        Array.append(updatedEntries, [(key, [rowIndex])])
                    } else {
                        updatedEntries
                    };
                    
                    // Update the index
                    let updatedIndex = {
                        name = index.name;
                        indexType = index.indexType;
                        columns = index.columns;
                        unique = index.unique;
                        var entries = finalEntries;
                    };
                    
                    table.indexes[i] := updatedIndex;
                };
                
                return #ok();
            };
            case (null) {
                return #err("Table not found: " # tableName);
            };
        };
    };
    
    /// Estimate the size of a record
    private func estimateRecordSize(record : Record) : Nat {
        var size = 0;
        
        for ((key, value) in record.vals()) {
            size += Text.size(key);
            
            // Estimated size of the value
            size += estimateValueSize(value);
        };
        
        size
    };
    
    /// Estimate the size of a value
    private func estimateValueSize(value : Value) : Nat {
        switch (value) {
            case (#Int(_)) 8;
            case (#Float(_)) 8;
            case (#Text(text)) Text.size(text);
            case (#Bool(_)) 1;
            case (#Blob(blob)) blob.size();
            case (#Null) 0;
            case (#Array(arr)) {
                var size = 0;
                for (v in arr.vals()) {
                    size += estimateValueSize(v);
                };
                size
            };
            case (#Record(rec)) {
                var size = 0;
                for ((k, v) in rec.vals()) {
                    size += Text.size(k);
                    size += estimateValueSize(v);
                };
                size
            };
            case (#Variant(name, valueOpt)) {
                var size = Text.size(name);
                switch (valueOpt) {
                    case (?v) size += estimateValueSize(v);
                    case (null) {};
                };
                size
            };
        };
    };
    
    // ===== Query Execution =====
    
    /// Execute a query on the shard
    public shared(msg) func executeQuery(query : ShardQuery) : async QueryResult<ShardResult> {
        let startTime = Time.now();
        
        // Update statistics
        queryCount += 1;
        
        try {
            var resultSet : ResultSet = {
                columns = [];
                rows = [];
                rowCount = 0;
            };
            
            var warnings : [Text] = [];
            
            // Transaction context
            var transactionId : ?TransactionId = query.transactionId;
            
            // Process each operation in sequence
            for (op in query.operations.vals()) {
                switch (op) {
                    case (#Scan(scanOp)) {
                        let tableName = scanOp.tableName;
                        
                        // Apply filter
                        let filterResult = applyFilter(tableName, scanOp.filter);
                        
                        switch (filterResult) {
                            case (#Match(indices)) {
                                // Get matching rows
                                switch (tables.get(tableName)) {
                                    case (?table) {
                                        let matchingRecords = Buffer.Buffer<Record>(indices.size());
                                        
                                        for (idx in indices.vals()) {
                                            if (idx < table.records.size()) {
                                                matchingRecords.add(table.records[idx]);
                                            };
                                        };
                                        
                                        // Extract column names from first record if available
                                        let columns = if (matchingRecords.size() > 0) {
                                            let record = matchingRecords.get(0);
                                            Iter.toArray(record.keys());
                                        } else {
                                            // If no records, use schema
                                            Array.map<CommonTypes.ColumnDefinition, Text>(
                                                table.schema.columns,
                                                func(col) { col.name }
                                            )
                                        };
                                        
                                        // Convert records to rows
                                        let rows = Buffer.Buffer<[Value]>(matchingRecords.size());
                                        
                                        for (record in matchingRecords.vals()) {
                                            let row = Array.map<Text, Value>(columns, func(col) {
                                                getColumn(record, col)
                                            });
                                            rows.add(row);
                                        };
                                        
                                        resultSet := {
                                            columns = columns;
                                            rows = Buffer.toArray(rows);
                                            rowCount = rows.size();
                                        };
                                    };
                                    case (null) {
                                        return #err(#ExecutionError("Table not found: " # tableName));
                                    };
                                };
                            };
                            case (#NoMatch) {
                                // No matching rows
                                resultSet := {
                                    columns = [];
                                    rows = [];
                                    rowCount = 0;
                                };
                            };
                            case (#Error(e)) {
                                return #err(#ExecutionError(e));
                            };
                        };
                    };
                    case (#IndexScan(indexOp)) {
                        // Similar to Scan but uses an index
                        // For brevity, I'm omitting the implementation
                        warnings := Array.append(warnings, ["Index scan not fully implemented"]);
                    };
                    case (#PointLookup(lookupOp)) {
                        let tableName = lookupOp.tableName;
                        let key = lookupOp.key;
                        
                        // This is a simplified implementation - in production we'd use an index
                        switch (tables.get(tableName)) {
                            case (?table) {
                                // Find primary key columns
                                let pkCols = table.schema.primaryKey;
                                
                                if (pkCols.size() > 0) {
                                    // Assume the key is for the first primary key column
                                    let pkCol = pkCols[0];
                                    
                                    // Find the record with matching key
                                    let matchingIdx = Array.findIndex<Record>(
                                        table.records, 
                                        func(rec) {
                                            switch (getRecordValue(rec, pkCol)) {
                                                case (?val) {
                                                    compareValues(val, key) == ?#EQ
                                                };
                                                case (null) false;
                                            };
                                        }
                                    );
                                    
                                    switch (matchingIdx) {
                                        case (?idx) {
                                            let record = table.records[idx];
                                            let columns = Iter.toArray(record.keys());
                                            let row = Array.map<Text, Value>(columns, func(col) {
                                                getColumn(record, col)
                                            });
                                            
                                            resultSet := {
                                                columns = columns;
                                                rows = [row];
                                                rowCount = 1;
                                            };
                                        };
                                        case (null) {
                                            // No matching record
                                            resultSet := {
                                                columns = [];
                                                rows = [];
                                                rowCount = 0;
                                            };
                                        };
                                    };
                                } else {
                                    warnings := Array.append(warnings, ["Table has no primary key: " # tableName]);
                                };
                            };
                            case (null) {
                                return #err(#ExecutionError("Table not found: " # tableName));
                            };
                        };
                    };
                    case (#Insert(insertOp)) {
                        let tableName = insertOp.tableName;
                        let records = insertOp.records;
                        
                        if (records.size() == 0) {
                            warnings := Array.append(warnings, ["No records to insert"]);
                        } else {
                            switch (tables.get(tableName)) {
                                case (?table) {
                                    var insertedCount = 0;
                                    
                                    for (record in records.vals()) {
                                        // Validate unique constraints
                                        let constraintCheck = checkUniqueConstraints(tableName, record, null);
                                        
                                        switch (constraintCheck) {
                                            case (#err(e)) {
                                                return #err(#ConstraintViolation(e));
                                            };
                                            case (#ok()) {
                                                // Add record to table
                                                let rowIndex = table.records.size();
                                                table.records := Array.append(table.records, [record]);
                                                
                                                // Update indexes
                                                let indexUpdate = updateIndexesAfterInsert(tableName, record, rowIndex);
                                                
                                                switch (indexUpdate) {
                                                    case (#err(e)) {
                                                        // Revert the insert
                                                        table.records := Array.tabulate<Record>(
                                                            rowIndex,
                                                            func(i) { table.records[i] }
                                                        );
                                                        
                                                        return #err(#ExecutionError(e));
                                                    };
                                                    case (#ok()) {
                                                        insertedCount += 1;
                                                        
                                                        // Update statistics
                                                        recordCount += 1;
                                                        dataSize += estimateRecordSize(record);
                                                    };
                                                };
                                            };
                                        };
                                    };
                                    
                                    resultSet := {
                                        columns = ["insertedCount"];
                                        rows = [[#Int(insertedCount)]];
                                        rowCount = 1;
                                    };
                                    
                                    // Update statistics
                                    writeCount += 1;
                                };
                                case (null) {
                                    return #err(#ExecutionError("Table not found: " # tableName));
                                };
                            };
                        };
                    };
                    case (#Update(updateOp)) {
                        let tableName = updateOp.tableName;
                        let updates = updateOp.updates;
                        
                        if (updates.size() == 0) {
                            warnings := Array.append(warnings, ["No updates to apply"]);
                        } else {
                            // Apply filter to find rows to update
                            let filterResult = applyFilter(tableName, updateOp.filter);
                            
                            switch (filterResult) {
                                case (#Match(indices)) {
                                    switch (tables.get(tableName)) {
                                        case (?table) {
                                            var updatedCount = 0;
                                            
                                            // Make a copy of the records array
                                            var updatedRecords = Array.tabulate<Record>(
                                                table.records.size(),
                                                func(i) { table.records[i] }
                                            );
                                            
                                            for (idx in indices.vals()) {
                                                if (idx < table.records.size()) {
                                                    let record = table.records[idx];
                                                    let updatedRecord = updateRecord(record, updates);
                                                    
                                                    // Validate unique constraints
                                                    let constraintCheck = checkUniqueConstraints(tableName, updatedRecord, ?idx);
                                                    
                                                    switch (constraintCheck) {
                                                        case (#err(e)) {
                                                            return #err(#ConstraintViolation(e));
                                                        };
                                                        case (#ok()) {
                                                            updatedRecords[idx] := updatedRecord;
                                                            updatedCount += 1;
                                                            
                                                            // Update dataSize
                                                            dataSize := dataSize - estimateRecordSize(record) + estimateRecordSize(updatedRecord);
                                                        };
                                                    };
                                                };
                                            };
                                            
                                            // Apply updates
                                            table.records := updatedRecords;
                                            
                                            // Update indexes
                                            // This would be more complex in a real implementation
                                            // For now, just note that we should rebuild indexes
                                            warnings := Array.append(warnings, ["Indexes need rebuilding after update"]);
                                            
                                            resultSet := {
                                                columns = ["updatedCount"];
                                                rows = [[#Int(updatedCount)]];
                                                rowCount = 1;
                                            };
                                            
                                            // Update statistics
                                            writeCount += 1;
                                        };
                                        case (null) {
                                            return #err(#ExecutionError("Table not found: " # tableName));
                                        };
                                    };
                                };
                                case (#NoMatch) {
                                    // No rows matched the filter
                                    resultSet := {
                                        columns = ["updatedCount"];
                                        rows = [[#Int(0)]];
                                        rowCount = 1;
                                    };
                                };
                                case (#Error(e)) {
                                    return #err(#ExecutionError(e));
                                };
                            };
                        };
                    };
                    case (#Delete(deleteOp)) {
                        let tableName = deleteOp.tableName;
                        
                        // Apply filter to find rows to delete
                        let filterResult = applyFilter(tableName, deleteOp.filter);
                        
                        switch (filterResult) {
                            case (#Match(indices)) {
                                switch (tables.get(tableName)) {
                                    case (?table) {
                                        let sortedIndices = Array.sort<Nat>(indices, Nat.compare);
                                        
                                        // Delete from highest index to lowest to avoid shifting problems
                                        let reversedIndices = Array.tabulate<Nat>(
                                            sortedIndices.size(),
                                            func(i) { sortedIndices[sortedIndices.size() - 1 - i] }
                                        );
                                        
                                        var deletedCount = 0;
                                        var deletedSize = 0;
                                        
                                        var remainingRecords = Array.tabulate<Record>(
                                            table.records.size(),
                                            func(i) { table.records[i] }
                                        );
                                        
                                        for (idx in reversedIndices.vals()) {
                                            if (idx < remainingRecords.size()) {
                                                let record = remainingRecords[idx];
                                                deletedSize += estimateRecordSize(record);
                                                
                                                // Remove record
                                                let before = Array.tabulate<Record>(idx, func(i) { remainingRecords[i] });
                                                let after = Array.tabulate<Record>(
                                                    remainingRecords.size() - idx - 1,
                                                    func(i) { remainingRecords[idx + 1 + i] }
                                                );
                                                
                                                remainingRecords := Array.append(before, after);
                                                deletedCount += 1;
                                            };
                                        };
                                        
                                        // Apply updates
                                        table.records := remainingRecords;
                                        
                                        // Update indexes
                                        // This would be more complex in a real implementation
                                        // For now, just note that we should rebuild indexes
                                        warnings := Array.append(warnings, ["Indexes need rebuilding after delete"]);
                                        
                                        resultSet := {
                                            columns = ["deletedCount"];
                                            rows = [[#Int(deletedCount)]];
                                            rowCount = 1;
                                        };
                                        
                                        // Update statistics
                                        writeCount += 1;
                                        recordCount -= deletedCount;
                                        dataSize -= deletedSize;
                                    };
                                    case (null) {
                                        return #err(#ExecutionError("Table not found: " # tableName));
                                    };
                                };
                            };
                            case (#NoMatch) {
                                // No rows matched the filter
                                resultSet := {
                                    columns = ["deletedCount"];
                                    rows = [[#Int(0)]];
                                    rowCount = 1;
                                };
                            };
                            case (#Error(e)) {
                                return #err(#ExecutionError(e));
                            };
                        };
                    };
                    case (#Project(projectOp)) {
                        // Project columns from the current result set
                        let columns = projectOp.columns;
                        
                        if (resultSet.rowCount > 0) {
                            if (columns.size() == 1 and columns[0] == "*") {
                                // Keep all columns
                            } else {
                                // Project specific columns
                                let currentRecords = Buffer.Buffer<Record>(resultSet.rowCount);
                                
                                // Convert rows back to records
                                for (i in Iter.range(0, resultSet.rowCount - 1)) {
                                    let row = resultSet.rows[i];
                                    var record = HashMap.HashMap<Text, Value>(10, Text.equal, Text.hash);
                                    
                                    for (j in Iter.range(0, resultSet.columns.size() - 1)) {
                                        record.put(resultSet.columns[j], row[j]);
                                    };
                                    
                                    currentRecords.add(record);
                                };
                                
                                // Project columns
                                let projectedRows = Buffer.Buffer<[Value]>(resultSet.rowCount);
                                
                                for (record in currentRecords.vals()) {
                                    let projectedRow = Array.map<Text, Value>(columns, func(col) {
                                        getColumn(record, col)
                                    });
                                    
                                    projectedRows.add(projectedRow);
                                };
                                
                                // Update result set
                                resultSet := {
                                    columns = columns;
                                    rows = Buffer.toArray(projectedRows);
                                    rowCount = projectedRows.size();
                                };
                            };
                        };
                    };
                    case _ {
                        // Other operations (Join, Aggregate, Sort, Limit) would be implemented here
                        warnings := Array.append(warnings, ["Operation not fully implemented: " # debug_show(op)]);
                    };
                };
            };
            
            let endTime = Time.now();
            let executionTimeNanos = Nat64.fromIntWrap(endTime - startTime);
            
            // Update statistics
            totalQueryTime += executionTimeNanos;
            
            let result : ShardResult = {
                queryId = query.queryId;
                resultSet = resultSet;
                executionTime = executionTimeNanos;
                cyclesConsumed = 1_000_000_000; // Placeholder
                warnings = warnings;
            };
            
            #ok(result)
        } catch (error) {
            // Update statistics
            errorCount += 1;
            
            #err(#ExecutionError("Error executing query: " # Error.message(error)))
        };
    };
    
    // ===== Transaction Support =====
    
    /// Prepare a transaction (two-phase commit)
    public shared(msg) func prepare(txId : TransactionId, operations : [TransactionOperation]) : async PrepareResult {
        // Update statistics
        writeCount += 1;
        
        try {
            // Check if transaction already exists
            switch (transactions.get(txId)) {
                case (?txn) {
                    if (txn.status != #Active) {
                        return #Failed("Transaction " # txId # " is already in " # debug_show(txn.status) # " state");
                    };
                };
                case (null) {
                    // Create new transaction
                    let transaction : TransactionState = {
                        id = txId;
                        status = #Active;
                        operations = operations;
                        createdAt = Time.now();
                        updatedAt = Time.now();
                        coordinator = msg.caller;
                        var prepareLog = null;
                    };
                    
                    transactions.put(txId, transaction);
                };
            };
            
            // Validate and log all operations
            let preparedOperations = Buffer.Buffer<ShardTypes.PreparedOperation>(0);
            let undoOperations = Buffer.Buffer<ShardTypes.UndoOperation>(0);
            
            for (op in operations.vals()) {
                switch (op) {
                    case (#Insert(insertOp)) {
                        let tableName = insertOp.tableName;
                        let records = insertOp.records;
                        
                        switch (tables.get(tableName)) {
                            case (?table) {
                                // Validate records against constraints
                                for (record in records.vals()) {
                                    let constraintCheck = checkUniqueConstraints(tableName, record, null);
                                    
                                    switch (constraintCheck) {
                                        case (#err(e)) {
                                            return #Failed("Constraint violation: " # e);
                                        };
                                        case (#ok()) {};
                                    };
                                };
                                
                                // Log prepared operation
                                let rowIndices = Array.tabulate<Nat>(
                                    records.size(),
                                    func(i) { table.records.size() + i }
                                );
                                
                                preparedOperations.add(#Insert({
                                    tableName = tableName;
                                    records = records;
                                    rowIndices = rowIndices;
                                }));
                                
                                // Log undo operation
                                undoOperations.add(#UndoInsert({
                                    tableName = tableName;
                                    rowIndices = rowIndices;
                                }));
                            };
                            case (null) {
                                return #Failed("Table not found: " # tableName);
                            };
                        };
                    };
                    case (#Update(updateOp)) {
                        let tableName = updateOp.tableName;
                        let updates = updateOp.updates;
                        
                        // Apply filter to find rows to update
                        let filterResult = applyFilter(tableName, updateOp.filter);
                        
                        switch (filterResult) {
                            case (#Match(indices)) {
                                switch (tables.get(tableName)) {
                                    case (?table) {
                                        let oldValues = Buffer.Buffer<[(Text, Value)]>(indices.size());
                                        
                                        for (idx in indices.vals()) {
                                            if (idx < table.records.size()) {
                                                let record = table.records[idx];
                                                let updatedRecord = updateRecord(record, updates);
                                                
                                                // Validate constraints
                                                let constraintCheck = checkUniqueConstraints(tableName, updatedRecord, ?idx);
                                                
                                                switch (constraintCheck) {
                                                    case (#err(e)) {
                                                        return #Failed("Constraint violation: " # e);
                                                    };
                                                    case (#ok()) {};
                                                };
                                                
                                                // Store old values for undo
                                                let recordOldValues = Buffer.Buffer<(Text, Value)>(updates.size());
                                                
                                                for ((col, _) in updates.vals()) {
                                                    switch (getRecordValue(record, col)) {
                                                        case (?val) {
                                                            recordOldValues.add((col, val));
                                                        };
                                                        case (null) {
                                                            recordOldValues.add((col, #Null));
                                                        };
                                                    };
                                                };
                                                
                                                oldValues.add(Buffer.toArray(recordOldValues));
                                            };
                                        };
                                        
                                        // Log prepared operation
                                        preparedOperations.add(#Update({
                                            tableName = tableName;
                                            updates = updates;
                                            affectedRows = indices;
                                            oldValues = Buffer.toArray(oldValues);
                                        }));
                                        
                                        // Log undo operation
                                        undoOperations.add(#UndoUpdate({
                                            tableName = tableName;
                                            affectedRows = indices;
                                            oldValues = Buffer.toArray(oldValues);
                                        }));
                                    };
                                    case (null) {
                                        return #Failed("Table not found: " # tableName);
                                    };
                                };
                            };
                            case (#NoMatch) {
                                // No rows matched, nothing to do
                            };
                            case (#Error(e)) {
                                return #Failed("Filter error: " # e);
                            };
                        };
                    };
                    case (#Delete(deleteOp)) {
                        let tableName = deleteOp.tableName;
                        
                        // Apply filter to find rows to delete
                        let filterResult = applyFilter(tableName, deleteOp.filter);
                        
                        switch (filterResult) {
                            case (#Match(indices)) {
                                switch (tables.get(tableName)) {
                                    case (?table) {
                                        let oldRecords = Buffer.Buffer<Record>(indices.size());
                                        
                                        for (idx in indices.vals()) {
                                            if (idx < table.records.size()) {
                                                oldRecords.add(table.records[idx]);
                                            };
                                        };
                                        
                                        // Log prepared operation
                                        preparedOperations.add(#Delete({
                                            tableName = tableName;
                                            affectedRows = indices;
                                            oldRecords = Buffer.toArray(oldRecords);
                                        }));
                                        
                                        // Log undo operation
                                        undoOperations.add(#UndoDelete({
                                            tableName = tableName;
                                            rowIndices = indices;
                                            records = Buffer.toArray(oldRecords);
                                        }));
                                    };
                                    case (null) {
                                        return #Failed("Table not found: " # tableName);
                                    };
                                };
                            };
                            case (#NoMatch) {
                                // No rows matched, nothing to do
                            };
                            case (#Error(e)) {
                                return #Failed("Filter error: " # e);
                            };
                        };
                    };
                };
            };
            
            // Update transaction state
            switch (transactions.get(txId)) {
                case (?txn) {
                    let prepareLog = {
                        preparedAt = Time.now();
                        operations = Buffer.toArray(preparedOperations);
                        undo = Buffer.toArray(undoOperations);
                    };
                    
                    let updatedTxn : TransactionState = {
                        id = txn.id;
                        status = #Prepared;
                        operations = txn.operations;
                        createdAt = txn.createdAt;
                        updatedAt = Time.now();
                        coordinator = txn.coordinator;
                        var prepareLog = ?prepareLog;
                    };
                    
                    transactions.put(txId, updatedTxn);
                };
                case (null) {
                    return #Failed("Transaction " # txId # " not found");
                };
            };
            
            #Ready
        } catch (error) {
            // Update statistics
            errorCount += 1;
            
            #Failed("Error preparing transaction: " # Error.message(error))
        };
    };
    
    /// Commit a prepared transaction
    public shared(msg) func commit(txId : TransactionId) : async CommitResult {
        // Update statistics
        writeCount += 1;
        
        try {
            switch (transactions.get(txId)) {
                case (?txn) {
                    if (txn.status != #Prepared) {
                        return #Failed("Transaction " # txId # " is not in prepared state");
                    };
                    
                    if (txn.coordinator != msg.caller) {
                        return #Failed("Only the coordinator can commit a transaction");
                    };
                    
                    // Apply prepared operations
                    switch (txn.prepareLog) {
                        case (?prepareLog) {
                            for (op in prepareLog.operations.vals()) {
                                switch (op) {
                                    case (#Insert(insertOp)) {
                                        let tableName = insertOp.tableName;
                                        let records = insertOp.records;
                                        
                                        switch (tables.get(tableName)) {
                                            case (?table) {
                                                // Add records to table
                                                table.records := Array.append(table.records, records);
                                                
                                                // Update indexes
                                                for (i in Iter.range(0, records.size() - 1)) {
                                                    let record = records[i];
                                                    let rowIndex = insertOp.rowIndices[i];
                                                    
                                                    ignore updateIndexesAfterInsert(tableName, record, rowIndex);
                                                };
                                                
                                                // Update statistics
                                                recordCount += records.size();
                                                for (record in records.vals()) {
                                                    dataSize += estimateRecordSize(record);
                                                };
                                            };
                                            case (null) {
                                                return #Failed("Table not found: " # tableName);
                                            };
                                        };
                                    };
                                    case (#Update(updateOp)) {
                                        let tableName = updateOp.tableName;
                                        let updates = updateOp.updates;
                                        let affectedRows = updateOp.affectedRows;
                                        
                                        switch (tables.get(tableName)) {
                                            case (?table) {
                                                var oldSize = 0;
                                                var newSize = 0;
                                                
                                                for (i in Iter.range(0, affectedRows.size() - 1)) {
                                                    let idx = affectedRows[i];
                                                    
                                                    if (idx < table.records.size()) {
                                                        let record = table.records[idx];
                                                        oldSize += estimateRecordSize(record);
                                                        
                                                        let updatedRecord = updateRecord(record, updates);
                                                        newSize += estimateRecordSize(updatedRecord);
                                                        
                                                        table.records[idx] := updatedRecord;
                                                    };
                                                };
                                                
                                                // Update statistics
                                                dataSize := dataSize - oldSize + newSize;
                                                
                                                // Rebuild indexes
                                                // This would be more complex in a real implementation
                                            };
                                            case (null) {
                                                return #Failed("Table not found: " # tableName);
                                            };
                                        };
                                    };
                                    case (#Delete(deleteOp)) {
                                        let tableName = deleteOp.tableName;
                                        let affectedRows = deleteOp.affectedRows;
                                        
                                        switch (tables.get(tableName)) {
                                            case (?table) {
                                                let sortedIndices = Array.sort<Nat>(affectedRows, Nat.compare);
                                                
                                                // Delete from highest index to lowest to avoid shifting problems
                                                let reversedIndices = Array.tabulate<Nat>(
                                                    sortedIndices.size(),
                                                    func(i) { sortedIndices[sortedIndices.size() - 1 - i] }
                                                );
                                                
                                                var deletedCount = 0;
                                                var deletedSize = 0;
                                                
                                                var remainingRecords = Array.tabulate<Record>(
                                                    table.records.size(),
                                                    func(i) { table.records[i] }
                                                );
                                                
                                                for (idx in reversedIndices.vals()) {
                                                    if (idx < remainingRecords.size()) {
                                                        let record = remainingRecords[idx];
                                                        deletedSize += estimateRecordSize(record);
                                                        
                                                        // Remove record
                                                        let before = Array.tabulate<Record>(idx, func(i) { remainingRecords[i] });
                                                        let after = Array.tabulate<Record>(
                                                            remainingRecords.size() - idx - 1,
                                                            func(i) { remainingRecords[idx + 1 + i] }
                                                        );
                                                        
                                                        remainingRecords := Array.append(before, after);
                                                        deletedCount += 1;
                                                    };
                                                };
                                                
                                                // Apply updates
                                                table.records := remainingRecords;
                                                
                                                // Update statistics
                                                recordCount -= deletedCount;
                                                dataSize -= deletedSize;
                                                
                                                // Rebuild indexes
                                                // This would be more complex in a real implementation
                                            };
                                            case (null) {
                                                return #Failed("Table not found: " # tableName);
                                            };
                                        };
                                    };
                                };
                            };
                        };
                        case (null) {
                            return #Failed("No prepare log found for transaction " # txId);
                        };
                    };
                    
                    // Update transaction status
                    let updatedTxn : TransactionState = {
                        id = txn.id;
                        status = #Committed;
                        operations = txn.operations;
                        createdAt = txn.createdAt;
                        updatedAt = Time.now();
                        coordinator = txn.coordinator;
                        var prepareLog = txn.prepareLog;
                    };
                    
                    transactions.put(txId, updatedTxn);
                    
                    #Committed
                };
                case (null) {
                    #Failed("Transaction " # txId # " not found")
                };
            };
        } catch (error) {
            // Update statistics
            errorCount += 1;
            
            #Failed("Error committing transaction: " # Error.message(error))
        };
    };
    
    /// Abort a transaction
    public shared(msg) func abort(txId : TransactionId) : async () {
        try {
            switch (transactions.get(txId)) {
                case (?txn) {
                    if (txn.status == #Committed) {
                        // Cannot abort a committed transaction
                        return;
                    };
                    
                    // Update transaction status
                    let updatedTxn : TransactionState = {
                        id = txn.id;
                        status = #Aborted;
                        operations = txn.operations;
                        createdAt = txn.createdAt;
                        updatedAt = Time.now();
                        coordinator = txn.coordinator;
                        var prepareLog = txn.prepareLog;
                    };
                    
                    transactions.put(txId, updatedTxn);
                };
                case (null) {
                    // Transaction not found, nothing to do
                };
            };
        } catch (error) {
            // Error handling
            Debug.print("Error aborting transaction: " # Error.message(error));
        };
    };
    
    // ===== Shard Management =====
    
    /// Get shard information
    public query func getInfo() : async ShardInfo {
        {
            id = shardId;
            databaseId = databaseId;
            tableCount = tables.size();
            recordCount = recordCount;
            dataSize = dataSize;
            status = status;
        }
    };
    
    /// Get shard metrics
    public query func getMetrics() : async ShardMetrics {
        let averageQueryTime = if (queryCount > 0) {
            totalQueryTime / Nat64.fromNat(queryCount)
        } else {
            0
        };
        
        let averageWriteTime = if (writeCount > 0) {
            totalWriteTime / Nat64.fromNat(writeCount)
        } else {
            0
        };
        
        {
            recordCount = recordCount;
            dataSize = dataSize;
            queryCount = queryCount;
            writeCount = writeCount;
            errorCount = errorCount;
            averageQueryTime = averageQueryTime;
            averageWriteTime = averageWriteTime;
            memoryUsage = 0; // Placeholder, would need a proper way to measure this
            cyclesBalance = Cycles.balance();
        }
    };
    
    /// Get the status of the shard
    public query func getStatus() : async ShardStatus {
        status
    };
    
    /// Update the schema of tables in the shard
    public shared(msg) func updateSchema(tableDefs : [TableSchema]) : async SchemaResult<()> {
        try {
            for (tableDef in tableDefs.vals()) {
                let tableName = tableDef.name;
                
                switch (tables.get(tableName)) {
                    case (?existingTable) {
                        // Check if this is a compatible schema change
                        // This would need much more sophisticated logic in a real implementation
                        
                        // For now, just update the schema definition but keep the data
                        let updatedTable : TableStorage = {
                            name = existingTable.name;
                            schema = tableDef;
                            var records = existingTable.records;
                            var indexes = existingTable.indexes;
                        };
                        
                        tables.put(tableName, updatedTable);
                    };
                    case (null) {
                        // New table, create it
                        let newTable : TableStorage = {
                            name = tableDef.name;
                            schema = tableDef;
                            var records = [];
                            var indexes = [];
                        };
                        
                        tables.put(tableName, newTable);
                    };
                };
            };
            
            #ok()
        } catch (error) {
            #err(#MigrationFailed("Error updating schema: " # Error.message(error)))
        };
    };
    
    /// Set the status of the shard
    public shared(msg) func setStatus(newStatus : ShardStatus) : async ShardResult<()> {
        // In a real implementation, we'd check caller authorization
        
        status := newStatus;
        #ok()
    };
}