/**
 * Query execution logic for Shard canister
 * 
 * This module is responsible for handling query execution within a shard,
 * including filtering, aggregation, and result formatting.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import HashMap "mo:base/HashMap";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";
import ShardTypes "./types";

module {
    // ===== Type Aliases =====
    
    type Value = CommonTypes.Value;
    type Record = CommonTypes.Record;
    type TableSchema = CommonTypes.TableSchema;
    type FilterExpression = CommonTypes.FilterExpression;
    type ResultSet = CommonTypes.ResultSet;
    
    type TableStorage = ShardTypes.TableStorage;
    type IndexStorage = ShardTypes.IndexStorage;
    type ShardOperation = CommonTypes.ShardOperation;
    type FilterResult = ShardTypes.FilterResult;
    type IndexLookupResult = ShardTypes.IndexLookupResult;
    type RowPredicate = ShardTypes.RowPredicate;
    
    type QueryError = Errors.QueryError;
    type QueryResult<T> = Errors.QueryResult<T>;
    
    // ===== Query Execution =====
    
    /// Execute a sequence of operations
    public func executeOperations(
        operations : [ShardOperation],
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        if (operations.size() == 0) {
            return #err(#ValidationError("No operations to execute"));
        };
        
        // Initialize result set
        var currentResultSet : ResultSet = {
            columns = [];
            rows = [];
            rowCount = 0;
        };
        
        // Process each operation in sequence
        for (op in operations.vals()) {
            let opResult = executeOperation(op, currentResultSet, tables);
            
            switch (opResult) {
                case (#err(error)) {
                    return #err(error);
                };
                case (#ok(resultSet)) {
                    currentResultSet := resultSet;
                };
            };
        };
        
        #ok(currentResultSet)
    };
    
    /// Execute a single operation
    public func executeOperation(
        operation : ShardOperation,
        currentResultSet : ResultSet,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (operation) {
            case (#Scan(scanOp)) {
                executeScan(scanOp.tableName, scanOp.filter, tables)
            };
            case (#IndexScan(indexOp)) {
                executeIndexScan(indexOp.tableName, indexOp.indexName, indexOp.filter, tables)
            };
            case (#PointLookup(lookupOp)) {
                executePointLookup(lookupOp.tableName, lookupOp.key, tables)
            };
            case (#Insert(insertOp)) {
                executeInsert(insertOp.tableName, insertOp.records, tables)
            };
            case (#Update(updateOp)) {
                executeUpdate(updateOp.tableName, updateOp.updates, updateOp.filter, tables)
            };
            case (#Delete(deleteOp)) {
                executeDelete(deleteOp.tableName, deleteOp.filter, tables)
            };
            case (#Join(joinOp)) {
                executeJoin(joinOp.leftTable, joinOp.rightTable, joinOp.condition, joinOp.joinType, tables)
            };
            case (#Aggregate(aggOp)) {
                executeAggregate(currentResultSet, aggOp.operations, aggOp.groupBy)
            };
            case (#Sort(sortOp)) {
                executeSort(currentResultSet, sortOp.columns)
            };
            case (#Limit(limitOp)) {
                executeLimit(currentResultSet, limitOp.count, limitOp.offset)
            };
            case (#Project(projectOp)) {
                executeProject(currentResultSet, projectOp.columns)
            };
        };
    };
    
    // ===== Operation Implementations =====
    
    /// Execute a table scan operation
    public func executeScan(
        tableName : Text,
        filter : ?FilterExpression,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                // Apply filter to find matching records
                let matchingIndices = applyFilter(table, filter);
                
                switch (matchingIndices) {
                    case (#Match(indices)) {
                        // Construct result set from matching rows
                        createResultSetFromIndices(table, indices)
                    };
                    case (#NoMatch) {
                        // Empty result set
                        #ok({
                            columns = getColumnNames(table);
                            rows = [];
                            rowCount = 0;
                        })
                    };
                    case (#Error(msg)) {
                        #err(#ExecutionError(msg))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute an index scan operation
    public func executeIndexScan(
        tableName : Text,
        indexName : Text,
        filter : ?FilterExpression,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                // Find the requested index
                let indexOpt = Array.find<IndexStorage>(
                    table.indexes,
                    func(idx) { idx.name == indexName }
                );
                
                switch (indexOpt) {
                    case (?index) {
                        // Use index to find matching records
                        let matchingIndices = lookupWithIndex(table, index, filter);
                        
                        switch (matchingIndices) {
                            case (#Match(indices)) {
                                // Construct result set from matching rows
                                createResultSetFromIndices(table, indices)
                            };
                            case (#NoMatch) {
                                // Empty result set
                                #ok({
                                    columns = getColumnNames(table);
                                    rows = [];
                                    rowCount = 0;
                                })
                            };
                            case (#NotIndexed) {
                                // Fall back to regular scan
                                executeScan(tableName, filter, tables)
                            };
                            case (#Error(msg)) {
                                #err(#ExecutionError(msg))
                            };
                        };
                    };
                    case (null) {
                        #err(#ValidationError("Index not found: " # indexName))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute a point lookup operation
    public func executePointLookup(
        tableName : Text,
        key : Value,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                // Find primary key columns
                let tableSchema = table.schema;
                
                if (tableSchema.primaryKey.size() == 0) {
                    return #err(#ValidationError("Table has no primary key: " # tableName));
                };
                
                let pkColumn = tableSchema.primaryKey[0];
                
                // Create an equals filter for the primary key
                let filter : FilterExpression = #Equals(pkColumn, key);
                
                // Apply filter
                let matchingIndices = applyFilter(table, ?filter);
                
                switch (matchingIndices) {
                    case (#Match(indices)) {
                        // Construct result set from matching rows
                        createResultSetFromIndices(table, indices)
                    };
                    case (#NoMatch) {
                        // Empty result set
                        #ok({
                            columns = getColumnNames(table);
                            rows = [];
                            rowCount = 0;
                        })
                    };
                    case (#Error(msg)) {
                        #err(#ExecutionError(msg))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute an insert operation
    public func executeInsert(
        tableName : Text,
        records : [Record],
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                if (records.size() == 0) {
                    return #ok({
                        columns = ["insertedCount"];
                        rows = [[#Int(0)]];
                        rowCount = 1;
                    });
                };
                
                var insertedCount = 0;
                var errors = Buffer.Buffer<Text>(0);
                
                // Add records to table
                for (record in records.vals()) {
                    // In a real implementation, we'd validate against schema
                    // and check constraints
                    
                    // For now, just append to the records array
                    table.records := Array.append(table.records, [record]);
                    insertedCount += 1;
                };
                
                // Return result with count of inserted records
                #ok({
                    columns = ["insertedCount"];
                    rows = [[#Int(insertedCount)]];
                    rowCount = 1;
                })
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute an update operation
    public func executeUpdate(
        tableName : Text,
        updates : [(Text, Value)],
        filter : ?FilterExpression,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                if (updates.size() == 0) {
                    return #ok({
                        columns = ["updatedCount"];
                        rows = [[#Int(0)]];
                        rowCount = 1;
                    });
                };
                
                // Apply filter to find records to update
                let matchingIndices = applyFilter(table, filter);
                
                switch (matchingIndices) {
                    case (#Match(indices)) {
                        var updatedCount = 0;
                        
                        // Update matching records
                        var updatedRecords = Array.tabulate<Record>(
                            table.records.size(),
                            func(i) { table.records[i] }
                        );
                        
                        for (idx in indices.vals()) {
                            if (idx < table.records.size()) {
                                let record = table.records[idx];
                                let updatedRecord = updateRecord(record, updates);
                                updatedRecords[idx] := updatedRecord;
                                updatedCount += 1;
                            };
                        };
                        
                        table.records := updatedRecords;
                        
                        // Return result with count of updated records
                        #ok({
                            columns = ["updatedCount"];
                            rows = [[#Int(updatedCount)]];
                            rowCount = 1;
                        })
                    };
                    case (#NoMatch) {
                        // No matching records to update
                        #ok({
                            columns = ["updatedCount"];
                            rows = [[#Int(0)]];
                            rowCount = 1;
                        })
                    };
                    case (#Error(msg)) {
                        #err(#ExecutionError(msg))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute a delete operation
    public func executeDelete(
        tableName : Text,
        filter : ?FilterExpression,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        switch (tables.get(tableName)) {
            case (?table) {
                // Apply filter to find records to delete
                let matchingIndices = applyFilter(table, filter);
                
                switch (matchingIndices) {
                    case (#Match(indices)) {
                        let sortedIndices = Array.sort<Nat>(
                            indices,
                            func(a, b) { Nat.compare(a, b) }
                        );
                        
                        // Delete from highest index to lowest to avoid shifting issues
                        let reversedIndices = Array.tabulate<Nat>(
                            sortedIndices.size(),
                            func(i) { sortedIndices[sortedIndices.size() - 1 - i] }
                        );
                        
                        var deletedCount = 0;
                        var remainingRecords = Array.tabulate<Record>(
                            table.records.size(),
                            func(i) { table.records[i] }
                        );
                        
                        for (idx in reversedIndices.vals()) {
                            if (idx < remainingRecords.size()) {
                                let before = Array.tabulate<Record>(idx, func(i) { remainingRecords[i] });
                                let after = Array.tabulate<Record>(
                                    remainingRecords.size() - idx - 1,
                                    func(i) { remainingRecords[idx + 1 + i] }
                                );
                                
                                remainingRecords := Array.append(before, after);
                                deletedCount += 1;
                            };
                        };
                        
                        table.records := remainingRecords;
                        
                        // Return result with count of deleted records
                        #ok({
                            columns = ["deletedCount"];
                            rows = [[#Int(deletedCount)]];
                            rowCount = 1;
                        })
                    };
                    case (#NoMatch) {
                        // No matching records to delete
                        #ok({
                            columns = ["deletedCount"];
                            rows = [[#Int(0)]];
                            rowCount = 1;
                        })
                    };
                    case (#Error(msg)) {
                        #err(#ExecutionError(msg))
                    };
                };
            };
            case (null) {
                #err(#ValidationError("Table not found: " # tableName))
            };
        };
    };
    
    /// Execute a join operation
    public func executeJoin(
        leftTableName : Text,
        rightTableName : Text,
        condition : FilterExpression,
        joinType : CommonTypes.JoinType,
        tables : HashMap.HashMap<Text, TableStorage>
    ) : QueryResult<ResultSet> {
        // Verify tables exist
        let leftTableOpt = tables.get(leftTableName);
        let rightTableOpt = tables.get(rightTableName);
        
        switch (leftTableOpt, rightTableOpt) {
            case (?leftTable, ?rightTable) {
                // Extract join condition
                var leftColumn = "";
                var rightColumn = "";
                
                switch (condition) {
                    case (#Equals(col1, #Text(col2))) {
                        // Assuming join condition is in the form table1.column = table2.column
                        let col1Parts = Text.split(col1, #char('.'));
                        let col1Array = Iter.toArray(col1Parts);
                        
                        if (col1Array.size() == 2) {
                            leftColumn := col1Array[1];
                            rightColumn := col2;
                        } else {
                            return #err(#ValidationError("Invalid join condition format"));
                        };
                    };
                    case _ {
                        return #err(#ValidationError("Unsupported join condition type"));
                    };
                };
                
                // Perform the join
                switch (joinType) {
                    case (#Inner) {
                        // Simplified inner join implementation
                        let leftRecords = leftTable.records;
                        let rightRecords = rightTable.records;
                        
                        let joinedRows = Buffer.Buffer<[Value]>(0);
                        let joinedColumns = Buffer.Buffer<Text>(0);
                        
                        // Prepare column names
                        for (col in getColumnNames(leftTable).vals()) {
                            joinedColumns.add(leftTableName # "." # col);
                        };
                        
                        for (col in getColumnNames(rightTable).vals()) {
                            joinedColumns.add(rightTableName # "." # col);
                        };
                        
                        // Perform join
                        for (leftRecord in leftRecords.vals()) {
                            let leftValue = Utils.getRecordValue(leftRecord, leftColumn);
                            
                            for (rightRecord in rightRecords.vals()) {
                                let rightValue = Utils.getRecordValue(rightRecord, rightColumn);
                                
                                // Compare values for join
                                if (leftValue != null and rightValue != null) {
                                    if (Utils.compareValues(Option.get(leftValue, #Null), Option.get(rightValue, #Null)) == #EQ) {
                                        // Match found, create joined row
                                        let joinedRow = Buffer.Buffer<Value>(0);
                                        
                                        // Add left record values
                                        for (col in getColumnNames(leftTable).vals()) {
                                            joinedRow.add(Option.get(Utils.getRecordValue(leftRecord, col), #Null));
                                        };
                                        
                                        // Add right record values
                                        for (col in getColumnNames(rightTable).vals()) {
                                            joinedRow.add(Option.get(Utils.getRecordValue(rightRecord, col), #Null));
                                        };
                                        
                                        joinedRows.add(Buffer.toArray(joinedRow));
                                    };
                                };
                            };
                        };
                        
                        // Return joined result set
                        #ok({
                            columns = Buffer.toArray(joinedColumns);
                            rows = Buffer.toArray(joinedRows);
                            rowCount = joinedRows.size();
                        })
                    };
                    case _ {
                        // Other join types not fully implemented
                        #err(#UnsupportedOperation("Join type not fully implemented yet"))
                    };
                };
            };
            case (null, _) {
                #err(#ValidationError("Table not found: " # leftTableName))
            };
            case (_, null) {
                #err(#ValidationError("Table not found: " # rightTableName))
            };
        };
    };
    
    /// Execute an aggregate operation
    public func executeAggregate(
        input : ResultSet,
        operations : [CommonTypes.AggregateOperation],
        groupBy : [Text]
    ) : QueryResult<ResultSet> {
        if (input.rowCount == 0) {
            return #ok(input); // Empty input, return as is
        };
        
        if (operations.size() == 0 and groupBy.size() == 0) {
            return #ok(input); // No aggregation needed
        };
        
        // Group by implementation
        if (groupBy.size() > 0) {
            // Find group by column indices
            let groupByIndices = Buffer.Buffer<Nat>(groupBy.size());
            
            for (col in groupBy.vals()) {
                var found = false;
                
                for (i in Iter.range(0, input.columns.size() - 1)) {
                    if (input.columns[i] == col) {
                        groupByIndices.add(i);
                        found := true;
                        break;
                    };
                };
                
                if (not found) {
                    return #err(#ValidationError("Group by column not found: " # col));
                };
            };
            
            // Group rows by group key
            let groups = HashMap.HashMap<Text, Buffer.Buffer<[Value]>>(10, Text.equal, Text.hash);
            
            for (row in input.rows.vals()) {
                // Build group key
                var groupKey = "";
                
                for (idx in groupByIndices.vals()) {
                    if (idx < row.size()) {
                        groupKey := groupKey # Utils.valueToText(row[idx]) # "|";
                    };
                };
                
                // Add row to group
                let groupRows = switch (groups.get(groupKey)) {
                    case (null) {
                        let newBuffer = Buffer.Buffer<[Value]>(1);
                        groups.put(groupKey, newBuffer);
                        newBuffer
                    };
                    case (?buffer) {
                        buffer
                    };
                };
                
                groupRows.add(row);
            };
            
            // Apply aggregations to each group
            let resultRows = Buffer.Buffer<[Value]>(groups.size());
            
            for ((groupKey, groupRows) in groups.entries()) {
                let groupRow = Buffer.Buffer<Value>(0);
                
                // Add group by values (first row's values for these columns)
                if (groupRows.size() > 0) {
                    let firstRow = groupRows.get(0);
                    
                    for (idx in groupByIndices.vals()) {
                        if (idx < firstRow.size()) {
                            groupRow.add(firstRow[idx]);
                        };
                    };
                };
                
                // Apply aggregate operations
                for (op in operations.vals()) {
                    switch (op) {
                        case (#Count(?colName)) {
                            // Count non-null values
                            let colIdx = findColumnIndex(input.columns, colName);
                            
                            switch (colIdx) {
                                case (?idx) {
                                    var count = 0;
                                    
                                    for (row in groupRows.vals()) {
                                        if (idx < row.size() and row[idx] != #Null) {
                                            count += 1;
                                        };
                                    };
                                    
                                    groupRow.add(#Int(count));
                                };
                                case (null) {
                                    return #err(#ValidationError("Column not found: " # colName));
                                };
                            };
                        };
                        case (#Count(null)) {
                            // Count all rows
                            groupRow.add(#Int(groupRows.size()));
                        };
                        case (#Sum(colName)) {
                            // Sum numeric values
                            let colIdx = findColumnIndex(input.columns, colName);
                            
                            switch (colIdx) {
                                case (?idx) {
                                    var sum = 0;
                                    var hasValues = false;
                                    
                                    for (row in groupRows.vals()) {
                                        if (idx < row.size()) {
                                            switch (row[idx]) {
                                                case (#Int(i)) {
                                                    sum += i;
                                                    hasValues := true;
                                                };
                                                case _ {
                                                    // Skip non-numeric values
                                                };
                                            };
                                        };
                                    };
                                    
                                    if (hasValues) {
                                        groupRow.add(#Int(sum));
                                    } else {
                                        groupRow.add(#Null);
                                    };
                                };
                                case (null) {
                                    return #err(#ValidationError("Column not found: " # colName));
                                };
                            };
                        };
                        case (#Avg(colName)) {
                            // Average numeric values
                            let colIdx = findColumnIndex(input.columns, colName);
                            
                            switch (colIdx) {
                                case (?idx) {
                                    var sum = 0;
                                    var count = 0;
                                    
                                    for (row in groupRows.vals()) {
                                        if (idx < row.size()) {
                                            switch (row[idx]) {
                                                case (#Int(i)) {
                                                    sum += i;
                                                    count += 1;
                                                };
                                                case _ {
                                                    // Skip non-numeric values
                                                };
                                            };
                                        };
                                    };
                                    
                                    if (count > 0) {
                                        groupRow.add(#Float(Float.fromInt(sum) / Float.fromInt(count)));
                                    } else {
                                        groupRow.add(#Null);
                                    };
                                };
                                case (null) {
                                    return #err(#ValidationError("Column not found: " # colName));
                                };
                            };
                        };
                        case (#Min(colName)) {
                            // Find minimum value
                            let colIdx = findColumnIndex(input.columns, colName);
                            
                            switch (colIdx) {
                                case (?idx) {
                                    var minValue : ?Value = null;
                                    
                                    for (row in groupRows.vals()) {
                                        if (idx < row.size() and row[idx] != #Null) {
                                            switch (minValue) {
                                                case (null) {
                                                    minValue := ?row[idx];
                                                };
                                                case (?curMin) {
                                                    if (Utils.compareValues(row[idx], curMin) == #LT) {
                                                        minValue := ?row[idx];
                                                    };
                                                };
                                            };
                                        };
                                    };
                                    
                                    groupRow.add(Option.get(minValue, #Null));
                                };
                                case (null) {
                                    return #err(#ValidationError("Column not found: " # colName));
                                };
                            };
                        };
                        case (#Max(colName)) {
                            // Find maximum value
                            let colIdx = findColumnIndex(input.columns, colName);
                            
                            switch (colIdx) {
                                case (?idx) {
                                    var maxValue : ?Value = null;
                                    
                                    for (row in groupRows.vals()) {
                                        if (idx < row.size() and row[idx] != #Null) {
                                            switch (maxValue) {
                                                case (null) {
                                                    maxValue := ?row[idx];
                                                };
                                                case (?curMax) {
                                                    if (Utils.compareValues(row[idx], curMax) == #GT) {
                                                        maxValue := ?row[idx];
                                                    };
                                                };
                                            };
                                        };
                                    };
                                    
                                    groupRow.add(Option.get(maxValue, #Null));
                                };
                                case (null) {
                                    return #err(#ValidationError("Column not found: " # colName));
                                };
                            };
                        };
                    };
                };
                
                resultRows.add(Buffer.toArray(groupRow));
            };
            
            // Build result columns
            let resultColumns = Buffer.Buffer<Text>(0);
            
            // Add group by columns
            for (col in groupBy.vals()) {
                resultColumns.add(col);
            };
            
            // Add aggregate columns
            for (op in operations.vals()) {
                switch (op) {
                    case (#Count(?colName)) {
                        resultColumns.add("COUNT(" # colName # ")");
                    };
                    case (#Count(null)) {
                        resultColumns.add("COUNT(*)");
                    };
                    case (#Sum(colName)) {
                        resultColumns.add("SUM(" # colName # ")");
                    };
                    case (#Avg(colName)) {
                        resultColumns.add("AVG(" # colName # ")");
                    };
                    case (#Min(colName)) {
                        resultColumns.add("MIN(" # colName # ")");
                    };
                    case (#Max(colName)) {
                        resultColumns.add("MAX(" # colName # ")");
                    };
                };
            };
            
            // Return aggregated result
            #ok({
                columns = Buffer.toArray(resultColumns);
                rows = Buffer.toArray(resultRows);
                rowCount = resultRows.size();
            })
        } else {
            // Simple aggregation without grouping
            let resultRow = Buffer.Buffer<Value>(operations.size());
            let resultColumns = Buffer.Buffer<Text>(operations.size());
            
            for (op in operations.vals()) {
                switch (op) {
                    case (#Count(?colName)) {
                        let colIdx = findColumnIndex(input.columns, colName);
                        
                        switch (colIdx) {
                            case (?idx) {
                                var count = 0;
                                
                                for (row in input.rows.vals()) {
                                    if (idx < row.size() and row[idx] != #Null) {
                                        count += 1;
                                    };
                                };
                                
                                resultRow.add(#Int(count));
                                resultColumns.add("COUNT(" # colName # ")");
                            };
                            case (null) {
                                return #err(#ValidationError("Column not found: " # colName));
                            };
                        };
                    };
                    case (#Count(null)) {
                        resultRow.add(#Int(input.rowCount));
                        resultColumns.add("COUNT(*)");
                    };
                    // Similar implementations for other aggregates...
                };
            };
            
            // Return aggregated result
            #ok({
                columns = Buffer.toArray(resultColumns);
                rows = [Buffer.toArray(resultRow)];
                rowCount = 1;
            })
        };
    };
    
    /// Execute a sort operation
    public func executeSort(
        input : ResultSet,
        columns : [(Text, {#Ascending; #Descending})]
    ) : QueryResult<ResultSet> {
        if (input.rowCount <= 1 or columns.size() == 0) {
            return #ok(input); // No sorting needed
        };
        
        // Find column indices for sorting
        let sortIndices = Buffer.Buffer<(Nat, Bool)>(columns.size()); // (column index, is ascending)
        
        for ((colName, direction) in columns.vals()) {
            let colIdx = findColumnIndex(input.columns, colName);
            
            switch (colIdx) {
                case (?idx) {
                    sortIndices.add((idx, direction == #Ascending));
                };
                case (null) {
                    return #err(#ValidationError("Sort column not found: " # colName));
                };
            };
        };
        
        // Create row indices for sorting
        let indices = Array.tabulate<Nat>(input.rowCount, func(i) { i });
        
        // Sort indices based on row values
        let sortedIndices = Array.sort<Nat>(
            indices,
            func(a, b) {
                for ((colIdx, isAscending) in sortIndices.vals()) {
                    // Get values for comparison
                    let aVal = if (colIdx < input.rows[a].size()) { input.rows[a][colIdx] } else { #Null };
                    let bVal = if (colIdx < input.rows[b].size()) { input.rows[b][colIdx] } else { #Null };
                    
                    // Compare values
                    let comparison = Utils.compareValues(aVal, bVal);
                    
                    if (comparison != #EQ) {
                        if (isAscending) {
                            // Ascending order
                            return switch (comparison) {
                                case (#LT) { #less };
                                case (#GT) { #greater };
                                case _ { #equal };
                            };
                        } else {
                            // Descending order
                            return switch (comparison) {
                                case (#LT) { #greater };
                                case (#GT) { #less };
                                case _ { #equal };
                            };
                        };
                    };
                };
                
                // If all columns are equal, maintain original order
                if (a < b) { #less } else { #greater }
            }
        );
        
        // Create sorted rows
        let sortedRows = Array.map<Nat, [Value]>(
            sortedIndices,
            func(idx) { input.rows[idx] }
        );
        
        // Return sorted result
        #ok({
            columns = input.columns;
            rows = sortedRows;
            rowCount = sortedRows.size();
        })
    };
    
    /// Execute a limit operation
    public func executeLimit(
        input : ResultSet,
        count : Nat,
        offset : Nat
    ) : QueryResult<ResultSet> {
        // Calculate effective range
        let startIdx = Nat.min(offset, input.rowCount);
        let endIdx = Nat.min(offset + count, input.rowCount);
        
        if (startIdx >= endIdx) {
            // Empty result
            return #ok({
                columns = input.columns;
                rows = [];
                rowCount = 0;
            });
        };
        
        // Apply limit and offset
        let limitedRows = Array.tabulate<[Value]>(
            endIdx - startIdx,
            func(i) { input.rows[startIdx + i] }
        );
        
        // Return limited result
        #ok({
            columns = input.columns;
            rows = limitedRows;
            rowCount = limitedRows.size();
        })
    };
    
    /// Execute a projection operation
    public func executeProject(
        input : ResultSet,
        columns : [Text]
    ) : QueryResult<ResultSet> {
        if (columns.size() == 0) {
            return #err(#ValidationError("No columns specified for projection"));
        };
        
        // Handle special case: SELECT *
        if (columns.size() == 1 and columns[0] == "*") {
            return #ok(input);
        };
        
        // Find column indices for projection
        let projectionIndices = Buffer.Buffer<Nat>(columns.size());
        
        for (colName in columns.vals()) {
            let colIdx = findColumnIndex(input.columns, colName);
            
            switch (colIdx) {
                case (?idx) {
                    projectionIndices.add(idx);
                };
                case (null) {
                    return #err(#ValidationError("Projection column not found: " # colName));
                };
            };
        };
        
        // Project columns for each row
        let projectedRows = Array.map<[Value], [Value]>(
            input.rows,
            func(row) {
                Array.map<Nat, Value>(
                    Buffer.toArray(projectionIndices),
                    func(idx) {
                        if (idx < row.size()) {
                            row[idx]
                        } else {
                            #Null
                        }
                    }
                )
            }
        );
        
        // Return projected result
        #ok({
            columns = columns;
            rows = projectedRows;
            rowCount = projectedRows.size();
        })
    };
    
    // ===== Helper Functions =====
    
    /// Apply a filter to a table
    public func applyFilter(
        table : TableStorage,
        filter : ?FilterExpression
    ) : FilterResult {
        // Without a filter, match all rows
        if (filter == null) {
            let indices = Array.tabulate<Nat>(table.records.size(), func(i) { i });
            return #Match(indices);
        };
        
        // Check if an index can be used for this filter
        switch (filter) {
            case (?f) {
                // Try index lookup
                for (index in table.indexes.vals()) {
                    let indexResult = tryIndexLookup(table, index, f);
                    
                    switch (indexResult) {
                        case (#Match(indices)) {
                            return #Match(indices);
                        };
                        case (#NoMatch) {
                            return #NoMatch;
                        };
                        case (#NotIndexed) {
                            // Continue to next index
                        };
                        case (#Error(msg)) {
                            return #Error(msg);
                        };
                    };
                };
                
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
            case (null) {
                let indices = Array.tabulate<Nat>(table.records.size(), func(i) { i });
                #Match(indices)
            };
        };
    };
    
    /// Try to use an index for a filter
    private func tryIndexLookup(
        table : TableStorage,
        index : IndexStorage,
        filter : FilterExpression
    ) : IndexLookupResult {
        // Only handle simple equality on indexed columns
        switch (filter) {
            case (#Equals(column, value)) {
                if (index.columns.size() == 1 and index.columns[0] == column) {
                    // Find entries for this value
                    for ((key, indices) in index.entries.vals()) {
                        if (Utils.compareValues(key, value) == #EQ) {
                            return #Match(indices);
                        };
                    };
                    
                    // No match found
                    return #NoMatch;
                };
            };
            case _ {
                // More complex filters not handled by this simple index lookup
            };
        };
        
        #NotIndexed
    };
    
    /// Use an index for a general lookup
    private func lookupWithIndex(
        table : TableStorage,
        index : IndexStorage,
        filter : ?FilterExpression
    ) : IndexLookupResult {
        switch (filter) {
            case (null) {
                // No filter, return all rows
                let allIndices = Buffer.Buffer<Nat>(0);
                
                for ((_, indices) in index.entries.vals()) {
                    for (idx in indices.vals()) {
                        allIndices.add(idx);
                    };
                };
                
                if (allIndices.size() > 0) {
                    #Match(Buffer.toArray(allIndices))
                } else {
                    #NoMatch
                }
            };
            case (?f) {
                tryIndexLookup(table, index, f)
            };
        };
    };
    
    /// Evaluate a filter expression against a record
    public func evaluateFilter(record : Record, filter : FilterExpression) : Bool {
        switch (filter) {
            case (#Equals(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        Utils.compareValues(recordValue, value) == #EQ
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#NotEquals(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        Utils.compareValues(recordValue, value) != #EQ
                    };
                    case (null) {
                        true
                    };
                };
            };
            case (#GreaterThan(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        Utils.compareValues(recordValue, value) == #GT
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#LessThan(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        Utils.compareValues(recordValue, value) == #LT
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#GreaterThanOrEqual(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        let comparison = Utils.compareValues(recordValue, value);
                        comparison == #GT or comparison == #EQ
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#LessThanOrEqual(column, value)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        let comparison = Utils.compareValues(recordValue, value);
                        comparison == #LT or comparison == #EQ
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#Like(column, pattern)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?#Text(text)) {
                        // Simple pattern matching with % wildcard
                        // This is a very simplified implementation
                        if (pattern == "%") return true;
                        
                        let startsWithWildcard = Text.startsWith(pattern, #text "%");
                        let endsWithWildcard = Text.endsWith(pattern, #text "%");
                        
                        if (startsWithWildcard and endsWithWildcard) {
                            let substr = Text.trim(pattern, #text "%");
                            Text.contains(text, #text substr)
                        } else if (startsWithWildcard) {
                            let suffix = Text.trimStart(pattern, #text "%");
                            Text.endsWith(text, #text suffix)
                        } else if (endsWithWildcard) {
                            let prefix = Text.trimEnd(pattern, #text "%");
                            Text.startsWith(text, #text prefix)
                        } else {
                            text == pattern
                        }
                    };
                    case _ {
                        false
                    };
                };
            };
            case (#Between(column, lower, upper)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        let greaterThanLower = Utils.compareValues(recordValue, lower) != #LT;
                        let lessThanUpper = Utils.compareValues(recordValue, upper) != #GT;
                        
                        greaterThanLower and lessThanUpper
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#In(column, values)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?recordValue) {
                        Array.some<Value>(
                            values,
                            func(v) { Utils.compareValues(recordValue, v) == #EQ }
                        )
                    };
                    case (null) {
                        false
                    };
                };
            };
            case (#IsNull(column)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?#Null) { true };
                    case (null) { true };
                    case _ { false };
                };
            };
            case (#IsNotNull(column)) {
                switch (Utils.getRecordValue(record, column)) {
                    case (?#Null) { false };
                    case (null) { false };
                    case _ { true };
                };
            };
            case (#And(conditions)) {
                Array.all<FilterExpression>(
                    conditions,
                    func(c) { evaluateFilter(record, c) }
                )
            };
            case (#Or(conditions)) {
                Array.some<FilterExpression>(
                    conditions,
                    func(c) { evaluateFilter(record, c) }
                )
            };
            case (#Not(condition)) {
                not evaluateFilter(record, condition)
            };
        };
    };
    
    /// Get column names from a table
    private func getColumnNames(table : TableStorage) : [Text] {
        Array.map<CommonTypes.ColumnDefinition, Text>(
            table.schema.columns,
            func(col) { col.name }
        )
    };
    
    /// Create a result set from matching row indices
    private func createResultSetFromIndices(
        table : TableStorage,
        indices : [Nat]
    ) : QueryResult<ResultSet> {
        let columnNames = getColumnNames(table);
        let rows = Buffer.Buffer<[Value]>(indices.size());
        
        for (idx in indices.vals()) {
            if (idx < table.records.size()) {
                let record = table.records[idx];
                
                // Extract values in column order
                let row = Array.map<Text, Value>(
                    columnNames,
                    func(colName) {
                        switch (Utils.getRecordValue(record, colName)) {
                            case (?value) { value };
                            case (null) { #Null };
                        }
                    }
                );
                
                rows.add(row);
            };
        };
        
        #ok({
            columns = columnNames;
            rows = Buffer.toArray(rows);
            rowCount = rows.size();
        })
    };
    
    /// Update a record with new values
    private func updateRecord(record : Record, updates : [(Text, Value)]) : Record {
        var updatedRecord = record;
        
        for ((column, value) in updates.vals()) {
            updatedRecord := Utils.setRecordValue(updatedRecord, column, value);
        };
        
        updatedRecord
    };
    
    /// Find the index of a column in a result set
    private func findColumnIndex(columns : [Text], columnName : Text) : ?Nat {
        for (i in Iter.range(0, columns.size() - 1)) {
            if (columns[i] == columnName) {
                return ?i;
            };
        };
        
        null
    };
}