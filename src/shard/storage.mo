/**
 * Storage management module for Shard canister
 * 
 * This module is responsible for handling data storage within a shard,
 * including record management, index maintenance, and storage optimization.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import HashMap "mo:base/HashMap";
import Hash "mo:base/Hash";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Text "mo:base/Text";
import TrieMap "mo:base/TrieMap";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";
import ShardTypes "./types";

module {
    // ===== Type Aliases =====
    
    type Value = CommonTypes.Value;
    type Record = CommonTypes.Record;
    type TableSchema = CommonTypes.TableSchema;
    type ColumnDefinition = CommonTypes.ColumnDefinition;
    type IndexDefinition = CommonTypes.IndexDefinition;
    type Key = CommonTypes.Key;
    
    type TableStorage = ShardTypes.TableStorage;
    type IndexStorage = ShardTypes.IndexStorage;
    type InsertResult = ShardTypes.InsertResult;
    type UpdateResult = ShardTypes.UpdateResult;
    type DeleteResult = ShardTypes.DeleteResult;
    
    // ===== Record Storage Functions =====
    
    /// Insert records into a table
    public func insertRecords(
        table : TableStorage,
        records : [Record]
    ) : Result.Result<InsertResult, Text> {
        if (records.size() == 0) {
            return #ok({
                tableName = table.name;
                insertedCount = 0;
                newRecords = [];
            });
        };
        
        // Validate records against schema
        for (record in records.vals()) {
            let validationResult = validateRecord(record, table.schema);
            
            switch (validationResult) {
                case (#err(e)) {
                    return #err(e);
                };
                case (#ok()) {
                    // Record is valid, continue
                };
            };
        };
        
        // Check unique constraints
        for (record in records.vals()) {
            let uniqueResult = checkUniqueConstraints(table, record, null);
            
            switch (uniqueResult) {
                case (#err(e)) {
                    return #err(e);
                };
                case (#ok()) {
                    // Constraints satisfied, continue
                };
            };
        };
        
        // Insert records
        let startIndex = table.records.size();
        let insertedRecords = Buffer.Buffer<Record>(records.size());
        
        for (record in records.vals()) {
            // Fill in default values for missing columns
            let completeRecord = fillDefaultValues(record, table.schema);
            insertedRecords.add(completeRecord);
        };
        
        // Update table records
        table.records := Array.append(table.records, Buffer.toArray(insertedRecords));
        
        // Update indexes
        for (i in Iter.range(0, insertedRecords.size() - 1)) {
            let record = insertedRecords.get(i);
            let rowIndex = startIndex + i;
            
            updateIndexesAfterInsert(table, record, rowIndex);
        };
        
        #ok({
            tableName = table.name;
            insertedCount = insertedRecords.size();
            newRecords = Buffer.toArray(insertedRecords);
        })
    };
    
    /// Update records in a table
    public func updateRecords(
        table : TableStorage,
        updates : [(Text, Value)],
        rowIndices : [Nat]
    ) : Result.Result<UpdateResult, Text> {
        if (updates.size() == 0 or rowIndices.size() == 0) {
            return #ok({
                tableName = table.name;
                updatedCount = 0;
                updatedIndices = [];
            });
        };
        
        // Validate column names
        for ((colName, _) in updates.vals()) {
            let colExists = Array.some<ColumnDefinition>(
                table.schema.columns,
                func(col) { col.name == colName }
            );
            
            if (not colExists) {
                return #err("Column not found: " # colName);
            };
        };
        
        // Update records
        let updatedIndices = Buffer.Buffer<Nat>(0);
        var updatedRecords = Array.tabulate<Record>(
            table.records.size(),
            func(i) { table.records[i] }
        );
        
        for (rowIndex in rowIndices.vals()) {
            if (rowIndex < table.records.size()) {
                let oldRecord = table.records[rowIndex];
                let updatedRecord = updateRecordFields(oldRecord, updates);
                
                // Validate updated record
                let validationResult = validateRecord(updatedRecord, table.schema);
                
                switch (validationResult) {
                    case (#err(e)) {
                        return #err("Validation error for row " # Nat.toText(rowIndex) # ": " # e);
                    };
                    case (#ok()) {
                        // Record is valid, continue
                    };
                };
                
                // Check unique constraints
                let uniqueResult = checkUniqueConstraints(table, updatedRecord, ?rowIndex);
                
                switch (uniqueResult) {
                    case (#err(e)) {
                        return #err("Constraint violation for row " # Nat.toText(rowIndex) # ": " # e);
                    };
                    case (#ok()) {
                        // Constraints satisfied, continue
                    };
                };
                
                // Apply update
                updatedRecords[rowIndex] := updatedRecord;
                updatedIndices.add(rowIndex);
                
                // Update indexes
                updateIndexesAfterUpdate(table, oldRecord, updatedRecord, rowIndex);
            };
        };
        
        // Update table records
        table.records := updatedRecords;
        
        #ok({
            tableName = table.name;
            updatedCount = updatedIndices.size();
            updatedIndices = Buffer.toArray(updatedIndices);
        })
    };
    
    /// Delete records from a table
    public func deleteRecords(
        table : TableStorage,
        rowIndices : [Nat]
    ) : Result.Result<DeleteResult, Text> {
        if (rowIndices.size() == 0) {
            return #ok({
                tableName = table.name;
                deletedCount = 0;
                deletedIndices = [];
            });
        };
        
        // Sort indices in descending order to avoid shifting issues
        let sortedIndices = Array.sort<Nat>(
            rowIndices,
            func(a, b) { Nat.compare(b, a) } // Descending order
        );
        
        // Delete records
        let deletedIndices = Buffer.Buffer<Nat>(0);
        var remainingRecords = Array.tabulate<Record>(
            table.records.size(),
            func(i) { table.records[i] }
        );
        
        for (rowIndex in sortedIndices.vals()) {
            if (rowIndex < remainingRecords.size()) {
                // Remove record from indexes
                updateIndexesBeforeDelete(table, remainingRecords[rowIndex], rowIndex);
                
                // Remove record
                let before = Array.tabulate<Record>(rowIndex, func(i) { remainingRecords[i] });
                let after = Array.tabulate<Record>(
                    remainingRecords.size() - rowIndex - 1,
                    func(i) { remainingRecords[rowIndex + 1 + i] }
                );
                
                remainingRecords := Array.append(before, after);
                deletedIndices.add(rowIndex);
                
                // Adjust indices of subsequent records in all indexes
                updateIndexesAfterDeleteShift(table, rowIndex);
            };
        };
        
        // Update table records
        table.records := remainingRecords;
        
        #ok({
            tableName = table.name;
            deletedCount = deletedIndices.size();
            deletedIndices = Buffer.toArray(deletedIndices);
        })
    };
    
    // ===== Index Management =====
    
    /// Build an index for a table
    public func buildIndex(
        table : TableStorage,
        indexDef : IndexDefinition
    ) : Result.Result<(), Text> {
        // Validate index definition
        let validationResult = validateIndexDefinition(indexDef, table.schema);
        
        switch (validationResult) {
            case (#err(e)) {
                return #err(e);
            };
            case (#ok()) {
                // Index definition is valid, continue
            };
        };
        
        // Create index
        let index : IndexStorage = {
            name = indexDef.name;
            indexType = indexDef.indexType;
            columns = indexDef.columns;
            unique = indexDef.unique;
            var entries = [];
        };
        
        // Populate index with existing data
        let indexEntries = HashMap.HashMap<Key, Buffer.Buffer<Nat>>(10, 
            equalKey, 
            hashKey
        );
        
        for (i in Iter.range(0, table.records.size() - 1)) {
            let record = table.records[i];
            let key = extractIndexKey(record, indexDef.columns);
            
            switch (indexEntries.get(key)) {
                case (null) {
                    let buffer = Buffer.Buffer<Nat>(1);
                    buffer.add(i);
                    indexEntries.put(key, buffer);
                };
                case (?buffer) {
                    // Check uniqueness constraint
                    if (indexDef.unique and buffer.size() > 0) {
                        return #err("Uniqueness violation in existing data for index: " # indexDef.name);
                    };
                    
                    buffer.add(i);
                };
            };
        };
        
        // Convert hashmap to array for storage
        let entries = Buffer.Buffer<(Key, [Nat])>(indexEntries.size());
        
        for ((key, buffer) in indexEntries.entries()) {
            entries.add((key, Buffer.toArray(buffer)));
        };
        
        // Update index entries
        index.entries := Buffer.toArray(entries);
        
        // Add index to table
        table.indexes := Array.append(table.indexes, [index]);
        
        #ok()
    };
    
    /// Rebuild all indexes for a table
    public func rebuildIndexes(
        table : TableStorage
    ) : Result.Result<(), Text> {
        if (table.indexes.size() == 0) {
            return #ok(); // No indexes to rebuild
        };
        
        // Rebuild each index
        for (i in Iter.range(0, table.indexes.size() - 1)) {
            let index = table.indexes[i];
            
            // Create index definition
            let indexDef : IndexDefinition = {
                name = index.name;
                table = table.name;
                columns = index.columns;
                unique = index.unique;
                indexType = index.indexType;
            };
            
            // Remove existing entries
            let emptyIndex = {
                name = index.name;
                indexType = index.indexType;
                columns = index.columns;
                unique = index.unique;
                var entries = [];
            };
            
            table.indexes[i] := emptyIndex;
            
            // Rebuild index
            let result = buildIndex(table, indexDef);
            
            switch (result) {
                case (#err(e)) {
                    return #err("Failed to rebuild index '" # index.name # "': " # e);
                };
                case (#ok()) {
                    // Index rebuilt successfully
                };
            };
        };
        
        #ok()
    };
    
    /// Drop an index from a table
    public func dropIndex(
        table : TableStorage,
        indexName : Text
    ) : Result.Result<(), Text> {
        let indexOpt = Array.find<IndexStorage>(
            table.indexes,
            func(idx) { idx.name == indexName }
        );
        
        switch (indexOpt) {
            case (null) {
                return #err("Index not found: " # indexName);
            };
            case (?_) {
                // Remove index
                table.indexes := Array.filter<IndexStorage>(
                    table.indexes,
                    func(idx) { idx.name != indexName }
                );
                
                #ok()
            };
        };
    };
    
    // ===== Storage Management =====
    
    /// Estimate storage usage for a table
    public func estimateTableSize(table : TableStorage) : Nat {
        var size = 0;
        
        // Estimate records size
        for (record in table.records.vals()) {
            size += estimateRecordSize(record);
        };
        
        // Estimate indexes size
        for (index in table.indexes.vals()) {
            size += estimateIndexSize(index);
        };
        
        size
    };
    
    /// Compact table storage to reduce memory usage
    public func compactTable(table : TableStorage) : Result.Result<Nat, Text> {
        let initialSize = estimateTableSize(table);
        
        // Rebuild indexes to remove any bloat
        let indexResult = rebuildIndexes(table);
        
        switch (indexResult) {
            case (#err(e)) {
                return #err("Failed to rebuild indexes during compaction: " # e);
            };
            case (#ok()) {
                // Indexes rebuilt successfully
            };
        };
        
        // Remove any empty slots in records array
        let validRecords = Array.filter<Record>(
            table.records,
            func(r) { r.size() > 0 }
        );
        
        // Update table records
        table.records := validRecords;
        
        // Calculate space saved
        let finalSize = estimateTableSize(table);
        let spaceReduced = if (initialSize > finalSize) {
            initialSize - finalSize
        } else {
            0
        };
        
        #ok(spaceReduced)
    };
    
    // ===== Schema Validation =====
    
    /// Validate a record against a table schema
    public func validateRecord(
        record : Record,
        schema : TableSchema
    ) : Result.Result<(), Text> {
        // Check primary key constraints
        if (schema.primaryKey.size() > 0) {
            for (pkCol in schema.primaryKey.vals()) {
                let hasValue = Option.isSome(Utils.getRecordValue(record, pkCol));
                if (not hasValue) {
                    return #err("Missing primary key value for column: " # pkCol);
                };
            };
        };
        
        // Check column constraints
        for (col in schema.columns.vals()) {
            let value = Utils.getRecordValue(record, col.name);
            
            // Check NOT NULL constraint
            let hasNotNull = Array.some<CommonTypes.Constraint>(
                col.constraints,
                func(c) { c == #NotNull }
            );
            
            if (hasNotNull or not col.nullable) {
                if (Option.isNull(value) or value == ?#Null) {
                    return #err("NOT NULL constraint violation for column: " # col.name);
                };
            };
            
            // Check data type for non-null values
            switch (value) {
                case (?v) {
                    if (v != #Null) {
                        let typeResult = validateValueType(v, col.dataType);
                        
                        switch (typeResult) {
                            case (#err(e)) {
                                return #err("Type mismatch for column '" # col.name # "': " # e);
                            };
                            case (#ok()) {
                                // Type matches, continue
                            };
                        };
                    };
                };
                case (null) {
                    // Null value already checked above
                };
            };
            
            // Check other constraints (like CHECK)
            for (constraint in col.constraints.vals()) {
                switch (constraint) {
                    case (#Check(predicate)) {
                        // In a real implementation, we'd evaluate the predicate
                        // This is a simplified version that doesn't support CHECK constraints yet
                    };
                    case _ {
                        // Other constraints handled elsewhere
                    };
                };
            };
        };
        
        #ok()
    };
    
    /// Validate that a value matches the expected data type
    public func validateValueType(
        value : Value,
        dataType : CommonTypes.DataType
    ) : Result.Result<(), Text> {
        switch (value, dataType) {
            case (#Int(_), #Int) { #ok() };
            case (#Float(_), #Float) { #ok() };
            case (#Text(_), #Text) { #ok() };
            case (#Bool(_), #Bool) { #ok() };
            case (#Blob(_), #Blob) { #ok() };
            case (#Null, _) { #ok() }; // Null can be any type
            case (#Array(items), #Array(itemType)) {
                // Check each item in array
                for (item in items.vals()) {
                    let itemResult = validateValueType(item, itemType);
                    switch (itemResult) {
                        case (#err(e)) {
                            return #err("Array item type mismatch: " # e);
                        };
                        case (#ok()) {
                            // Type matches, continue
                        };
                    };
                };
                #ok()
            };
            case (#Record(fields), #Record(fieldTypes)) {
                // Check each field in record
                for (fieldType in fieldTypes.vals()) {
                    let fieldValue = Utils.getRecordValue(fields, fieldType.name);
                    
                    switch (fieldValue) {
                        case (?v) {
                            let fieldResult = validateValueType(v, fieldType.fieldType);
                            switch (fieldResult) {
                                case (#err(e)) {
                                    return #err("Field '" # fieldType.name # "' type mismatch: " # e);
                                };
                                case (#ok()) {
                                    // Type matches, continue
                                };
                            };
                        };
                        case (null) {
                            // Missing field, which might be OK depending on nullability
                        };
                    };
                };
                #ok()
            };
            case (#Variant(label, optValue), #Variant(variantTypes)) {
                // Find matching variant
                let variantType = Array.find<CommonTypes.VariantType>(
                    variantTypes,
                    func(vt) { vt.name == label }
                );
                
                switch (variantType) {
                    case (?vt) {
                        switch (optValue, vt.variantType) {
                            case (null, null) {
                                #ok()
                            };
                            case (?value, ?expectedType) {
                                validateValueType(value, expectedType)
                            };
                            case (null, ?_) {
                                #err("Variant '" # label # "' missing value")
                            };
                            case (?_, null) {
                                #err("Variant '" # label # "' should not have value")
                            };
                        };
                    };
                    case (null) {
                        #err("Variant type '" # label # "' not found in schema")
                    };
                };
            };
            case (#Optional(value), #Optional(type)) {
                validateValueType(value, type)
            };
            case _ {
                #err("Type mismatch: expected " # debug_show(dataType) # ", got " # debug_show(value))
            };
        };
    };
    
    /// Validate an index definition
    public func validateIndexDefinition(
        index : IndexDefinition,
        schema : TableSchema
    ) : Result.Result<(), Text> {
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
                schema.columns,
                func(col) { col.name == colName }
            );
            
            if (not colExists) {
                return #err("Column not found: " # colName);
            };
        };
        
        #ok()
    };
    
    /// Check if a record satisfies unique constraints
    public func checkUniqueConstraints(
        table : TableStorage,
        record : Record,
        excludeIndex : ?Nat
    ) : Result.Result<(), Text> {
        // Check primary key constraints
        if (table.schema.primaryKey.size() > 0) {
            // Extract primary key value
            let pkValues = Buffer.Buffer<(Text, Value)>(table.schema.primaryKey.size());
            
            for (pkCol in table.schema.primaryKey.vals()) {
                switch (Utils.getRecordValue(record, pkCol)) {
                    case (?value) {
                        pkValues.add((pkCol, value));
                    };
                    case (null) {
                        return #err("Missing primary key value for column: " # pkCol);
                    };
                };
            };
            
            // Check if any existing record has the same PK
            for (i in Iter.range(0, table.records.size() - 1)) {
                // Skip the record being updated
                if (excludeIndex == ?i) {
                    continue;
                };
                
                let existingRecord = table.records[i];
                var matches = true;
                
                for ((col, value) in Buffer.toArray(pkValues).vals()) {
                    switch (Utils.getRecordValue(existingRecord, col)) {
                        case (?existingValue) {
                            if (Utils.compareValues(value, existingValue) != #EQ) {
                                matches := false;
                                break;
                            };
                        };
                        case (null) {
                            matches := false;
                            break;
                        };
                    };
                };
                
                if (matches) {
                    return #err("Primary key violation: duplicate key value");
                };
            };
        };
        
        // Check unique index constraints
        for (index in table.indexes.vals()) {
            if (index.unique) {
                // Extract key for this index
                let key = extractIndexKey(record, index.columns);
                
                // Check if any existing record has the same key
                for ((entryKey, rowIndices) in index.entries.vals()) {
                    if (Utils.compareValues(key, entryKey) == #EQ) {
                        // Found matching key, check if it's not our excluded record
                        for (rowIdx in rowIndices.vals()) {
                            if (excludeIndex != ?rowIdx) {
                                return #err("Unique constraint violation on index: " # index.name);
                            };
                        };
                    };
                };
            };
        };
        
        #ok()
    };
    
    // ===== Helper Functions =====
    
    /// Update indexes after inserting a record
    private func updateIndexesAfterInsert(
        table : TableStorage,
        record : Record,
        rowIndex : Nat
    ) {
        for (i in Iter.range(0, table.indexes.size() - 1)) {
            let index = table.indexes[i];
            let key = extractIndexKey(record, index.columns);
            
            // Find existing entry for this key
            var found = false;
            var updatedEntries = Array.map<(Key, [Nat]), (Key, [Nat])>(
                index.entries,
                func(entry) {
                    let (entryKey, rowIndices) = entry;
                    
                    if (Utils.compareValues(entryKey, key) == #EQ) {
                        found := true;
                        (entryKey, Array.append(rowIndices, [rowIndex]))
                    } else {
                        entry
                    };
                }
            );
            
            // If no existing entry, add a new one
            if (not found) {
                updatedEntries := Array.append(updatedEntries, [(key, [rowIndex])]);
            };
            
            // Update the index
            let updatedIndex = {
                name = index.name;
                indexType = index.indexType;
                columns = index.columns;
                unique = index.unique;
                var entries = updatedEntries;
            };
            
            table.indexes[i] := updatedIndex;
        };
    };
    
    /// Update indexes after updating a record
    private func updateIndexesAfterUpdate(
        table : TableStorage,
        oldRecord : Record,
        newRecord : Record,
        rowIndex : Nat
    ) {
        for (i in Iter.range(0, table.indexes.size() - 1)) {
            let index = table.indexes[i];
            let oldKey = extractIndexKey(oldRecord, index.columns);
            let newKey = extractIndexKey(newRecord, index.columns);
            
            // If key hasn't changed, no need to update index
            if (Utils.compareValues(oldKey, newKey) == #EQ) {
                continue;
            };
            
            // Remove entry for old key
            var updatedEntries = Array.map<(Key, [Nat]), (Key, [Nat])>(
                index.entries,
                func(entry) {
                    let (entryKey, rowIndices) = entry;
                    
                    if (Utils.compareValues(entryKey, oldKey) == #EQ) {
                        // Remove rowIndex from indices
                        let filteredIndices = Array.filter<Nat>(
                            rowIndices,
                            func(idx) { idx != rowIndex }
                        );
                        
                        if (filteredIndices.size() > 0) {
                            (entryKey, filteredIndices)
                        } else {
                            // Empty entry, will be filtered out below
                            (entryKey, [])
                        }
                    } else {
                        entry
                    };
                }
            );
            
            // Remove empty entries
            updatedEntries := Array.filter<(Key, [Nat])>(
                updatedEntries,
                func(entry) {
                    let (_, rowIndices) = entry;
                    rowIndices.size() > 0
                }
            );
            
            // Add entry for new key
            var found = false;
            updatedEntries := Array.map<(Key, [Nat]), (Key, [Nat])>(
                updatedEntries,
                func(entry) {
                    let (entryKey, rowIndices) = entry;
                    
                    if (Utils.compareValues(entryKey, newKey) == #EQ) {
                        found := true;
                        (entryKey, Array.append(rowIndices, [rowIndex]))
                    } else {
                        entry
                    };
                }
            );
            
            // If no existing entry, add a new one
            if (not found) {
                updatedEntries := Array.append(updatedEntries, [(newKey, [rowIndex])]);
            };
            
            // Update the index
            let updatedIndex = {
                name = index.name;
                indexType = index.indexType;
                columns = index.columns;
                unique = index.unique;
                var entries = updatedEntries;
            };
            
            table.indexes[i] := updatedIndex;
        };
    };
    
    /// Update indexes before deleting a record
    private func updateIndexesBeforeDelete(
        table : TableStorage,
        record : Record,
        rowIndex : Nat
    ) {
        for (i in Iter.range(0, table.indexes.size() - 1)) {
            let index = table.indexes[i];
            let key = extractIndexKey(record, index.columns);
            
            // Remove entry for this key
            var updatedEntries = Array.map<(Key, [Nat]), (Key, [Nat])>(
                index.entries,
                func(entry) {
                    let (entryKey, rowIndices) = entry;
                    
                    if (Utils.compareValues(entryKey, key) == #EQ) {
                        // Remove rowIndex from indices
                        let filteredIndices = Array.filter<Nat>(
                            rowIndices,
                            func(idx) { idx != rowIndex }
                        );
                        
                        if (filteredIndices.size() > 0) {
                            (entryKey, filteredIndices)
                        } else {
                            // Empty entry, will be filtered out below
                            (entryKey, [])
                        }
                    } else {
                        entry
                    };
                }
            );
            
            // Remove empty entries
            updatedEntries := Array.filter<(Key, [Nat])>(
                updatedEntries,
                func(entry) {
                    let (_, rowIndices) = entry;
                    rowIndices.size() > 0
                }
            );
            
            // Update the index
            let updatedIndex = {
                name = index.name;
                indexType = index.indexType;
                columns = index.columns;
                unique = index.unique;
                var entries = updatedEntries;
            };
            
            table.indexes[i] := updatedIndex;
        };
    };
    
    /// Update indexes after deleting a record (shift row indices)
    private func updateIndexesAfterDeleteShift(
        table : TableStorage,
        deletedIndex : Nat
    ) {
        for (i in Iter.range(0, table.indexes.size() - 1)) {
            let index = table.indexes[i];
            
            // Adjust row indices
            let updatedEntries = Array.map<(Key, [Nat]), (Key, [Nat])>(
                index.entries,
                func(entry) {
                    let (entryKey, rowIndices) = entry;
                    
                    // Shift indices that are greater than deletedIndex
                    let adjustedIndices = Array.map<Nat, Nat>(
                        rowIndices,
                        func(idx) {
                            if (idx > deletedIndex) {
                                idx - 1
                            } else {
                                idx
                            }
                        }
                    );
                    
                    (entryKey, adjustedIndices)
                }
            );
            
            // Update the index
            let updatedIndex = {
                name = index.name;
                indexType = index.indexType;
                columns = index.columns;
                unique = index.unique;
                var entries = updatedEntries;
            };
            
            table.indexes[i] := updatedIndex;
        };
    };
    
    /// Extract a key for an index from a record
    private func extractIndexKey(record : Record, columns : [Text]) : Key {
        if (columns.size() == 1) {
            // Single column index
            let colName = columns[0];
            
            switch (Utils.getRecordValue(record, colName)) {
                case (?value) {
                    value
                };
                case (null) {
                    #Null
                };
            };
        } else {
            // Composite index
            let keyFields = Buffer.Buffer<(Text, Value)>(columns.size());
            
            for (colName in columns.vals()) {
                switch (Utils.getRecordValue(record, colName)) {
                    case (?value) {
                        keyFields.add((colName, value));
                    };
                    case (null) {
                        keyFields.add((colName, #Null));
                    };
                };
            };
            
            #Record(Buffer.toArray(keyFields))
        };
    };
    
    /// Fill in default values for missing columns
    private func fillDefaultValues(record : Record, schema : TableSchema) : Record {
        var filledRecord = record;
        
        for (col in schema.columns.vals()) {
            if (not Utils.recordHasKey(filledRecord, col.name)) {
                // Column missing, use default value if available
                switch (col.defaultValue) {
                    case (?defaultVal) {
                        filledRecord := Utils.setRecordValue(filledRecord, col.name, defaultVal);
                    };
                    case (null) {
                        if (col.nullable) {
                            filledRecord := Utils.setRecordValue(filledRecord, col.name, #Null);
                        };
                        // If not nullable and no default, validation will catch this
                    };
                };
            };
        };
        
        filledRecord
    };
    
    /// Update a record with new field values
    private func updateRecordFields(record : Record, updates : [(Text, Value)]) : Record {
        var updatedRecord = record;
        
        for ((column, value) in updates.vals()) {
            updatedRecord := Utils.setRecordValue(updatedRecord, column, value);
        };
        
        updatedRecord
    };
    
    /// Estimate the size of a record in bytes
    private func estimateRecordSize(record : Record) : Nat {
        var size = 0;
        
        for ((key, value) in record.vals()) {
            size += Text.size(key);
            size += estimateValueSize(value);
        };
        
        size
    };
    
    /// Estimate the size of a value in bytes
    private func estimateValueSize(value : Value) : Nat {
        switch (value) {
            case (#Int(_)) { 8 };
            case (#Float(_)) { 8 };
            case (#Text(t)) { Text.size(t) };
            case (#Bool(_)) { 1 };
            case (#Blob(b)) { b.size() };
            case (#Null) { 1 };
            case (#Array(arr)) {
                var size = 0;
                for (item in arr.vals()) {
                    size += estimateValueSize(item);
                };
                size + 8 // Array overhead
            };
            case (#Record(fields)) {
                var size = 0;
                for ((key, val) in fields.vals()) {
                    size += Text.size(key) + estimateValueSize(val);
                };
                size + 8 // Record overhead
            };
            case (#Variant(label, val)) {
                var size = Text.size(label);
                switch (val) {
                    case (?v) { size += estimateValueSize(v) };
                    case (null) { };
                };
                size + 4 // Variant overhead
            };
        };
    };
    
    /// Estimate the size of an index in bytes
    private func estimateIndexSize(index : IndexStorage) : Nat {
        var size = Text.size(index.name);
        
        for (col in index.columns.vals()) {
            size += Text.size(col);
        };
        
        for ((key, rowIndices) in index.entries.vals()) {
            size += estimateValueSize(key);
            size += rowIndices.size() * 4; // 4 bytes per row index
        };
        
        size
    };
    
    /// Key equality function for index entries
    private func equalKey(k1 : Key, k2 : Key) : Bool {
        Utils.compareValues(k1, k2) == #EQ
    };
    
    /// Key hash function for index entries
    private func hashKey(k : Key) : Hash.Hash {
        Utils.hashValue(k)
    };
}