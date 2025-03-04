/**
 * Common utility functions for DEDaS
 * 
 * This module contains utility functions used across all canisters
 * in the DEDaS system.
 */

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Buffer "mo:base/Buffer";
import Char "mo:base/Char";
import Debug "mo:base/Debug";
import Float "mo:base/Float";
import Int "mo:base/Int";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Trie "mo:base/Trie";
import TrieMap "mo:base/TrieMap";

import CommonTypes "types";

module {
    type Key = CommonTypes.Key;
    type Value = CommonTypes.Value;
    type Record = CommonTypes.Record;
    
    // ===== ID Generation =====
    
    /// Generate a unique ID with a prefix and timestamp
    public func generateId(prefix : Text) : Text {
        let timestamp = Int.abs(Time.now());
        let randomSuffix = Int.abs(Time.now() / 1000) % 10000;
        prefix # "-" # Nat.toText(timestamp) # "-" # Nat.toText(randomSuffix)
    };
    
    /// Generate a database ID
    public func generateDatabaseId() : Text {
        generateId("db")
    };
    
    /// Generate a shard ID
    public func generateShardId() : Text {
        generateId("shard")
    };
    
    /// Generate a transaction ID
    public func generateTransactionId() : Text {
        generateId("txn")
    };
    
    /// Generate a query ID
    public func generateQueryId() : Text {
        generateId("qry")
    };
    
    // ===== Validation Functions =====
    
    /// Check if a name is valid (alphanumeric, underscore, hyphen)
    public func isValidName(name : Text) : Bool {
        if (Text.size(name) == 0) {
            return false;
        };
        
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
    
    /// Check if a database name is valid
    public func isValidDatabaseName(name : Text) : Bool {
        isValidName(name)
    };
    
    /// Check if a table name is valid
    public func isValidTableName(name : Text) : Bool {
        isValidName(name)
    };
    
    /// Check if a column name is valid
    public func isValidColumnName(name : Text) : Bool {
        isValidName(name)
    };
    
    // ===== Value Conversion Functions =====
    
    /// Convert a Value to Text for display
    public func valueToText(value : Value) : Text {
        switch (value) {
            case (#Int(i)) { Int.toText(i) };
            case (#Float(f)) { Float.toText(f) };
            case (#Text(t)) { "\"" # t # "\"" };
            case (#Bool(b)) { if (b) "true" else "false" };
            case (#Blob(b)) { 
                let size = b.size();
                if (size <= 8) {
                    "0x" # Blob.toArray(b).vals()
                } else {
                    "0x" # Blob.toArray(b).vals() # "... (" # Nat.toText(size) # " bytes)"
                }
            };
            case (#Null) { "null" };
            case (#Array(a)) {
                let buffer = Buffer.Buffer<Text>(a.size());
                for (v in a.vals()) {
                    buffer.add(valueToText(v));
                };
                "[" # Text.join(", ", Buffer.toArray(buffer).vals()) # "]"
            };
            case (#Record(r)) {
                let buffer = Buffer.Buffer<Text>(r.size());
                for ((k, v) in r.vals()) {
                    buffer.add(k # ": " # valueToText(v));
                };
                "{" # Text.join(", ", Buffer.toArray(buffer).vals()) # "}"
            };
            case (#Variant(label, value)) {
                switch (value) {
                    case (?v) { "#" # label # "(" # valueToText(v) # ")" };
                    case (null) { "#" # label };
                };
            };
        };
    };
    
    /// Try to convert a Value to a specific type
    public func tryCoerceValue(value : Value, targetType : CommonTypes.DataType) : ?Value {
        switch (value, targetType) {
            // Direct matches
            case (#Int(_), #Int) { ?value };
            case (#Float(_), #Float) { ?value };
            case (#Text(_), #Text) { ?value };
            case (#Bool(_), #Bool) { ?value };
            case (#Blob(_), #Blob) { ?value };
            case (#Null, #Null) { ?value };
            
            // Conversion between numeric types
            case (#Int(i), #Float) { ?#Float(Float.fromInt(i)) };
            case (#Float(f), #Int) { ?#Int(Int.fromFloat(f)) };
            
            // Conversion to text
            case (_, #Text) { ?#Text(valueToText(value)) };
            
            // Conversion from text
            case (#Text(t), #Int) {
                try {
                    ?#Int(textToInt(t))
                } catch (e) {
                    null
                }
            };
            case (#Text(t), #Float) {
                try {
                    ?#Float(textToFloat(t))
                } catch (e) {
                    null
                }
            };
            case (#Text(t), #Bool) {
                let lower = Text.toLower(t);
                if (lower == "true" or lower == "yes" or lower == "1") {
                    ?#Bool(true)
                } else if (lower == "false" or lower == "no" or lower == "0") {
                    ?#Bool(false)
                } else {
                    null
                }
            };
            
            // Optional handling
            case (#Null, #Optional(_)) { ?#Null };
            case (v, #Optional(innerType)) {
                switch (tryCoerceValue(v, innerType)) {
                    case (?converted) { ?converted };
                    case (null) { null };
                };
            };
            
            // Array handling
            case (#Array(arr), #Array(innerType)) {
                let convertedBuffer = Buffer.Buffer<Value>(arr.size());
                var allConverted = true;
                
                for (item in arr.vals()) {
                    switch (tryCoerceValue(item, innerType)) {
                        case (?converted) {
                            convertedBuffer.add(converted);
                        };
                        case (null) {
                            allConverted := false;
                        };
                    };
                };
                
                if (allConverted) {
                    ?#Array(Buffer.toArray(convertedBuffer))
                } else {
                    null
                }
            };
            
            // No valid conversion found
            case (_, _) { null };
        };
    };
    
    /// Convert text to Int, throws if invalid
    public func textToInt(text : Text) : Int {
        let trimmed = Text.trim(text, #predicate(Char.isWhitespace));
        
        if (Text.size(trimmed) == 0) {
            Debug.trap("Cannot convert empty string to Int");
        };
        
        var isNegative = false;
        var startIndex = 0;
        
        if (Text.charAt(trimmed, 0) == '-') {
            isNegative := true;
            startIndex := 1;
        } else if (Text.charAt(trimmed, 0) == '+') {
            startIndex := 1;
        };
        
        var value : Int = 0;
        
        for (i in Iter.range(startIndex, Text.size(trimmed) - 1)) {
            let char = Text.charAt(trimmed, i);
            if (char < '0' or char > '9') {
                Debug.trap("Invalid character in Int: " # Char.toText(char));
            };
            
            let digit = Nat32.toNat(Char.toNat32(char) - Char.toNat32('0'));
            value := value * 10 + digit;
        };
        
        if (isNegative) {
            -value
        } else {
            value
        }
    };
    
    /// Convert text to Float, throws if invalid
    public func textToFloat(text : Text) : Float {
        let trimmed = Text.trim(text, #predicate(Char.isWhitespace));
        
        if (Text.size(trimmed) == 0) {
            Debug.trap("Cannot convert empty string to Float");
        };
        
        var isNegative = false;
        var startIndex = 0;
        
        if (Text.charAt(trimmed, 0) == '-') {
            isNegative := true;
            startIndex := 1;
        } else if (Text.charAt(trimmed, 0) == '+') {
            startIndex := 1;
        };
        
        var intPart : Float = 0;
        var fracPart : Float = 0;
        var fracDigits : Nat = 0;
        var inFracPart = false;
        
        for (i in Iter.range(startIndex, Text.size(trimmed) - 1)) {
            let char = Text.charAt(trimmed, i);
            
            if (char == '.') {
                if (inFracPart) {
                    Debug.trap("Multiple decimal points in Float");
                };
                inFracPart := true;
            } else if (char >= '0' and char <= '9') {
                let digit = Float.fromInt(
                    Nat32.toNat(Char.toNat32(char) - Char.toNat32('0'))
                );
                
                if (inFracPart) {
                    fracDigits += 1;
                    fracPart := fracPart * 10 + digit;
                } else {
                    intPart := intPart * 10 + digit;
                };
            } else {
                Debug.trap("Invalid character in Float: " # Char.toText(char));
            };
        };
        
        var result = intPart;
        
        if (fracDigits > 0) {
            result += fracPart / Float.pow(10, Float.fromInt(fracDigits));
        };
        
        if (isNegative) {
            -result
        } else {
            result
        }
    };
    
    // ===== Record Operations =====
    
    /// Create a record from key-value pairs
    public func createRecord(pairs : [(Text, Value)]) : Record {
        var record = Iter.toArray(Iter.fromArray(pairs));
        record
    };
    
    /// Get a value from a record by key
    public func getRecordValue(record : Record, key : Text) : ?Value {
        for ((k, v) in record.vals()) {
            if (k == key) {
                return ?v;
            };
        };
        
        null
    };
    
    /// Set a value in a record
    public func setRecordValue(record : Record, key : Text, value : Value) : Record {
        var found = false;
        let newRecord = Array.map<(Text, Value), (Text, Value)>(
            record,
            func((k, v)) {
                if (k == key) {
                    found := true;
                    (k, value)
                } else {
                    (k, v)
                }
            }
        );
        
        if (found) {
            newRecord
        } else {
            Array.append(newRecord, [(key, value)])
        }
    };
    
    /// Remove a key from a record
    public func removeRecordKey(record : Record, key : Text) : Record {
        let buffer = Buffer.Buffer<(Text, Value)>(0);
        
        for ((k, v) in record.vals()) {
            if (k != key) {
                buffer.add((k, v));
            };
        };
        
        Buffer.toArray(buffer)
    };
    
    /// Check if a key exists in a record
    public func recordHasKey(record : Record, key : Text) : Bool {
        for ((k, _) in record.vals()) {
            if (k == key) {
                return true;
            };
        };
        
        false
    };
    
    // ===== Comparison Functions =====
    
    /// Compare two Values
    public func compareValues(v1 : Value, v2 : Value) : {#LT; #EQ; #GT} {
        switch (v1, v2) {
            case (#Int(i1), #Int(i2)) {
                if (i1 < i2) #LT
                else if (i1 > i2) #GT
                else #EQ
            };
            case (#Float(f1), #Float(f2)) {
                if (f1 < f2) #LT
                else if (f1 > f2) #GT
                else #EQ
            };
            case (#Int(i), #Float(f)) {
                let fi = Float.fromInt(i);
                if (fi < f) #LT
                else if (fi > f) #GT
                else #EQ
            };
            case (#Float(f), #Int(i)) {
                let fi = Float.fromInt(i);
                if (f < fi) #LT
                else if (f > fi) #GT
                else #EQ
            };
            case (#Text(t1), #Text(t2)) {
                if (t1 < t2) #LT
                else if (t1 > t2) #GT
                else #EQ
            };
            case (#Bool(b1), #Bool(b2)) {
                if (b1 == b2) #EQ
                else if (b1) #GT
                else #LT
            };
            case (#Null, #Null) { #EQ };
            case (#Null, _) { #LT };
            case (_, #Null) { #GT };
            case (#Blob(b1), #Blob(b2)) {
                let size1 = b1.size();
                let size2 = b2.size();
                
                if (size1 < size2) #LT
                else if (size1 > size2) #GT
                else {
                    // Compare byte by byte
                    let arr1 = Blob.toArray(b1);
                    let arr2 = Blob.toArray(b2);
                    
                    var i = 0;
                    while (i < size1) {
                        if (arr1[i] < arr2[i]) return #LT;
                        if (arr1[i] > arr2[i]) return #GT;
                        i += 1;
                    };
                    
                    #EQ
                }
            };
            case (#Array(a1), #Array(a2)) {
                let size1 = a1.size();
                let size2 = a2.size();
                
                var i = 0;
                let minSize = Nat.min(size1, size2);
                
                while (i < minSize) {
                    let comparison = compareValues(a1[i], a2[i]);
                    if (comparison != #EQ) return comparison;
                    i += 1;
                };
                
                if (size1 < size2) #LT
                else if (size1 > size2) #GT
                else #EQ
            };
            case (#Record(r1), #Record(r2)) {
                // Compare by keys first
                let keys1 = Array.sort<Text>(Array.map<(Text, Value), Text>(r1, func((k, _)) { k }), Text.compare);
                let keys2 = Array.sort<Text>(Array.map<(Text, Value), Text>(r2, func((k, _)) { k }), Text.compare);
                
                let size1 = keys1.size();
                let size2 = keys2.size();
                
                var i = 0;
                let minSize = Nat.min(size1, size2);
                
                while (i < minSize) {
                    let keyComparison = Text.compare(keys1[i], keys2[i]);
                    if (keyComparison != #equal) {
                        return if (keyComparison == #less) #LT else #GT;
                    };
                    i += 1;
                };
                
                if (size1 < size2) return #LT;
                if (size1 > size2) return #GT;
                
                // Keys are the same, compare values
                for (i in Iter.range(0, size1 - 1)) {
                    let key = keys1[i];
                    let val1 = Option.get(getRecordValue(r1, key), #Null);
                    let val2 = Option.get(getRecordValue(r2, key), #Null);
                    
                    let comparison = compareValues(val1, val2);
                    if (comparison != #EQ) return comparison;
                };
                
                #EQ
            };
            case (#Variant(l1, v1), #Variant(l2, v2)) {
                let labelComparison = Text.compare(l1, l2);
                if (labelComparison != #equal) {
                    return if (labelComparison == #less) #LT else #GT;
                };
                
                switch (v1, v2) {
                    case (?val1, ?val2) {
                        compareValues(val1, val2)
                    };
                    case (null, ?_) {
                        #LT
                    };
                    case (?_, null) {
                        #GT
                    };
                    case (null, null) {
                        #EQ
                    };
                };
            };
            case _ {
                // For different types, we define an arbitrary ordering based on type tag
                // This is useful for consistent sorting even when types don't naturally compare
                let typeOrder = func(v : Value) : Nat {
                    switch (v) {
                        case (#Null) 0;
                        case (#Bool(_)) 1;
                        case (#Int(_)) 2;
                        case (#Float(_)) 3;
                        case (#Text(_)) 4;
                        case (#Blob(_)) 5;
                        case (#Array(_)) 6;
                        case (#Record(_)) 7;
                        case (#Variant(_, _)) 8;
                    };
                };
                
                let order1 = typeOrder(v1);
                let order2 = typeOrder(v2);
                
                if (order1 < order2) #LT
                else if (order1 > order2) #GT
                else #EQ
            };
        };
    };
    
    // ===== Hashing Functions =====
    
    /// Hash a Value for use in hash tables
    public func hashValue(v : Value) : Nat32 {
        switch (v) {
            case (#Int(i)) {
                // Use the existing Int.hash function (it doesn't exist, using a simple hash)
                let asNat = if (i >= 0) Nat64.fromIntWrap(i) else Nat64.fromIntWrap(-i) ^ 0xFFFFFFFFFFFFFFFF;
                return Nat32.fromNat(Nat64.toNat(asNat & 0xFFFFFFFF));
            };
            case (#Float(f)) {
                // Convert to bits and hash as integer
                if (f == 0) return 0;
                if (f < 0) return hashValue(#Int(-1)) ^ hashValue(#Float(-f));
                
                let intPart = Int.abs(Float.toInt(f));
                let fracPart = Int.abs(Float.toInt((f - Float.fromInt(intPart)) * 1_000_000));
                
                return hashValue(#Int(intPart)) ^ hashValue(#Int(fracPart));
            };
            case (#Text(t)) {
                Text.hash(t)
            };
            case (#Bool(b)) {
                if (b) 1 else 0
            };
            case (#Blob(b)) {
                var hash : Nat32 = 0;
                for (byte in Blob.toArray(b).vals()) {
                    hash := hash +% Nat32.fromNat(Nat8.toNat(byte));
                    hash := hash *% 16777619;
                };
                hash
            };
            case (#Null) {
                0
            };
            case (#Array(a)) {
                var hash : Nat32 = 0;
                for (item in a.vals()) {
                    hash := hash +% hashValue(item);
                    hash := hash *% 16777619;
                };
                hash
            };
            case (#Record(r)) {
                var hash : Nat32 = 0;
                for ((k, v) in r.vals()) {
                    hash := hash +% Text.hash(k);
                    hash := hash *% 16777619;
                    hash := hash +% hashValue(v);
                    hash := hash *% 16777619;
                };
                hash
            };
            case (#Variant(label, value)) {
                var hash = Text.hash(label);
                
                switch (value) {
                    case (?v) {
                        hash := hash +% hashValue(v);
                        hash := hash *% 16777619;
                    };
                    case (null) {
                        // Use the label hash only
                    };
                };
                
                hash
            };
        };
    };
}