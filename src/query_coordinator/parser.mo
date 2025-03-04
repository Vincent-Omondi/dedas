/**
 * Query Parser module for DEDaS
 * 
 * This module is responsible for parsing SQL-like queries into the
 * internal query representation used by DEDaS.
 */

import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Char "mo:base/Char";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
import Option "mo:base/Option";
import Result "mo:base/Result";
import Text "mo:base/Text";

import CommonTypes "../common/types";
import Errors "../common/errors";
import Utils "../common/utils";

module {
    type Value = CommonTypes.Value;
    type FilterExpression = CommonTypes.FilterExpression;
    type Query = CommonTypes.Query;
    
    type QueryError = Errors.QueryError;
    type QueryResult<T> = Errors.QueryResult<T>;
    
    // ===== Token Types =====
    
    /// Represents a token in the SQL query
    public type Token = {
        #Keyword : Text;
        #Identifier : Text;
        #StringLiteral : Text;
        #NumericLiteral : Text;
        #Operator : Text;
        #Punctuation : Text;
        #EOF;
    };
    
    /// Represents a token with its position
    public type TokenWithPosition = {
        token : Token;
        startPos : Nat;
        endPos : Nat;
    };
    
    // ===== Parser State =====
    
    /// Represents the parser state
    public type ParserState = {
        var tokens : [TokenWithPosition];
        var position : Nat;
        var errors : [Text];
    };
    
    // ===== Tokenizer Functions =====
    
    /// Tokenize a SQL-like query string
    public func tokenize(query : Text) : [TokenWithPosition] {
        let result = Buffer.Buffer<TokenWithPosition>(0);
        let chars = Text.toIter(query);
        var pos : Nat = 0;
        
        // List of SQL keywords
        let keywords = [
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
            "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
            "AS", "AND", "OR", "NOT", "LIKE", "IN", "BETWEEN", "IS", "NULL",
            "TRUE", "FALSE", "ASC", "DESC", "DISTINCT", "COUNT", "SUM",
            "AVG", "MIN", "MAX", "CREATE", "TABLE", "INDEX", "DROP"
        ];
        
        // Helper function to check if a character is whitespace
        let isWhitespace = func(c : Char) : Bool {
            c == ' ' or c == '\t' or c == '\n' or c == '\r'
        };
        
        // Helper function to check if a character is a letter
        let isLetter = func(c : Char) : Bool {
            (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_'
        };
        
        // Helper function to check if a character is a digit
        let isDigit = func(c : Char) : Bool {
            c >= '0' and c <= '9'
        };
        
        // Helper function to check if a character is a valid identifier character
        let isIdentChar = func(c : Char) : Bool {
            isLetter(c) or isDigit(c) or c == '_'
        };
        
        // Helper function to check if a text is a keyword
        let isKeyword = func(text : Text) : Bool {
            let upper = Text.toUppercase(text);
            for (keyword in keywords.vals()) {
                if (upper == keyword) {
                    return true;
                };
            };
            false
        };
        
        // Process the query character by character
        label l loop {
            let startPos = pos;
            switch (chars.next()) {
                case (?c) {
                    if (isWhitespace(c)) {
                        // Skip whitespace
                        pos += 1;
                    } else if (isLetter(c)) {
                        // Identifier or keyword
                        var ident = Text.fromChar(c);
                        pos += 1;
                        
                        label identLoop loop {
                            switch (chars.next()) {
                                case (?nextChar) {
                                    if (isIdentChar(nextChar)) {
                                        ident := ident # Text.fromChar(nextChar);
                                        pos += 1;
                                    } else {
                                        chars.prev();
                                        break identLoop;
                                    };
                                };
                                case (null) {
                                    break identLoop;
                                };
                            };
                        };
                        
                        if (isKeyword(ident)) {
                            result.add({
                                token = #Keyword(Text.toUppercase(ident));
                                startPos = startPos;
                                endPos = pos;
                            });
                        } else {
                            result.add({
                                token = #Identifier(ident);
                                startPos = startPos;
                                endPos = pos;
                            });
                        };
                    } else if (isDigit(c) or c == '-' or c == '+') {
                        // Numeric literal
                        var isNegative = c == '-';
                        var hasExponent = false;
                        var hasDecimal = false;
                        var numStr = Text.fromChar(c);
                        pos += 1;
                        
                        // If it's a sign, the next character must be a digit
                        if (c == '-' or c == '+') {
                            switch (chars.next()) {
                                case (?nextChar) {
                                    if (isDigit(nextChar)) {
                                        numStr := numStr # Text.fromChar(nextChar);
                                        pos += 1;
                                    } else {
                                        // It's an operator, not a number
                                        result.add({
                                            token = #Operator(numStr);
                                            startPos = startPos;
                                            endPos = pos;
                                        });
                                        chars.prev();
                                        continue l;
                                    };
                                };
                                case (null) {
                                    // End of input, it's an operator
                                    result.add({
                                        token = #Operator(numStr);
                                        startPos = startPos;
                                        endPos = pos;
                                    });
                                    break l;
                                };
                            };
                        };
                        
                        label numLoop loop {
                            switch (chars.next()) {
                                case (?nextChar) {
                                    if (isDigit(nextChar)) {
                                        numStr := numStr # Text.fromChar(nextChar);
                                        pos += 1;
                                    } else if (nextChar == '.' and not hasDecimal and not hasExponent) {
                                        numStr := numStr # ".";
                                        hasDecimal := true;
                                        pos += 1;
                                    } else if ((nextChar == 'e' or nextChar == 'E') and not hasExponent) {
                                        numStr := numStr # "e";
                                        hasExponent := true;
                                        pos += 1;
                                        
                                        // Exponent can have a sign
                                        switch (chars.next()) {
                                            case (?signChar) {
                                                if (signChar == '+' or signChar == '-') {
                                                    numStr := numStr # Text.fromChar(signChar);
                                                    pos += 1;
                                                } else {
                                                    chars.prev();
                                                };
                                            };
                                            case (null) {
                                                // Invalid exponent, but we'll let validation catch it
                                            };
                                        };
                                    } else {
                                        chars.prev();
                                        break numLoop;
                                    };
                                };
                                case (null) {
                                    break numLoop;
                                };
                            };
                        };
                        
                        result.add({
                            token = #NumericLiteral(numStr);
                            startPos = startPos;
                            endPos = pos;
                        });
                    } else if (c == '\'') {
                        // String literal
                        var str = "";
                        pos += 1;
                        var escaped = false;
                        
                        label strLoop loop {
                            switch (chars.next()) {
                                case (?nextChar) {
                                    pos += 1;
                                    
                                    if (escaped) {
                                        if (nextChar == 'n') {
                                            str := str # "\n";
                                        } else if (nextChar == 't') {
                                            str := str # "\t";
                                        } else if (nextChar == 'r') {
                                            str := str # "\r";
                                        } else if (nextChar == '\'') {
                                            str := str # "'";
                                        } else if (nextChar == '\\') {
                                            str := str # "\\";
                                        } else {
                                            // Invalid escape, just add the character
                                            str := str # Text.fromChar(nextChar);
                                        };
                                        escaped := false;
                                    } else if (nextChar == '\\') {
                                        escaped := true;
                                    } else if (nextChar == '\'') {
                                        break strLoop;
                                    } else {
                                        str := str # Text.fromChar(nextChar);
                                    };
                                };
                                case (null) {
                                    // Unterminated string, we'll let validation catch it
                                    break strLoop;
                                };
                            };
                        };
                        
                        result.add({
                            token = #StringLiteral(str);
                            startPos = startPos;
                            endPos = pos;
                        });
                    } else if (c == '"') {
                        // Quoted identifier
                        var ident = "";
                        pos += 1;
                        
                        label identLoop loop {
                            switch (chars.next()) {
                                case (?nextChar) {
                                    pos += 1;
                                    
                                    if (nextChar == '"') {
                                        break identLoop;
                                    } else {
                                        ident := ident # Text.fromChar(nextChar);
                                    };
                                };
                                case (null) {
                                    // Unterminated identifier, we'll let validation catch it
                                    break identLoop;
                                };
                            };
                        };
                        
                        result.add({
                            token = #Identifier(ident);
                            startPos = startPos;
                            endPos = pos;
                        });
                    } else if (c == '=' or c == '<' or c == '>' or c == '!') {
                        // Operators
                        var op = Text.fromChar(c);
                        pos += 1;
                        
                        // Check for two-character operators
                        switch (chars.next()) {
                            case (?nextChar) {
                                if ((c == '<' and nextChar == '=') or 
                                    (c == '>' and nextChar == '=') or 
                                    (c == '!' and nextChar == '=')) {
                                    op := op # "=";
                                    pos += 1;
                                } else {
                                    chars.prev();
                                };
                            };
                            case (null) {
                                // End of input, single char operator
                            };
                        };
                        
                        result.add({
                            token = #Operator(op);
                            startPos = startPos;
                            endPos = pos;
                        });
                    } else if (c == ',' or c == '(' or c == ')' or c == ';' or c == '*') {
                        // Punctuation
                        result.add({
                            token = #Punctuation(Text.fromChar(c));
                            startPos = startPos;
                            endPos = pos + 1;
                        });
                        pos += 1;
                    } else {
                        // Unrecognized character, skip
                        pos += 1;
                    };
                };
                case (null) {
                    break l;
                };
            };
        };
        
        // Add EOF token
        result.add({
            token = #EOF;
            startPos = pos;
            endPos = pos;
        });
        
        Buffer.toArray(result)
    };
    
    // ===== Parser Functions =====
    
    /// Parse a SQL-like query string
    public func parseQuery(queryStr : Text) : QueryResult<Query> {
        let tokens = tokenize(queryStr);
        
        // Create parser state
        let state : ParserState = {
            var tokens = tokens;
            var position = 0;
            var errors = [];
        };
        
        // Look at the first token to determine query type
        let firstToken = currentToken(state);
        
        switch (firstToken.token) {
            case (#Keyword("SELECT")) {
                parseSelectQuery(state)
            };
            case (#Keyword("INSERT")) {
                parseInsertQuery(state)
            };
            case (#Keyword("UPDATE")) {
                parseUpdateQuery(state)
            };
            case (#Keyword("DELETE")) {
                parseDeleteQuery(state)
            };
            case _ {
                #err(#ParseError("Expected SELECT, INSERT, UPDATE, or DELETE, got " # 
                                 tokenToString(firstToken.token)))
            };
        };
    };
    
    /// Parse a SELECT query
    private func parseSelectQuery(state : ParserState) : QueryResult<Query> {
        try {
            // Consume SELECT keyword
            consumeToken(state, #Keyword("SELECT"));
            
            // Parse projection
            var projection = Buffer.Buffer<CommonTypes.ProjectionElement>(0);
            
            if (checkToken(state, #Punctuation("*"))) {
                consumeToken(state, #Punctuation("*"));
                projection.add({
                    expression = "*";
                    alias = null;
                });
            } else {
                // Parse comma-separated list of expressions
                label projLoop loop {
                    let expr = parseExpression(state);
                    var alias : ?Text = null;
                    
                    // Check for AS alias
                    if (checkToken(state, #Keyword("AS"))) {
                        consumeToken(state, #Keyword("AS"));
                        
                        let aliasToken = consumeToken(state, #Identifier(""));
                        switch (aliasToken.token) {
                            case (#Identifier(name)) {
                                alias := ?name;
                            };
                            case _ {
                                state.errors := Array.append(state.errors, ["Expected identifier after AS"]);
                            };
                        };
                    };
                    
                    projection.add({
                        expression = expr;
                        alias = alias;
                    });
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue projLoop;
                    } else {
                        break projLoop;
                    };
                };
            };
            
            // Parse FROM clause
            consumeToken(state, #Keyword("FROM"));
            
            // Parse table references
            var tables = Buffer.Buffer<CommonTypes.TableReference>(0);
            
            label tablesLoop loop {
                let tableToken = consumeToken(state, #Identifier(""));
                var tableName = "";
                
                switch (tableToken.token) {
                    case (#Identifier(name)) {
                        tableName := name;
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected table name"]);
                    };
                };
                
                var alias : ?Text = null;
                
                // Check for optional alias
                if (checkToken(state, #Keyword("AS"))) {
                    consumeToken(state, #Keyword("AS"));
                    
                    let aliasToken = consumeToken(state, #Identifier(""));
                    switch (aliasToken.token) {
                        case (#Identifier(name)) {
                            alias := ?name;
                        };
                        case _ {
                            state.errors := Array.append(state.errors, ["Expected identifier after AS"]);
                        };
                    };
                } else if (checkToken(state, #Identifier(""))) {
                    // Alias without AS keyword
                    let aliasToken = consumeToken(state, #Identifier(""));
                    switch (aliasToken.token) {
                        case (#Identifier(name)) {
                            alias := ?name;
                        };
                        case _ {};
                    };
                };
                
                var join : ?CommonTypes.JoinSpecification = null;
                
                // Check for JOIN clause
                if (checkToken(state, #Keyword("JOIN")) or
                    checkToken(state, #Keyword("INNER")) or
                    checkToken(state, #Keyword("LEFT")) or
                    checkToken(state, #Keyword("RIGHT"))) {
                    
                    var joinType : CommonTypes.JoinType = #Inner;
                    
                    // Parse join type
                    if (checkToken(state, #Keyword("LEFT"))) {
                        consumeToken(state, #Keyword("LEFT"));
                        joinType := #Left;
                    } else if (checkToken(state, #Keyword("RIGHT"))) {
                        consumeToken(state, #Keyword("RIGHT"));
                        joinType := #Right;
                    } else if (checkToken(state, #Keyword("INNER"))) {
                        consumeToken(state, #Keyword("INNER"));
                    };
                    
                    // Consume JOIN keyword
                    consumeToken(state, #Keyword("JOIN"));
                    
                    // Parse joined table
                    let joinedTableToken = consumeToken(state, #Identifier(""));
                    var joinedTableName = "";
                    
                    switch (joinedTableToken.token) {
                        case (#Identifier(name)) {
                            joinedTableName := name;
                        };
                        case _ {
                            state.errors := Array.append(state.errors, ["Expected table name after JOIN"]);
                        };
                    };
                    
                    var joinedTableAlias : ?Text = null;
                    
                    // Check for optional alias
                    if (checkToken(state, #Keyword("AS"))) {
                        consumeToken(state, #Keyword("AS"));
                        
                        let aliasToken = consumeToken(state, #Identifier(""));
                        switch (aliasToken.token) {
                            case (#Identifier(name)) {
                                joinedTableAlias := ?name;
                            };
                            case _ {
                                state.errors := Array.append(state.errors, ["Expected identifier after AS"]);
                            };
                        };
                    } else if (checkToken(state, #Identifier(""))) {
                        // Alias without AS keyword
                        let aliasToken = consumeToken(state, #Identifier(""));
                        switch (aliasToken.token) {
                            case (#Identifier(name)) {
                                joinedTableAlias := ?name;
                            };
                            case _ {};
                        };
                    };
                    
                    // Parse ON condition
                    consumeToken(state, #Keyword("ON"));
                    let joinCondition = parseFilterExpression(state);
                    
                    join := ?{
                        joinType = joinType;
                        table = joinedTableName;
                        alias = joinedTableAlias;
                        condition = joinCondition;
                    };
                };
                
                tables.add({
                    table = tableName;
                    alias = alias;
                    join = join;
                });
                
                if (checkToken(state, #Punctuation(","))) {
                    consumeToken(state, #Punctuation(","));
                    continue tablesLoop;
                } else {
                    break tablesLoop;
                };
            };
            
            // Parse optional WHERE clause
            var filter : ?CommonTypes.FilterExpression = null;
            
            if (checkToken(state, #Keyword("WHERE"))) {
                consumeToken(state, #Keyword("WHERE"));
                filter := ?parseFilterExpression(state);
            };
            
            // Parse optional GROUP BY clause
            var groupBy : [Text] = [];
            
            if (checkToken(state, #Keyword("GROUP"))) {
                consumeToken(state, #Keyword("GROUP"));
                consumeToken(state, #Keyword("BY"));
                
                let groupByColumns = Buffer.Buffer<Text>(0);
                
                label groupByLoop loop {
                    let colToken = consumeToken(state, #Identifier(""));
                    switch (colToken.token) {
                        case (#Identifier(name)) {
                            groupByColumns.add(name);
                        };
                        case _ {
                            state.errors := Array.append(state.errors, ["Expected column name in GROUP BY"]);
                        };
                    };
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue groupByLoop;
                    } else {
                        break groupByLoop;
                    };
                };
                
                groupBy := Buffer.toArray(groupByColumns);
            };
            
            // Parse optional HAVING clause
            var having : ?CommonTypes.FilterExpression = null;
            
            if (checkToken(state, #Keyword("HAVING"))) {
                consumeToken(state, #Keyword("HAVING"));
                having := ?parseFilterExpression(state);
            };
            
            // Parse optional ORDER BY clause
            var orderBy : [CommonTypes.OrderElement] = [];
            
            if (checkToken(state, #Keyword("ORDER"))) {
                consumeToken(state, #Keyword("ORDER"));
                consumeToken(state, #Keyword("BY"));
                
                let orderElements = Buffer.Buffer<CommonTypes.OrderElement>(0);
                
                label orderByLoop loop {
                    let colToken = consumeToken(state, #Identifier(""));
                    var columnName = "";
                    
                    switch (colToken.token) {
                        case (#Identifier(name)) {
                            columnName := name;
                        };
                        case _ {
                            state.errors := Array.append(state.errors, ["Expected column name in ORDER BY"]);
                        };
                    };
                    
                    var direction : {#Ascending; #Descending} = #Ascending;
                    
                    if (checkToken(state, #Keyword("ASC"))) {
                        consumeToken(state, #Keyword("ASC"));
                    } else if (checkToken(state, #Keyword("DESC"))) {
                        consumeToken(state, #Keyword("DESC"));
                        direction := #Descending;
                    };
                    
                    orderElements.add({
                        expression = columnName;
                        direction = direction;
                    });
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue orderByLoop;
                    } else {
                        break orderByLoop;
                    };
                };
                
                orderBy := Buffer.toArray(orderElements);
            };
            
            // Parse optional LIMIT clause
            var limit : ?Nat = null;
            
            if (checkToken(state, #Keyword("LIMIT"))) {
                consumeToken(state, #Keyword("LIMIT"));
                
                let limitToken = consumeToken(state, #NumericLiteral(""));
                switch (limitToken.token) {
                    case (#NumericLiteral(value)) {
                        try {
                            limit := ?Utils.textToInt(value);
                        } catch (e) {
                            state.errors := Array.append(state.errors, ["Invalid LIMIT value"]);
                        };
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected number after LIMIT"]);
                    };
                };
            };
            
            // Parse optional OFFSET clause
            var offset : ?Nat = null;
            
            if (checkToken(state, #Keyword("OFFSET"))) {
                consumeToken(state, #Keyword("OFFSET"));
                
                let offsetToken = consumeToken(state, #NumericLiteral(""));
                switch (offsetToken.token) {
                    case (#NumericLiteral(value)) {
                        try {
                            offset := ?Utils.textToInt(value);
                        } catch (e) {
                            state.errors := Array.append(state.errors, ["Invalid OFFSET value"]);
                        };
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected number after OFFSET"]);
                    };
                };
            };
            
            let selectQuery : CommonTypes.SelectQuery = {
                tables = Buffer.toArray(tables);
                projection = Buffer.toArray(projection);
                filter = filter;
                groupBy = groupBy;
                having = having;
                orderBy = orderBy;
                limit = limit;
                offset = offset;
            };
            
            if (state.errors.size() > 0) {
                #err(#ParseError(Text.join("; ", state.errors.vals())))
            } else {
                #ok(#Select(selectQuery))
            }
        } catch (e) {
            #err(#ParseError("Error parsing SELECT query: " # Error.message(e)))
        };
    };
    
    /// Parse an INSERT query
    private func parseInsertQuery(state : ParserState) : QueryResult<Query> {
        try {
            // Consume INSERT keyword
            consumeToken(state, #Keyword("INSERT"));
            consumeToken(state, #Keyword("INTO"));
            
            // Parse table name
            let tableToken = consumeToken(state, #Identifier(""));
            var tableName = "";
            
            switch (tableToken.token) {
                case (#Identifier(name)) {
                    tableName := name;
                };
                case _ {
                    state.errors := Array.append(state.errors, ["Expected table name after INSERT INTO"]);
                };
            };
            
            // Parse optional column list
            var columns : [Text] = [];
            
            if (checkToken(state, #Punctuation("("))) {
                consumeToken(state, #Punctuation("("));
                
                let columnNames = Buffer.Buffer<Text>(0);
                
                label columnsLoop loop {
                    let colToken = consumeToken(state, #Identifier(""));
                    switch (colToken.token) {
                        case (#Identifier(name)) {
                            columnNames.add(name);
                        };
                        case _ {
                            state.errors := Array.append(state.errors, ["Expected column name"]);
                        };
                    };
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue columnsLoop;
                    } else {
                        break columnsLoop;
                    };
                };
                
                consumeToken(state, #Punctuation(")"));
                columns := Buffer.toArray(columnNames);
            };
            
            // Parse VALUES clause
            consumeToken(state, #Keyword("VALUES"));
            
            // Parse value lists
            let valuesList = Buffer.Buffer<[Value]>(0);
            
            label valuesLoop loop {
                consumeToken(state, #Punctuation("("));
                
                let values = Buffer.Buffer<Value>(0);
                
                label valueItemsLoop loop {
                    let value = parseLiteral(state);
                    values.add(value);
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue valueItemsLoop;
                    } else {
                        break valueItemsLoop;
                    };
                };
                
                consumeToken(state, #Punctuation(")"));
                valuesList.add(Buffer.toArray(values));
                
                if (checkToken(state, #Punctuation(","))) {
                    consumeToken(state, #Punctuation(","));
                    continue valuesLoop;
                } else {
                    break valuesLoop;
                };
            };
            
            // Parse optional RETURNING clause
            var returning : [Text] = [];
            
            if (checkToken(state, #Keyword("RETURNING"))) {
                consumeToken(state, #Keyword("RETURNING"));
                
                let returningColumns = Buffer.Buffer<Text>(0);
                
                if (checkToken(state, #Punctuation("*"))) {
                    consumeToken(state, #Punctuation("*"));
                    returningColumns.add("*");
                } else {
                    label returningLoop loop {
                        let colToken = consumeToken(state, #Identifier(""));
                        switch (colToken.token) {
                            case (#Identifier(name)) {
                                returningColumns.add(name);
                            };
                            case _ {
                                state.errors := Array.append(state.errors, ["Expected column name in RETURNING"]);
                            };
                        };
                        
                        if (checkToken(state, #Punctuation(","))) {
                            consumeToken(state, #Punctuation(","));
                            continue returningLoop;
                        } else {
                            break returningLoop;
                        };
                    };
                };
                
                returning := Buffer.toArray(returningColumns);
            };
            
            let insertQuery : CommonTypes.InsertQuery = {
                table = tableName;
                columns = columns;
                values = Buffer.toArray(valuesList);
                returning = returning;
            };
            
            if (state.errors.size() > 0) {
                #err(#ParseError(Text.join("; ", state.errors.vals())))
            } else {
                #ok(#Insert(insertQuery))
            }
        } catch (e) {
            #err(#ParseError("Error parsing INSERT query: " # Error.message(e)))
        };
    };
    
    /// Parse an UPDATE query
    private func parseUpdateQuery(state : ParserState) : QueryResult<Query> {
        try {
            // Consume UPDATE keyword
            consumeToken(state, #Keyword("UPDATE"));
            
            // Parse table name
            let tableToken = consumeToken(state, #Identifier(""));
            var tableName = "";
            
            switch (tableToken.token) {
                case (#Identifier(name)) {
                    tableName := name;
                };
                case _ {
                    state.errors := Array.append(state.errors, ["Expected table name after UPDATE"]);
                };
            };
            
            // Parse SET clause
            consumeToken(state, #Keyword("SET"));
            
            // Parse column=value assignments
            let updates = Buffer.Buffer<(Text, Value)>(0);
            
            label updatesLoop loop {
                let colToken = consumeToken(state, #Identifier(""));
                var columnName = "";
                
                switch (colToken.token) {
                    case (#Identifier(name)) {
                        columnName := name;
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected column name"]);
                    };
                };
                
                consumeToken(state, #Operator("="));
                
                let value = parseLiteral(state);
                updates.add((columnName, value));
                
                if (checkToken(state, #Punctuation(","))) {
                    consumeToken(state, #Punctuation(","));
                    continue updatesLoop;
                } else {
                    break updatesLoop;
                };
            };
            
            // Parse optional WHERE clause
            var filter : ?CommonTypes.FilterExpression = null;
            
            if (checkToken(state, #Keyword("WHERE"))) {
                consumeToken(state, #Keyword("WHERE"));
                filter := ?parseFilterExpression(state);
            };
            
            // Parse optional RETURNING clause
            var returning : [Text] = [];
            
            if (checkToken(state, #Keyword("RETURNING"))) {
                consumeToken(state, #Keyword("RETURNING"));
                
                let returningColumns = Buffer.Buffer<Text>(0);
                
                if (checkToken(state, #Punctuation("*"))) {
                    consumeToken(state, #Punctuation("*"));
                    returningColumns.add("*");
                } else {
                    label returningLoop loop {
                        let colToken = consumeToken(state, #Identifier(""));
                        switch (colToken.token) {
                            case (#Identifier(name)) {
                                returningColumns.add(name);
                            };
                            case _ {
                                state.errors := Array.append(state.errors, ["Expected column name in RETURNING"]);
                            };
                        };
                        
                        if (checkToken(state, #Punctuation(","))) {
                            consumeToken(state, #Punctuation(","));
                            continue returningLoop;
                        } else {
                            break returningLoop;
                        };
                    };
                };
                
                returning := Buffer.toArray(returningColumns);
            };
            
            let updateQuery : CommonTypes.UpdateQuery = {
                table = tableName;
                updates = Buffer.toArray(updates);
                filter = filter;
                returning = returning;
            };
            
            if (state.errors.size() > 0) {
                #err(#ParseError(Text.join("; ", state.errors.vals())))
            } else {
                #ok(#Update(updateQuery))
            }
        } catch (e) {
            #err(#ParseError("Error parsing UPDATE query: " # Error.message(e)))
        };
    };
    
    /// Parse a DELETE query
    private func parseDeleteQuery(state : ParserState) : QueryResult<Query> {
        try {
            // Consume DELETE keyword
            consumeToken(state, #Keyword("DELETE"));
            consumeToken(state, #Keyword("FROM"));
            
            // Parse table name
            let tableToken = consumeToken(state, #Identifier(""));
            var tableName = "";
            
            switch (tableToken.token) {
                case (#Identifier(name)) {
                    tableName := name;
                };
                case _ {
                    state.errors := Array.append(state.errors, ["Expected table name after DELETE FROM"]);
                };
            };
            
            // Parse optional WHERE clause
            var filter : ?CommonTypes.FilterExpression = null;
            
            if (checkToken(state, #Keyword("WHERE"))) {
                consumeToken(state, #Keyword("WHERE"));
                filter := ?parseFilterExpression(state);
            };
            
            // Parse optional RETURNING clause
            var returning : [Text] = [];
            
            if (checkToken(state, #Keyword("RETURNING"))) {
                consumeToken(state, #Keyword("RETURNING"));
                
                let returningColumns = Buffer.Buffer<Text>(0);
                
                if (checkToken(state, #Punctuation("*"))) {
                    consumeToken(state, #Punctuation("*"));
                    returningColumns.add("*");
                } else {
                    label returningLoop loop {
                        let colToken = consumeToken(state, #Identifier(""));
                        switch (colToken.token) {
                            case (#Identifier(name)) {
                                returningColumns.add(name);
                            };
                            case _ {
                                state.errors := Array.append(state.errors, ["Expected column name in RETURNING"]);
                            };
                        };
                        
                        if (checkToken(state, #Punctuation(","))) {
                            consumeToken(state, #Punctuation(","));
                            continue returningLoop;
                        } else {
                            break returningLoop;
                        };
                    };
                };
                
                returning := Buffer.toArray(returningColumns);
            };
            
            let deleteQuery : CommonTypes.DeleteQuery = {
                table = tableName;
                filter = filter;
                returning = returning;
            };
            
            if (state.errors.size() > 0) {
                #err(#ParseError(Text.join("; ", state.errors.vals())))
            } else {
                #ok(#Delete(deleteQuery))
            }
        } catch (e) {
            #err(#ParseError("Error parsing DELETE query: " # Error.message(e)))
        };
    };
    
    /// Parse a filter expression
    private func parseFilterExpression(state : ParserState) : CommonTypes.FilterExpression {
        parseLogicalExpression(state)
    };
    
    /// Parse a logical expression (AND/OR)
    private func parseLogicalExpression(state : ParserState) : CommonTypes.FilterExpression {
        var expr = parseComparisonExpression(state);
        
        label logicalLoop loop {
            if (checkToken(state, #Keyword("AND"))) {
                consumeToken(state, #Keyword("AND"));
                let rightExpr = parseComparisonExpression(state);
                
                // Combine with existing AND conditions if possible
                switch (expr) {
                    case (#And(conditions)) {
                        expr := #And(Array.append(conditions, [rightExpr]));
                    };
                    case _ {
                        expr := #And([expr, rightExpr]);
                    };
                };
                
                continue logicalLoop;
            } else if (checkToken(state, #Keyword("OR"))) {
                consumeToken(state, #Keyword("OR"));
                let rightExpr = parseComparisonExpression(state);
                
                // Combine with existing OR conditions if possible
                switch (expr) {
                    case (#Or(conditions)) {
                        expr := #Or(Array.append(conditions, [rightExpr]));
                    };
                    case _ {
                        expr := #Or([expr, rightExpr]);
                    };
                };
                
                continue logicalLoop;
            } else {
                break logicalLoop;
            };
        };
        
        expr
    };
    
    /// Parse a comparison expression
    private func parseComparisonExpression(state : ParserState) : CommonTypes.FilterExpression {
        if (checkToken(state, #Keyword("NOT"))) {
            consumeToken(state, #Keyword("NOT"));
            
            if (checkToken(state, #Punctuation("("))) {
                // NOT (expression)
                consumeToken(state, #Punctuation("("));
                let expr = parseFilterExpression(state);
                consumeToken(state, #Punctuation(")"));
                
                return #Not(expr);
            } else {
                // NOT condition
                let expr = parseComparisonExpression(state);
                return #Not(expr);
            };
        } else if (checkToken(state, #Punctuation("("))) {
            // Parenthesized expression
            consumeToken(state, #Punctuation("("));
            let expr = parseFilterExpression(state);
            consumeToken(state, #Punctuation(")"));
            
            return expr;
        } else {
            // Column comparison
            let colToken = consumeToken(state, #Identifier(""));
            var columnName = "";
            
            switch (colToken.token) {
                case (#Identifier(name)) {
                    columnName := name;
                };
                case _ {
                    state.errors := Array.append(state.errors, ["Expected column name"]);
                };
            };
            
            if (checkToken(state, #Keyword("IS"))) {
                consumeToken(state, #Keyword("IS"));
                
                if (checkToken(state, #Keyword("NULL"))) {
                    consumeToken(state, #Keyword("NULL"));
                    return #IsNull(columnName);
                } else if (checkToken(state, #Keyword("NOT"))) {
                    consumeToken(state, #Keyword("NOT"));
                    
                    if (checkToken(state, #Keyword("NULL"))) {
                        consumeToken(state, #Keyword("NULL"));
                        return #IsNotNull(columnName);
                    } else {
                        state.errors := Array.append(state.errors, ["Expected NULL after IS NOT"]);
                        // Return a default expression
                        return #IsNotNull(columnName);
                    };
                } else {
                    state.errors := Array.append(state.errors, ["Expected NULL or NOT after IS"]);
                    // Return a default expression
                    return #IsNull(columnName);
                };
            } else if (checkToken(state, #Keyword("BETWEEN"))) {
                consumeToken(state, #Keyword("BETWEEN"));
                
                let lowerValue = parseLiteral(state);
                consumeToken(state, #Keyword("AND"));
                let upperValue = parseLiteral(state);
                
                return #Between(columnName, lowerValue, upperValue);
            } else if (checkToken(state, #Keyword("IN"))) {
                consumeToken(state, #Keyword("IN"));
                consumeToken(state, #Punctuation("("));
                
                let values = Buffer.Buffer<Value>(0);
                
                label inLoop loop {
                    let value = parseLiteral(state);
                    values.add(value);
                    
                    if (checkToken(state, #Punctuation(","))) {
                        consumeToken(state, #Punctuation(","));
                        continue inLoop;
                    } else {
                        break inLoop;
                    };
                };
                
                consumeToken(state, #Punctuation(")"));
                
                return #In(columnName, Buffer.toArray(values));
            } else if (checkToken(state, #Keyword("LIKE"))) {
                consumeToken(state, #Keyword("LIKE"));
                
                let patternToken = consumeToken(state, #StringLiteral(""));
                var pattern = "";
                
                switch (patternToken.token) {
                    case (#StringLiteral(text)) {
                        pattern := text;
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected string pattern after LIKE"]);
                    };
                };
                
                return #Like(columnName, pattern);
            } else {
                // Standard comparison operators
                let opToken = currentToken(state);
                var operator = "";
                
                switch (opToken.token) {
                    case (#Operator(op)) {
                        operator := op;
                        consumeToken(state, #Operator(op));
                    };
                    case _ {
                        state.errors := Array.append(state.errors, ["Expected comparison operator"]);
                    };
                };
                
                let value = parseLiteral(state);
                
                switch (operator) {
                    case "=" { #Equals(columnName, value) };
                    case "!=" or "<>" { #NotEquals(columnName, value) };
                    case ">" { #GreaterThan(columnName, value) };
                    case "<" { #LessThan(columnName, value) };
                    case ">=" { #GreaterThanOrEqual(columnName, value) };
                    case "<=" { #LessThanOrEqual(columnName, value) };
                    case _ {
                        state.errors := Array.append(state.errors, ["Unsupported operator: " # operator]);
                        #Equals(columnName, value) // Default
                    };
                };
            };
        };
    };
    
    /// Parse an expression (for projection)
    private func parseExpression(state : ParserState) : Text {
        // For now, we only support simple column names
        // In a real implementation, this would parse complex expressions
        
        let colToken = consumeToken(state, #Identifier(""));
        switch (colToken.token) {
            case (#Identifier(name)) {
                name
            };
            case _ {
                state.errors := Array.append(state.errors, ["Expected column name"]);
                ""
            };
        };
    };
    
    /// Parse a literal value
    private func parseLiteral(state : ParserState) : Value {
        let token = currentToken(state);
        
        switch (token.token) {
            case (#StringLiteral(text)) {
                consumeToken(state, #StringLiteral(""));
                #Text(text)
            };
            case (#NumericLiteral(text)) {
                consumeToken(state, #NumericLiteral(""));
                
                if (Text.contains(text, #text ".") or 
                    Text.contains(text, #text "e") or 
                    Text.contains(text, #text "E")) {
                    // Float
                    try {
                        #Float(Utils.textToFloat(text))
                    } catch (e) {
                        state.errors := Array.append(state.errors, ["Invalid float literal: " # text]);
                        #Float(0)
                    }
                } else {
                    // Integer
                    try {
                        #Int(Utils.textToInt(text))
                    } catch (e) {
                        state.errors := Array.append(state.errors, ["Invalid integer literal: " # text]);
                        #Int(0)
                    }
                }
            };
            case (#Keyword("TRUE")) {
                consumeToken(state, #Keyword("TRUE"));
                #Bool(true)
            };
            case (#Keyword("FALSE")) {
                consumeToken(state, #Keyword("FALSE"));
                #Bool(false)
            };
            case (#Keyword("NULL")) {
                consumeToken(state, #Keyword("NULL"));
                #Null
            };
            case (#Punctuation("(")) {
                // Array literal or parenthesized expression
                consumeToken(state, #Punctuation("("));
                
                if (checkToken(state, #Punctuation(")"))) {
                    // Empty array
                    consumeToken(state, #Punctuation(")"));
                    #Array([])
                } else {
                    let firstValue = parseLiteral(state);
                    
                    if (checkToken(state, #Punctuation(","))) {
                        // Array literal
                        let values = Buffer.Buffer<Value>(0);
                        values.add(firstValue);
                        
                        label arrayLoop loop {
                            consumeToken(state, #Punctuation(","));
                            values.add(parseLiteral(state));
                            
                            if (checkToken(state, #Punctuation(","))) {
                                continue arrayLoop;
                            } else {
                                break arrayLoop;
                            };
                        };
                        
                        consumeToken(state, #Punctuation(")"));
                        #Array(Buffer.toArray(values))
                    } else {
                        // Parenthesized expression, return the value
                        consumeToken(state, #Punctuation(")"));
                        firstValue
                    }
                }
            };
            case _ {
                state.errors := Array.append(state.errors, ["Expected literal, got " # tokenToString(token.token)]);
                #Null
            };
        };
    };
    
    // ===== Parser Utility Functions =====
    
    /// Get the current token
    private func currentToken(state : ParserState) : TokenWithPosition {
        if (state.position >= state.tokens.size()) {
            {
                token = #EOF;
                startPos = 0;
                endPos = 0;
            }
        } else {
            state.tokens[state.position]
        }
    };
    
    /// Check if the current token matches the expected type
    private func checkToken(state : ParserState, expected : Token) : Bool {
        let current = currentToken(state);
        
        switch (current.token, expected) {
            case (#Keyword(k1), #Keyword(k2)) {
                Text.toUppercase(k1) == Text.toUppercase(k2)
            };
            case (#Identifier(_), #Identifier(_)) {
                true
            };
            case (#StringLiteral(_), #StringLiteral(_)) {
                true
            };
            case (#NumericLiteral(_), #NumericLiteral(_)) {
                true
            };
            case (#Operator(op1), #Operator(op2)) {
                op1 == op2
            };
            case (#Punctuation(p1), #Punctuation(p2)) {
                p1 == p2
            };
            case (#EOF, #EOF) {
                true
            };
            case _ {
                false
            };
        };
    };
    
    /// Consume a token and advance the position
    private func consumeToken(state : ParserState, expected : Token) : TokenWithPosition {
        let current = currentToken(state);
        
        if (checkToken(state, expected)) {
            state.position += 1;
            current
        } else {
            state.errors := Array.append(state.errors, ["Expected " # tokenToString(expected) # ", got " # tokenToString(current.token)]);
            
            // Try to recover by advancing
            state.position += 1;
            current
        }
    };
    
    /// Convert a token to a string representation
    private func tokenToString(token : Token) : Text {
        switch (token) {
            case (#Keyword(k)) { "keyword " # k };
            case (#Identifier(id)) { "identifier " # id };
            case (#StringLiteral(s)) { "string '" # s # "'" };
            case (#NumericLiteral(n)) { "number " # n };
            case (#Operator(op)) { "operator " # op };
            case (#Punctuation(p)) { "punctuation " # p };
            case (#EOF) { "end of input" };
        };
    };
}