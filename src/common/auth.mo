/**
 * Authentication and authorization utilities for DEDaS
 * 
 * This module contains types and utilities for handling authentication
 * and role-based access control in the DEDaS system.
 */

import Array "mo:base/Array";
import HashMap "mo:base/HashMap";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";
import Result "mo:base/Result";

import CommonTypes "types";
import Errors "errors";
import Utils "utils";

module {
    // ===== Type Aliases =====
    
    type DatabaseId = CommonTypes.DatabaseId;
    
    type AuthError = Errors.AuthError;
    type AuthResult<T> = Errors.AuthResult<T>;
    
    // ===== Authentication Types =====
    
    /// Represents a session for an authenticated user
    public type Session = {
        id : Text;
        principal : Principal;
        created : Time.Time;
        expires : Time.Time;
        permissions : [Permission];
    };
    
    /// Represents credentials used for authentication
    public type Credentials = {
        #Principal : Principal;
        #ApiKey : Text;
        #DelegationToken : Text;
    };
    
    /// Represents a delegation token for delegated access
    public type DelegationToken = {
        id : Text;
        delegator : Principal;
        delegate : Principal;
        permissions : [Permission];
        created : Time.Time;
        expires : Time.Time;
        signature : Blob;
    };
    
    // ===== Authorization Types =====
    
    /// Represents a permission
    public type Permission = {
        #Read;
        #Write;
        #Admin;
        #Schema;
    };
    
    /// Represents a resource type
    public type ResourceType = {
        #Database;
        #Table;
        #Row;
        #Column;
    };
    
    /// Represents a resource
    public type Resource = {
        resourceType : ResourceType;
        resourceName : Text;
    };
    
    /// Represents an access policy
    public type AccessPolicy = {
        principal : Principal;
        resource : Resource;
        permissions : [Permission];
        conditions : ?FilterCondition;
    };
    
    /// Represents a filter condition
    public type FilterCondition = {
        column : Text;
        operator : FilterOperator;
        value : CommonTypes.Value;
    };
    
    /// Represents a filter operator
    public type FilterOperator = {
        #Equals;
        #NotEquals;
        #GreaterThan;
        #LessThan;
        #GreaterThanOrEqual;
        #LessThanOrEqual;
        #Like;
        #In;
    };
    
    /// Represents a role
    public type Role = {
        name : Text;
        permissions : [Permission];
    };
    
    /// Represents a role assignment
    public type RoleAssignment = {
        principal : Principal;
        role : Text;
        resource : Resource;
        created : Time.Time;
        expiresAt : ?Time.Time;
    };
    
    // ===== Authentication Functions =====
    
    /// Validate credentials and return a session
    public func authenticate(
        credentials : Credentials, 
        sessionDurationNanos : Nat
    ) : AuthResult<Session> {
        switch (credentials) {
            case (#Principal(p)) {
                if (Principal.isAnonymous(p)) {
                    return #err(#NotAuthenticated("Anonymous principal not allowed"));
                };
                
                let sessionId = Utils.generateId("session");
                let now = Time.now();
                
                let session : Session = {
                    id = sessionId;
                    principal = p;
                    created = now;
                    expires = now + sessionDurationNanos;
                    permissions = []; // Permissions should be loaded separately
                };
                
                #ok(session)
            };
            case (#ApiKey(key)) {
                // API key validation would be implemented here
                // For now, we'll return an error
                #err(#InvalidCredentials("API key authentication not implemented"))
            };
            case (#DelegationToken(token)) {
                // Delegation token validation would be implemented here
                // For now, we'll return an error
                #err(#InvalidCredentials("Delegation token authentication not implemented"))
            };
        };
    };
    
    /// Check if a session is valid
    public func isSessionValid(session : Session) : Bool {
        Time.now() < session.expires
    };
    
    /// Create a delegation token
    public func createDelegationToken(
        delegator : Principal,
        delegate : Principal,
        permissions : [Permission],
        expirationNanos : Nat
    ) : DelegationToken {
        let tokenId = Utils.generateId("token");
        let now = Time.now();
        
        // In a real implementation, we'd use cryptographic signatures
        // For now, just create a placeholder signature
        let signatureText = Principal.toText(delegator) # Principal.toText(delegate) # 
                           Text.join(",", Array.map<Permission, Text>(permissions, permissionToText));
        let signature = Text.encodeUtf8(signatureText);
        
        {
            id = tokenId;
            delegator = delegator;
            delegate = delegate;
            permissions = permissions;
            created = now;
            expires = now + expirationNanos;
            signature = signature;
        }
    };
    
    /// Verify a delegation token
    public func verifyDelegationToken(token : DelegationToken) : Bool {
        // Check if token has expired
        if (Time.now() > token.expires) {
            return false;
        };
        
        // In a real implementation, we'd verify the cryptographic signature
        // For now, just recreate the signature and compare
        let signatureText = Principal.toText(token.delegator) # Principal.toText(token.delegate) # 
                           Text.join(",", Array.map<Permission, Text>(token.permissions, permissionToText));
        let expectedSignature = Text.encodeUtf8(signatureText);
        
        // Compare signatures
        if (token.signature.size() != expectedSignature.size()) {
            return false;
        };
        
        let tokenBytes = token.signature;
        let expectedBytes = expectedSignature;
        
        for (i in Iter.range(0, tokenBytes.size() - 1)) {
            if (tokenBytes[i] != expectedBytes[i]) {
                return false;
            };
        };
        
        true
    };
    
    /// Convert a permission to text
    public func permissionToText(permission : Permission) : Text {
        switch (permission) {
            case (#Read) { "READ" };
            case (#Write) { "WRITE" };
            case (#Admin) { "ADMIN" };
            case (#Schema) { "SCHEMA" };
        };
    };
    
    // ===== Authorization Functions =====
    
    /// Check if a principal has permission for a resource
    public func hasPermission(
        principal : Principal,
        resource : Resource,
        permission : Permission,
        policies : [AccessPolicy]
    ) : Bool {
        // Find relevant policies for this principal and resource
        let matchingPolicies = Array.filter<AccessPolicy>(
            policies,
            func(policy) {
                principalMatches(policy.principal, principal) and resourceMatches(policy.resource, resource)
            }
        );
        
        // Check if any policy grants the requested permission
        for (policy in matchingPolicies.vals()) {
            if (Array.contains<Permission>(policy.permissions, permission, permissionEqual)) {
                return true;
            };
        };
        
        false
    };
    
    /// Check if a principal can perform an operation
    public func canPerformOperation(
        principal : Principal,
        resource : Resource,
        operation : {#Read; #Write; #Admin; #Schema},
        policies : [AccessPolicy]
    ) : Bool {
        let requiredPermission = switch (operation) {
            case (#Read) { #Read };
            case (#Write) { #Write };
            case (#Admin) { #Admin };
            case (#Schema) { #Schema };
        };
        
        hasPermission(principal, resource, requiredPermission, policies)
    };
    
    /// Check if a principal matches another principal
    public func principalMatches(policy : Principal, user : Principal) : Bool {
        Principal.equal(policy, user)
    };
    
    /// Check if a resource matches another resource
    public func resourceMatches(policyResource : Resource, userResource : Resource) : Bool {
        // Check resource type
        if (policyResource.resourceType != userResource.resourceType) {
            return false;
        };
        
        // Check resource name
        // For hierarchical resources, we need to handle wildcards and parent resources
        switch (policyResource.resourceType) {
            case (#Database) {
                // Database names must match exactly, or policy can use "*" wildcard
                policyResource.resourceName == "*" or policyResource.resourceName == userResource.resourceName
            };
            case (#Table) {
                // Table names can be wildcarded with "*", or specific tables
                if (policyResource.resourceName == "*") {
                    return true;
                };
                
                // Check if we have a database.table format
                let policyParts = Text.split(policyResource.resourceName, #char('.'));
                let userParts = Text.split(userResource.resourceName, #char('.'));
                
                let policyArray = Iter.toArray(policyParts);
                let userArray = Iter.toArray(userParts);
                
                // If we don't have database.table format, just compare names
                if (policyArray.size() != 2 or userArray.size() != 2) {
                    return policyResource.resourceName == userResource.resourceName;
                };
                
                // Check database match
                if (policyArray[0] != "*" and policyArray[0] != userArray[0]) {
                    return false;
                };
                
                // Check table match
                policyArray[1] == "*" or policyArray[1] == userArray[1]
            };
            case (#Column) {
                // Column names can be wildcarded with "*"
                if (policyResource.resourceName == "*") {
                    return true;
                };
                
                // Check if we have a database.table.column format
                let policyParts = Text.split(policyResource.resourceName, #char('.'));
                let userParts = Text.split(userResource.resourceName, #char('.'));
                
                let policyArray = Iter.toArray(policyParts);
                let userArray = Iter.toArray(userParts);
                
                // If we don't have database.table.column format, just compare names
                if (policyArray.size() != 3 or userArray.size() != 3) {
                    return policyResource.resourceName == userResource.resourceName;
                };
                
                // Check database match
                if (policyArray[0] != "*" and policyArray[0] != userArray[0]) {
                    return false;
                };
                
                // Check table match
                if (policyArray[1] != "*" and policyArray[1] != userArray[1]) {
                    return false;
                };
                
                // Check column match
                policyArray[2] == "*" or policyArray[2] == userArray[2]
            };
            case (#Row) {
                // Row resources use table names and conditions
                // For simplicity, we'll just check table names here
                // In a real implementation, we'd check the conditions against the row
                policyResource.resourceName == "*" or policyResource.resourceName == userResource.resourceName
            };
        };
    };
    
    /// Check if two permissions are equal
    public func permissionEqual(p1 : Permission, p2 : Permission) : Bool {
        switch (p1, p2) {
            case (#Read, #Read) { true };
            case (#Write, #Write) { true };
            case (#Admin, #Admin) { true };
            case (#Schema, #Schema) { true };
            case _ { false };
        };
    };
    
    // ===== Row-Level Security =====
    
    /// Apply row-level security to a filter
    public func applyRowLevelSecurity(
        filter : ?CommonTypes.FilterExpression,
        principal : Principal,
        tableName : Text,
        policies : [AccessPolicy]
    ) : ?CommonTypes.FilterExpression {
        // Find row-level policies for this principal and table
        let rowPolicies = Array.filter<AccessPolicy>(
            policies,
            func(policy) {
                principalMatches(policy.principal, principal) and 
                policy.resource.resourceType == #Row and
                policy.resource.resourceName == tableName and
                policy.conditions != null
            }
        );
        
        if (rowPolicies.size() == 0) {
            // No row-level policies, return original filter
            return filter;
        };
        
        // Convert policy conditions to filter expressions
        let policyFilters = Array.mapFilter<AccessPolicy, CommonTypes.FilterExpression>(
            rowPolicies,
            func(policy) {
                switch (policy.conditions) {
                    case (?condition) {
                        ?conditionToFilterExpression(condition)
                    };
                    case (null) {
                        null
                    };
                };
            }
        );
        
        if (policyFilters.size() == 0) {
            // No valid policy filters, return original filter
            return filter;
        };
        
        // Combine policy filters with OR (any matching policy is sufficient)
        let combinedPolicyFilter = if (policyFilters.size() == 1) {
            policyFilters[0]
        } else {
            #Or(policyFilters)
        };
        
        // Combine with original filter using AND
        switch (filter) {
            case (?originalFilter) {
                ?#And([originalFilter, combinedPolicyFilter])
            };
            case (null) {
                ?combinedPolicyFilter
            };
        }
    };
    
    /// Convert a filter condition to a filter expression
    public func conditionToFilterExpression(condition : FilterCondition) : CommonTypes.FilterExpression {
        switch (condition.operator) {
            case (#Equals) { #Equals(condition.column, condition.value) };
            case (#NotEquals) { #NotEquals(condition.column, condition.value) };
            case (#GreaterThan) { #GreaterThan(condition.column, condition.value) };
            case (#LessThan) { #LessThan(condition.column, condition.value) };
            case (#GreaterThanOrEqual) { #GreaterThanOrEqual(condition.column, condition.value) };
            case (#LessThanOrEqual) { #LessThanOrEqual(condition.column, condition.value) };
            case (#Like) {
                switch (condition.value) {
                    case (#Text(pattern)) { #Like(condition.column, pattern) };
                    case (_) { #Equals(condition.column, condition.value) }; // Fallback
                };
            };
            case (#In) {
                switch (condition.value) {
                    case (#Array(values)) { #In(condition.column, values) };
                    case (_) { #Equals(condition.column, condition.value) }; // Fallback
                };
            };
        };
    };
    
    // ===== Role-Based Access Control =====
    
    /// Get a role by name
    public func getRole(name : Text, roles : [(Text, Role)]) : ?Role {
        for ((roleName, role) in roles.vals()) {
            if (roleName == name) {
                return ?role;
            };
        };
        
        null
    };
    
    /// Get permissions for a principal
    public func getPermissionsForPrincipal(
        principal : Principal,
        resource : Resource,
        roleAssignments : [RoleAssignment],
        roles : [(Text, Role)]
    ) : [Permission] {
        // Find relevant role assignments for this principal and resource
        let relevantAssignments = Array.filter<RoleAssignment>(
            roleAssignments,
            func(assignment) {
                Principal.equal(assignment.principal, principal) and 
                resourceMatches(assignment.resource, resource) and
                (switch (assignment.expiresAt) {
                    case (?expiry) { Time.now() < expiry };
                    case (null) { true };
                })
            }
        );
        
        if (relevantAssignments.size() == 0) {
            return [];
        };
        
        // Collect permissions from all roles
        let permissionSet = HashMap.HashMap<Permission, Bool>(10, permissionEqual, permissionHash);
        
        for (assignment in relevantAssignments.vals()) {
            let roleOpt = getRole(assignment.role, roles);
            
            switch (roleOpt) {
                case (?role) {
                    for (permission in role.permissions.vals()) {
                        permissionSet.put(permission, true);
                    };
                };
                case (null) {
                    // Role not found, ignore
                };
            };
        };
        
        // Convert set to array
        let result = Buffer.Buffer<Permission>(permissionSet.size());
        for ((permission, _) in permissionSet.entries()) {
            result.add(permission);
        };
        
        Buffer.toArray(result)
    };
    
    /// Hash a permission
    public func permissionHash(permission : Permission) : Nat32 {
        let text = permissionToText(permission);
        Text.hash(text)
    };
}