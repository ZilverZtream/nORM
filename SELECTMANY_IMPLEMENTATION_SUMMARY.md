# SelectMany Translation Implementation Summary

## Overview
This implementation enhances nORM's `ExpressionToSqlVisitor` and `QueryTranslator` to provide robust SelectMany translation, supporting collection flattening, filtered navigation properties, and transparent identifier handling.

## Acceptance Criteria Validation

### ✅ 1. Flattening
**Requirement**: `db.Blogs.SelectMany(b => b.Posts)` translates to `SELECT p.* FROM Blogs b INNER JOIN Posts p ON b.Id = p.BlogId`

**Implementation**:
- Location: `QueryTranslator.HandleSelectMany()` (lines 900-905, 961-1043)
- Detection: Checks if `collectionSelector.Body` is a `MemberExpression` referencing a navigation property
- SQL Generation: Creates `INNER JOIN` using foreign key relationship from `TableMapping.Relations`
- Example output: `SELECT [Post columns] FROM Blog T0 INNER JOIN Post T1 ON T0.Id = T1.BlogId`

**Code Path**:
```csharp
if (collectionSelector.Body is MemberExpression memberExpr &&
    outerMapping.Relations.TryGetValue(memberExpr.Member.Name, out relation))
{
    // Generate INNER JOIN with FK relationship
}
```

### ✅ 2. Filtered Flattening
**Requirement**: `db.Blogs.SelectMany(b => b.Posts.Where(p => p.Active))` translates to a JOIN with a WHERE clause

**Implementation**:
- Location: `QueryTranslator.HandleSelectMany()` (lines 906-915, 1011-1028)
- Detection: Identifies `MethodCallExpression` for `Where` on a navigation property member
- Filter Extraction: Extracts the lambda predicate from the Where call
- SQL Generation: Creates `INNER JOIN` then applies filter as WHERE clause
- Example output: `SELECT [columns] FROM Blog T0 INNER JOIN Post T1 ON T0.Id = T1.BlogId WHERE (T1.Active = 1)`

**Code Path**:
```csharp
else if (collectionSelector.Body is MethodCallExpression methodCall &&
         methodCall.Method.Name == "Where" &&
         methodCall.Arguments[0] is MemberExpression navMember &&
         outerMapping.Relations.TryGetValue(navMember.Member.Name, out relation))
{
    navigationMember = navMember;
    filterPredicate = StripQuotes(methodCall.Arguments[1]) as LambdaExpression;
}

// Later: Apply filter
if (filterPredicate != null)
{
    var filterVisitor = FastExpressionVisitorPool.Get(...);
    var filterSql = filterVisitor.Translate(filterPredicate.Body);
    _where.Append($"({filterSql})");
}
```

### ✅ 3. Transparent Identifier Handling
**Requirement**: Handle compiler-generated transparent identifiers (e.g., `<>h__TransparentIdentifier0`) for chained operations

**Implementation**:
- Location: `QueryTranslator.HandleSelectMany()` (lines 967-974, 1050-1057)
- Parameter Registration: Both result selector parameters are registered in `_correlatedParams` dictionary
- Scope Propagation: Dictionary is passed to child visitors for nested expression translation
- Example: `SelectMany(b => b.Posts, (b, p) => new { Blog = b, Post = p }).Select(x => new { x.Blog.Name, x.Post.Title })`

**Code Path**:
```csharp
// Register both result selector parameters for transparent identifier support
if (resultSelector != null && resultSelector.Parameters.Count > 1)
{
    if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
        _correlatedParams[resultSelector.Parameters[0]] = (outerMapping, outerAlias);
    if (!_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
        _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
}
```

**How it Works**:
1. Result selector creates anonymous type: `new { Blog = b, Post = p }`
2. Both `b` and `p` parameters are mapped to their table aliases (T0, T1)
3. Subsequent `Select` operations can access `x.Blog` and `x.Post`
4. `ExpressionToSqlVisitor.VisitMember` resolves these through `_parameterMappings`

### ✅ 4. Scope Management
**Requirement**: Update `_parameterMappings` to maintain multiple active table aliases for parent and child access

**Implementation**:
- Dictionary: `_correlatedParams` maintains `(TableMapping, Alias)` for each parameter
- Context Passing: `VisitorContext` propagates mappings to nested visitors
- Column Resolution: `ExpressionToSqlVisitor.VisitMember` uses mappings to generate qualified column names

**Example**:
```
_correlatedParams:
  b → (BlogMapping, "T0")
  p → (PostMapping, "T1")

When translating: b.Name → T0.Name
When translating: p.Title → T1.Title
```

## Additional Enhancements

### 1. Cross Join Support
- Handles non-navigation SelectMany (e.g., `blogs.SelectMany(b => categories)`)
- Generates: `FROM Blog T0 CROSS JOIN Category T1`

### 2. Comprehensive Documentation
- Added XML documentation to `HandleSelectMany` method
- Includes examples for all supported scenarios
- Documents transparent identifier mechanism

### 3. Test Coverage
- Created `SelectManyTests.cs` with 9 comprehensive test cases:
  - Simple navigation flattening
  - Navigation with result selector
  - Cross join scenarios
  - Filtered navigation properties
  - Chained operations with transparent identifiers
  - Real-world scenarios (customers in London)

## Files Modified

1. **src/nORM/Query/QueryTranslator.cs**
   - Enhanced `HandleSelectMany()` method
   - Added filtered navigation detection
   - Improved parameter registration for transparent identifiers
   - Added comprehensive XML documentation

2. **tests/SelectManyTests.cs** (New)
   - 9 test methods covering all scenarios
   - Both translation tests and execution tests
   - Cross-provider tests (SQLite, SQL Server, MySQL)

## Technical Details

### Navigation Property Detection
```csharp
// Simple: b => b.Posts
collectionSelector.Body is MemberExpression memberExpr

// Filtered: b => b.Posts.Where(p => p.Active)
collectionSelector.Body is MethodCallExpression { Method.Name = "Where" }
```

### Foreign Key Resolution
Uses `TableMapping.Relations` to find:
- `PrincipalKey`: Parent table's key column
- `ForeignKey`: Child table's foreign key column
- `DependentType`: Child entity type

### Parameter Lifetime
1. Collection selector parameter registered at line 888-889
2. Result selector parameters registered at lines 967-974
3. Filter predicate parameter registered at line 1014-1015
4. All mappings passed to child visitors via `VisitorContext`

## Performance Considerations

- Uses object pooling for `ExpressionToSqlVisitor` instances
- Reuses `_correlatedParams` dictionary across visitor invocations
- Minimal allocations for common scenarios
- Efficient SQL generation with `OptimizedSqlBuilder`

## Backward Compatibility

All changes are additive and backward compatible:
- Existing SelectMany queries continue to work
- No breaking changes to public APIs
- Enhances existing functionality without removing features

## Testing Strategy

1. **Unit Tests**: Validate SQL generation without database
2. **Integration Tests**: Execute queries against real database
3. **Cross-Provider Tests**: Ensure compatibility with SQLite, SQL Server, MySQL
4. **Edge Cases**: Filtered navigation, chained operations, transparent identifiers
