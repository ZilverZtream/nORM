# Querying Data

nORM provides a full LINQ provider and utilities for working with raw SQL.

## LINQ Queries

```csharp
var users = await context.Query<User>()
    .Where(u => u.CreatedAt > DateTime.UtcNow.AddDays(-30))
    .OrderBy(u => u.Name)
    .ToListAsync();
```

### Joins and Grouping

```csharp
var stats = await context.Query<User>()
    .GroupBy(u => u.CreatedAt.Date)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .ToListAsync();
```

Joins across multiple entities are supported using standard LINQ syntax.

## Raw SQL

```csharp
var users = await context.FromSqlRawAsync<User>(
    "SELECT * FROM Users WHERE CreatedAt > @date",
    DateTime.UtcNow.AddDays(-7));
```

## Stored Procedures

```csharp
var results = await context.ExecuteStoredProcedureAsync<UserStats>(
    "sp_GetUserStatistics",
    new { StartDate = DateTime.UtcNow.AddMonths(-1) });
```

Output parameters are also supported via `ExecuteStoredProcedureWithOutputAsync`.
