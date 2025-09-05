# Advanced Features

nORM ships with enterprise capabilities that go beyond basic CRUD operations.

## Bulk Operations

```csharp
var users = GenerateUsers(10000);
await context.BulkInsertAsync(users);
```

Bulk update and delete helpers are also available.

## Caching and Connection Management

- Built-in connection pooling with support for read replicas and failover
- Query result caching with automatic invalidation

## LINQ to JSON and Temporal Queries

Work with JSON columns using JSON path expressions and query temporal tables for historical data.

## Interceptors and Global Filters

Hook into command execution or apply global filters such as soft deletes:

```csharp
context.AddInterceptor(new LoggingInterceptor());
context.AddGlobalFilter<User>(u => !u.IsDeleted);
```

## Source Generation

Compile-time source generators precompile queries for maximum performance and minimum allocations.

For a detailed list of features refer to the [project README](../README.md).
