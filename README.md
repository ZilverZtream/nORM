# nORM - The Norm
*A high-performance, enterprise-grade ORM for .NET*

[![NuGet](https://img.shields.io/nuget/v/nORM.svg)](https://www.nuget.org/packages/nORM/)
[![Downloads](https://img.shields.io/nuget/dt/nORM.svg)](https://www.nuget.org/packages/nORM/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

nORM (The Norm) is a modern, high-performance Object-Relational Mapping (ORM) library for .NET that combines the best of Entity Framework's features with Dapper's speed. Built for enterprise applications that demand both developer productivity and exceptional performance.

## 🚀 Why nORM?

- **🏎️ Blazing Fast**: Dapper-speed IL materialization with zero-allocation query execution
- **🔍 Full LINQ Support**: Complete LINQ provider with joins, grouping, subqueries, and complex projections
- **📦 Migrations**: Versioned schema changes with an easy migration runner
- **📊 Bulk Operations**: High-performance bulk insert, update, and delete operations
- **📄 Raw SQL & Stored Procedures**: Execute raw SQL queries and stored procedures with ease
- **♻️ Connection Pooling**: Built-in pooling for efficient connection management
- **🔧 Provider Agnostic**: Support for SQL Server, PostgreSQL, SQLite, and MySQL
- **🔗 Smart Conventions**: Automatic relationship discovery using standard naming patterns
- **🧩 Fluent Configuration**: Configure models with a flexible fluent API
- **🎯 Simple API**: Clean, intuitive API that feels familiar to EF users
- **🔨 Database Scaffolding**: Reverse-engineer existing databases into entity classes and a DbContext
- **📄 LINQ to JSON**: Query JSON columns using JSON path expressions
- **⚙️ Zero-Configuration**: Auto-discover schemas and query tables without entity classes
- **📈 Window Functions**: Use ROW_NUMBER, RANK, LAG, and more through LINQ
- **⏳ Temporal Queries**: Query historical data and tag-based versioning
- **🔌 Advanced Connection Management**: Connection pooling, read replicas, and failover
- **🧠 Query Compilation**: Compile LINQ queries for reuse
- **🔗 Navigation Properties**: Automatic lazy loading and batched retrieval
- **🛠️ Interceptors**: Hook into command execution and SaveChanges
- **🏢 Multi-Tenancy**: Tenant-aware filtering and caching
- **🗃️ Advanced Caching**: Cache query results with invalidation
- **🔁 Retry Policies**: Built-in strategies for transient fault handling
- **🚫 Global Query Filters**: Apply filters such as soft deletes globally
- **🧩 Source Generation**: Compile-time query optimization
- **💧 Streaming Queries**: IAsyncEnumerable support for large result sets

## 📦 Installation

```bash
dotnet add package nORM
```

## 🎯 Quick Start

### Basic Setup

```csharp
using nORM.Core;
using nORM.Providers;
using Microsoft.Data.SqlClient;

// Create context and let it manage the connection
var provider = new SqlServerProvider();
var context = new DbContext("Server=.;Database=MyApp;Trusted_Connection=true", provider);

// Or create and pass an existing connection
// using var connection = new SqlConnection("Server=.;Database=MyApp;Trusted_Connection=true");
// var context = new DbContext(connection, provider);

// Define your entities
public class User
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

### LINQ Queries

```csharp
// Simple queries
var users = await context.Query<User>()
    .Where(u => u.Name.StartsWith("John"))
    .OrderBy(u => u.CreatedAt)
    .ToListAsync();

// For read-only queries, disable tracking to boost performance
var readOnlyUsers = await context.Query<User>()
    .AsNoTracking()
    .Where(u => u.Name.StartsWith("John"))
    .ToListAsync();

// Complex queries with projections
var userSummary = await context.Query<User>()
    .Where(u => u.CreatedAt > DateTime.Now.AddDays(-30))
    .Select(u => new { u.Name, u.Email })
    .ToListAsync();

// Joins and grouping
var userStats = await context.Query<User>()
    .GroupBy(u => u.CreatedAt.Date)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .ToListAsync();
```

### CRUD Operations

```csharp
// Insert
var user = new User { Name = "John Doe", Email = "john@example.com", CreatedAt = DateTime.Now };
await context.InsertAsync(user); // Auto-populates Id

// Update
user.Email = "newemail@example.com";
await context.UpdateAsync(user);

// Delete
await context.DeleteAsync(user);
```

### Bulk Operations

```csharp
// Bulk insert 10,000 records efficiently
var users = GenerateUsers(10000);
await context.BulkInsertAsync(users);

// Bulk update
await context.BulkUpdateAsync(modifiedUsers);

// Bulk delete
await context.BulkDeleteAsync(usersToDelete);
```

### Scaffolding from Existing Database

```csharp
using nORM.Scaffolding;

await DatabaseScaffolder.ScaffoldAsync(
    connection,
    provider,
    outputDirectory: "Models",
    namespaceName: "MyApp.Models",
    contextName: "MyAppContext");
```

This generates entity classes and a DbContext from the existing database schema,
providing a quick starting point for new projects.

## ✨ Advanced Features

### LINQ to JSON

```csharp
var results = await context.Query<Product>()
    .Where(p => Json.Value<string>(p.Metadata, "$.category") == "electronics")
    .ToListAsync();
```

### Zero-Configuration & Auto-Discovery

```csharp
// Query any table without defining entity classes
var users = await context.Query("Users").ToListAsync();

// Auto-scaffold entire database
await DatabaseScaffolder.ScaffoldAsync(connection, provider, outputDir, "MyApp.Entities");
```

### Window Functions

```csharp
var ranked = await context.Query<Sale>()
    .WithRowNumber((sale, rowNum) => new { sale.Amount, RowNumber = rowNum })
    .WithRank((sale, rank) => new { sale.Amount, Rank = rank })
    .ToListAsync();
```

### Temporal Queries & Versioning

```csharp
// Query data as it existed at a specific time
var historical = await context.Query<Product>()
    .AsOf(DateTime.Parse("2023-01-01"))
    .ToListAsync();

// Query data as of a tagged point in time
await context.CreateTagAsync("release-v1.0");
var releaseData = await context.Query<Product>()
    .AsOf("release-v1.0")
    .ToListAsync();
```

### Advanced Connection Management

```csharp
var topology = new DatabaseTopology();
topology.AddNode("primary", "Server=.;Database=MyApp;Trusted_Connection=true");
topology.AddReadReplica("replica", "Server=replica;Database=MyApp;Trusted_Connection=true");

var manager = new ConnectionManager(topology);
await using var readConnection = await manager.GetReadConnectionAsync();
```

### Query Compilation

```csharp
var compiled = Norm.CompileQuery<MyContext, int, User>(
    (ctx, id) => ctx.Query<User>().Where(u => u.Id == id)
);

var user = await compiled(context, 123);
```

### Navigation Properties & Lazy Loading

```csharp
var order = await context.Query<Order>().FirstAsync();
// Accessing navigation property triggers automatic lazy loading
var items = order.Items;
```

### Interceptors

```csharp
public class LoggingInterceptor : IDbCommandInterceptor
{
    public Task BeforeExecuteAsync(DbCommand command)
    {
        Console.WriteLine(command.CommandText);
        return Task.CompletedTask;
    }
}

options.AddInterceptor(new LoggingInterceptor());
```

### Advanced Caching

```csharp
var products = await context.Query<Product>()
    .Where(p => p.Category == "Electronics")
    .Cacheable(TimeSpan.FromMinutes(30))
    .ToListAsync();
```

### Global Query Filters

```csharp
options.AddGlobalFilter<ISoftDeletable>(e => !e.IsDeleted);
```

### Source Generation

Compile-time query optimization through the `SourceGeneration` tooling allows
generating high-performance query pipelines during build.

### Streaming Queries

```csharp
await foreach (var user in context.Query<User>().AsAsyncEnumerable())
{
    // process each user without loading entire result set into memory
}
```

## 🏢 Enterprise Features

### Multi-Tenancy

```csharp
public class TenantProvider : ITenantProvider
{
    public object GetCurrentTenantId() => HttpContext.Current.User.TenantId;
}

var options = new DbContextOptions
{
    TenantProvider = new TenantProvider(),
    TenantColumnName = "TenantId"
};

var context = new DbContext(connection, provider, options);
// All queries automatically filtered by tenant
```

### Connection Resilience

```csharp
var retryPolicy = new RetryPolicy
{
    MaxRetries = 3,
    BaseDelay = TimeSpan.FromSeconds(1),
    ShouldRetry = ex => ex is SqlException sqlEx && sqlEx.Number == 1205 // Deadlock
};

var options = new DbContextOptions { RetryPolicy = retryPolicy };
var context = new DbContext(connection, provider, options);
```

### Connection Pooling

```csharp
// Create a pool that manages SQL Server connections
var pool = new ConnectionPool(
    () => new SqlConnection("Server=.;Database=MyApp;Trusted_Connection=true"),
    new ConnectionPoolOptions
    {
        MaxPoolSize = 50,
        MinPoolSize = 5,
        ConnectionIdleLifetime = TimeSpan.FromMinutes(5)
    });

await using var pooledConnection = await pool.RentAsync();
var context = new DbContext(pooledConnection, provider);
// Disposing the connection or context returns it to the pool
```

### Advanced Logging

```csharp
var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger("nORM");

var options = new DbContextOptions { Logger = logger };
```

### Fluent Configuration

```csharp
var options = new DbContextOptions
{
    OnModelCreating = modelBuilder =>
    {
        modelBuilder.Entity<User>()
            .ToTable("ApplicationUsers")
            .HasKey(u => u.Id)
            .Property(u => u.Email).HasColumnName("EmailAddress");
    }
};
```

## 🔧 Database Providers

nORM supports multiple database providers out of the box:

```csharp
// SQL Server
var provider = new SqlServerProvider();

// PostgreSQL
var provider = new PostgresProvider(new NpgsqlParameterFactory());

// SQLite
var provider = new SqliteProvider();

// MySQL
var provider = new MySqlProvider(new MySqlParameterFactory());
```

## 📊 Raw SQL & Stored Procedures

```csharp
// Raw SQL queries
var users = await context.FromSqlRawAsync<User>(
    "SELECT * FROM Users WHERE CreatedAt > @date", 
    DateTime.Now.AddDays(-7));

// Stored procedures
var results = await context.ExecuteStoredProcedureAsync<UserStats>(
    "sp_GetUserStatistics",
    new { StartDate = DateTime.Now.AddMonths(-1) });

// Stored procedures with OUTPUT parameters
var spResult = await context.ExecuteStoredProcedureWithOutputAsync<UserStats>(
    "sp_GetUserStatistics",
    parameters: new { StartDate = DateTime.Now.AddMonths(-1) },
    outputParameters: new[] { new OutputParameter("TotalUsers", DbType.Int32) });
var totalUsers = (int)spResult.OutputParameters["TotalUsers"]!;
```

## 🔄 Migrations

```csharp
public class CreateUsersTable : Migration
{
    public CreateUsersTable() : base(20240101001, "CreateUsersTable") { }

    public override void Up(DbConnection connection, DbTransaction transaction)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = @"
            CREATE TABLE Users (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Name NVARCHAR(255) NOT NULL,
                Email NVARCHAR(255) NOT NULL,
                CreatedAt DATETIME2 NOT NULL
            )";
        cmd.ExecuteNonQuery();
    }

    public override void Down(DbConnection connection, DbTransaction transaction)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "DROP TABLE Users";
        cmd.ExecuteNonQuery();
    }
}

// Apply migrations
var runner = new SqlServerMigrationRunner(connection, Assembly.GetExecutingAssembly());
await runner.ApplyMigrationsAsync();
```

## ⚡ Performance

nORM is built for performance:

- **IL Materialization**: Hand-crafted IL generation for zero-allocation object materialization
- **Query Caching**: Compiled query plans are cached for maximum performance
- **Bulk Operations**: Native bulk operations for each database provider
- **Minimal Allocations**: Designed to minimize garbage collection pressure

### Benchmarks

| Method               | Mean        | Memory    |
|---------------------|------------:|----------:|
| Query_Simple_nORM   |    62.54 us |  12.03 KB |
| Count_nORM          |    66.75 us |   7.32 KB |
| Query_Simple_RawAdo |    70.29 us |    6.1 KB |
| Query_Simple_Dapper |    77.38 us |   6.76 KB |
| Query_Simple_EfCore |    81.94 us |   8.89 KB |
| Count_Dapper        |    83.41 us |   1.14 KB |
| Insert_Single_nORM  |    87.20 us |  12.48 KB |
| Count_EfCore        |   104.95 us |    4.7 KB |
| Query_Join_Dapper   |   106.25 us |  14.43 KB |
| Query_Complex_nORM  |   116.82 us |   9.66 KB |
| Query_Join_nORM     |   131.11 us |  41.37 KB |
| Query_Complex_Dapper |   189.78 us |  13.21 KB |
| Query_Complex_EfCore |   191.30 us |  15.88 KB |
| Query_Join_EfCore   |   214.23 us |  55.67 KB |
| BulkInsert_nORM     | 1,497.19 us | 336.18 KB |
| Insert_Single_RawAdo | 5,014.80 us |   2.2 KB |
| Insert_Single_Dapper | 5,060.18 us |   4.96 KB |
| Insert_Single_EfCore | 5,408.51 us |  67.53 KB |
| BulkInsert_Dapper   | 5,905.85 us | 409.34 KB |
| BulkInsert_EfCore   | 8,896.02 us | 1300.15 KB |

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

1. Clone the repository
2. Install .NET 8.0 SDK
3. Run `dotnet restore`
4. Run `dotnet build`
5. Run tests: `dotnet test`

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by the performance of Dapper and the features of Entity Framework
- Built with ❤️ for the .NET community

## 📞 Support

- 📖 [Documentation](https://github.com/zilverztream/nORM/wiki)
- 🐛 [Issues](https://github.com/zilverztream/nORM/issues)
- 💬 [Discussions](https://github.com/zilverztream/nORM/discussions)

---

*nORM - Setting the standard for .NET ORMs* ⭐