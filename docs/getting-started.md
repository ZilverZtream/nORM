# Getting Started

This guide walks through installing nORM, configuring a database provider, and performing basic CRUD operations.

## Installation

```bash
dotnet add package nORM
```

## Creating a Context

```csharp
using nORM.Core;
using nORM.Providers;
using Microsoft.Data.SqlClient;

var provider = new SqlServerProvider();
var context = new DbContext("Server=.;Database=MyApp;Trusted_Connection=true", provider);
```

You can also pass an existing `DbConnection` instance to `DbContext` for full control over connection management.

## Defining Entities

```csharp
public class User
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

## Querying Data

```csharp
var users = await context.Query<User>()
    .Where(u => u.Name.StartsWith("John"))
    .OrderBy(u => u.CreatedAt)
    .ToListAsync();
```

Use `AsNoTracking()` for read‑only queries to avoid change tracking overhead.

## CRUD Operations

```csharp
var user = new User { Name = "John", Email = "john@example.com", CreatedAt = DateTime.Now };
await context.InsertAsync(user);

user.Email = "new@example.com";
await context.UpdateAsync(user);

await context.DeleteAsync(user);
```

Continue reading [Querying Data](querying.md) for more advanced query capabilities.
