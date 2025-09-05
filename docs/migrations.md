# Migrations

nORM includes a simple migration framework for versioning schema changes.

## Creating a Migration

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
```

## Applying Migrations

```csharp
var runner = new SqlServerMigrationRunner(connection, Assembly.GetExecutingAssembly());
await runner.ApplyMigrationsAsync();
```
