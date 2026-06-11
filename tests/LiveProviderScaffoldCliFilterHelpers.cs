#nullable enable
using System;
using System.Data.Common;
using System.Linq;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        CleanupIdentifierCollisionScaffold(connection, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);

        var invalid = provider.Escape(invalidIdentifierTable);
        var dash = provider.Escape(collisionDashTable);
        var underscore = provider.Escape(collisionUnderscoreTable);
        var id = provider.Escape("Id");
        var invalidLeadingDigit = provider.Escape("1st-name");
        var hasSpace = provider.Escape("has space");
        var firstNameDash = provider.Escape("first-name");
        var firstNameUnderscore = provider.Escape("first_name");
        var toString = provider.Escape("ToString");
        var equals = provider.Escape("Equals");
        var value = provider.Escape("Value");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {invalid} ({id} {idType} NOT NULL PRIMARY KEY, {invalidLeadingDigit} {text} NOT NULL, {hasSpace} {idType} NULL, {firstNameDash} {text} NOT NULL, {firstNameUnderscore} {text} NOT NULL, {toString} {text} NOT NULL, {equals} {text} NOT NULL)",
            $"CREATE TABLE {dash} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)",
            $"CREATE TABLE {underscore} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)");
    }

    private static void CleanupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        Execute(connection,
            DropTable(kind, collisionUnderscoreTable, provider.Escape(collisionUnderscoreTable)),
            DropTable(kind, collisionDashTable, provider.Escape(collisionDashTable)),
            DropTable(kind, invalidIdentifierTable, provider.Escape(invalidIdentifierTable)));
    }

    private static void SetupProjectAwareScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupProjectAwareScaffold(connection, provider, kind, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {notes} {text} NULL)");
    }

    private static void CleanupProjectAwareScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
        => Execute(connection, DropTable(kind, tableName, provider.Escape(tableName)));

    private static void SetupTableFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        params string[] tableNames)
    {
        CleanupTableFilterValuesScaffold(connection, provider, kind, tableNames);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        foreach (var tableName in tableNames)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(tableName)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
        }
    }

    private static void CleanupTableFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        params string[] tableNames)
    {
        Execute(
            connection,
            tableNames
                .Reverse()
                .Select(tableName => DropTable(kind, tableName, provider.Escape(tableName)))
                .ToArray());
    }

    private static void SetupSchemaFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string csvSchemaOne,
        string csvSchemaTwo,
        string multiSchemaOne,
        string multiSchemaTwo,
        string skippedSchema,
        string csvTableOne,
        string csvTableTwo,
        string multiTableOne,
        string multiTableTwo,
        string skippedTable)
    {
        CleanupSchemaFilterValuesScaffold(
            connection,
            provider,
            kind,
            csvSchemaOne,
            csvSchemaTwo,
            multiSchemaOne,
            multiSchemaTwo,
            skippedSchema,
            csvTableOne,
            csvTableTwo,
            multiTableOne,
            multiTableTwo,
            skippedTable);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(csvTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(csvTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(multiTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(multiTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
            return;
        }

        CreateSchema(provider, connection, kind, csvSchemaOne);
        CreateSchema(provider, connection, kind, csvSchemaTwo);
        CreateSchema(provider, connection, kind, multiSchemaOne);
        CreateSchema(provider, connection, kind, multiSchemaTwo);
        CreateSchema(provider, connection, kind, skippedSchema);

        Execute(
            connection,
            $"CREATE TABLE {Qualified(provider, csvSchemaOne, csvTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, csvSchemaTwo, csvTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, multiSchemaOne, multiTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, multiSchemaTwo, multiTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, skippedSchema, skippedTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupSchemaFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string csvSchemaOne,
        string csvSchemaTwo,
        string multiSchemaOne,
        string multiSchemaTwo,
        string skippedSchema,
        string csvTableOne,
        string csvTableTwo,
        string multiTableOne,
        string multiTableTwo,
        string skippedTable)
    {
        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                DropTable(kind, multiTableTwo, provider.Escape(multiTableTwo)),
                DropTable(kind, multiTableOne, provider.Escape(multiTableOne)),
                DropTable(kind, csvTableTwo, provider.Escape(csvTableTwo)),
                DropTable(kind, csvTableOne, provider.Escape(csvTableOne)));
            return;
        }

        Execute(
            connection,
            DropTable(kind, skippedSchema + "." + skippedTable, Qualified(provider, skippedSchema, skippedTable)),
            DropTable(kind, multiSchemaTwo + "." + multiTableTwo, Qualified(provider, multiSchemaTwo, multiTableTwo)),
            DropTable(kind, multiSchemaOne + "." + multiTableOne, Qualified(provider, multiSchemaOne, multiTableOne)),
            DropTable(kind, csvSchemaTwo + "." + csvTableTwo, Qualified(provider, csvSchemaTwo, csvTableTwo)),
            DropTable(kind, csvSchemaOne + "." + csvTableOne, Qualified(provider, csvSchemaOne, csvTableOne)));

        DropSchema(provider, connection, kind, skippedSchema);
        DropSchema(provider, connection, kind, multiSchemaTwo);
        DropSchema(provider, connection, kind, multiSchemaOne);
        DropSchema(provider, connection, kind, csvSchemaTwo);
        DropSchema(provider, connection, kind, csvSchemaOne);
    }

    private static void SetupSchemaAndTableFilterUnionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string schemaTable,
        string explicitTable,
        string skippedTable)
    {
        CleanupSchemaAndTableFilterUnionScaffold(connection, provider, kind, schemaName, schemaTable, explicitTable, skippedTable);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(schemaTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(explicitTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
            return;
        }

        CreateSchema(provider, connection, kind, schemaName);
        Execute(
            connection,
            $"CREATE TABLE {Qualified(provider, schemaName, schemaTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {provider.Escape(explicitTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {provider.Escape(skippedTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupSchemaAndTableFilterUnionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string schemaTable,
        string explicitTable,
        string skippedTable)
    {
        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                DropTable(kind, explicitTable, provider.Escape(explicitTable)),
                DropTable(kind, schemaTable, provider.Escape(schemaTable)));
            return;
        }

        Execute(
            connection,
            DropTable(kind, skippedTable, provider.Escape(skippedTable)),
            DropTable(kind, explicitTable, provider.Escape(explicitTable)),
            DropTable(kind, schemaName + "." + schemaTable, Qualified(provider, schemaName, schemaTable)));

        DropSchema(provider, connection, kind, schemaName);
    }

    private static void CreateSchema(DatabaseProvider provider, DbConnection connection, ProviderKind kind, string schemaName)
    {
        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");
    }

    private static void DropSchema(DatabaseProvider provider, DbConnection connection, ProviderKind kind, string schemaName)
    {
        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

}
