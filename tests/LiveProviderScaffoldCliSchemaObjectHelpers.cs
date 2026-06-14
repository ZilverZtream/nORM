#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string nameIndex,
        string uniqueIndex)
    {
        CleanupIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {orderNo} int NOT NULL, {name} {text} NOT NULL)",
            $"CREATE INDEX {provider.Escape(nameIndex)} ON {table} ({name})",
            $"CREATE UNIQUE INDEX {provider.Escape(uniqueIndex)} ON {table} ({tenantId}, {orderNo})");
    }

    private static void CleanupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string partialIndex,
        string includedIndex,
        string descendingIndex)
    {
        CleanupProviderIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var activeType = kind == ProviderKind.SqlServer
            ? "bit"
            : kind == ProviderKind.Postgres
                ? "boolean"
                : "int";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {active} {activeType} NOT NULL, {includedValue} int NOT NULL)",
            $"CREATE INDEX {provider.Escape(descendingIndex)} ON {table} ({name} DESC)");

        if (kind != ProviderKind.MySql)
            Execute(connection, $"CREATE INDEX {provider.Escape(partialIndex)} ON {table} ({name}) WHERE {activePredicate}");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            Execute(connection, $"CREATE INDEX {provider.Escape(includedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static void CleanupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        CleanupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var touched = provider.Escape("Touched");
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection, $"CREATE TABLE {table} ({id} {intType} NOT NULL PRIMARY KEY, {touched} {intType} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, triggerName)}} ON {{table}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                        UPDATE target
                        SET {{touched}} = 1
                        FROM {{table}} AS target
                        INNER JOIN inserted AS source ON source.{{id}} = target.{{id}};
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                Execute(connection, $$"""
                    CREATE FUNCTION {{provider.Escape(functionName)}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        NEW."Touched" := 1;
                        RETURN NEW;
                    END
                    $$
                    """);
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {provider.Escape(functionName)}()");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW SET NEW.{touched} = 1");
                break;
            case ProviderKind.Sqlite:
                Execute(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(triggerName)}} AFTER INSERT ON {{table}}
                    BEGIN
                        UPDATE {{table}} SET {{touched}} = 1 WHERE {{id}} = NEW.{{id}};
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{triggerName}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, triggerName)}",
                    DropTable(kind, "dbo." + tableName, SqlServerQualified(provider, tableName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)} ON {provider.Escape(tableName)}",
                    $"DROP FUNCTION IF EXISTS {provider.Escape(functionName)}()",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            case ProviderKind.MySql:
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)}",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void SetupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string keylessTable)
    {
        CleanupKeylessWarning(connection, provider, keylessTable);

        var table = provider.Escape(keylessTable);
        var status = provider.Escape("Status");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({status} {text} NOT NULL)");
    }

    private static void CleanupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        string keylessTable)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(keylessTable)}");
    }

    private static void SetupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupEfStyleAliasTable(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
