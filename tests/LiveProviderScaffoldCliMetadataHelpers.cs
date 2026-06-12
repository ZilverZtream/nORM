#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string checkName,
        string defaultName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var payload = provider.Escape("Payload");
        var createdAt = provider.Escape("CreatedAt");
        var nameLength = provider.Escape("NameLength");
        var check = provider.Escape(checkName);
        var defaultConstraint = provider.Escape(defaultName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) COLLATE Latin1_General_BIN2 NOT NULL CONSTRAINT {defaultConstraint} DEFAULT ('new'), {payload} varbinary(4) NOT NULL DEFAULT 0xDEADBEEF, {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {check} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) COLLATE \"C\" NOT NULL DEFAULT 'new', {payload} bytea NOT NULL DEFAULT '\\xDEADBEEF'::bytea, {createdAt} timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'utc'), {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {check} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'new', {payload} varbinary(4) NOT NULL DEFAULT 0xDEADBEEF, {nameLength} int GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {check} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {name} TEXT COLLATE NOCASE NOT NULL DEFAULT 'new', {payload} BLOB NOT NULL DEFAULT X'DEADBEEF', {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {check} CHECK (length({name}) > 0))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, createSql);
    }

    private static void SetupUnnamedCheckConstraintMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} int NOT NULL, CHECK ({amount} > 0))");
    }

    private static void SetupUnnamedUniqueConstraintIndex(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {code} {text} NOT NULL UNIQUE, {name} {text} NOT NULL)");
    }

    private static void CleanupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupDatabaseComments(connection, provider, kind, tableName);

        const string tableComment = "Table <summary> & description";
        const string columnComment = "Name <tag> & details";
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL COMMENT {SqlLiteral(columnComment)}) COMMENT={SqlLiteral(tableComment)}");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(tableComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(columnComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"COMMENT ON TABLE {table} IS {SqlLiteral(tableComment)}",
                    $"COMMENT ON COLUMN {table}.{name} IS {SqlLiteral(columnComment)}");
                break;
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        Execute(connection, $"DROP TABLE IF EXISTS {table}");
    }

    private static void SetupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupScalarFacets(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
    }

    private static void SetupTemporalStoreTypes(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupTemporalStoreTypes(connection, provider, kind, tableName);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");
        var businessDate = provider.Escape("BusinessDate");
        var startsAt = provider.Escape("StartsAt");
        var createdAt = provider.Escape("CreatedAt");
        var offsetAt = provider.Escape("OffsetAt");
        var sql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time NULL, {createdAt} datetime2 NOT NULL, {offsetAt} datetimeoffset NULL)",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time without time zone NULL, {createdAt} timestamp without time zone NOT NULL, {offsetAt} timestamp with time zone NULL)",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL)",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL, {offsetAt} DATETIMEOFFSET NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, sql);
    }

    private static void CleanupTemporalStoreTypes(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        Execute(connection, $"DROP TABLE IF EXISTS {table}");
    }

    private static void CleanupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

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
