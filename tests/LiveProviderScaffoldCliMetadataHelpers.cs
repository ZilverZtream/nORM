#nullable enable

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

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
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BLOB"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
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

    private static void SetupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupProviderSpecificColumnDiagnostics(connection, provider, kind, tableName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var providerColumnSql = kind switch
        {
            ProviderKind.SqlServer => $"{provider.Escape("Location")} geometry NULL",
            ProviderKind.Postgres => $"{provider.Escape("Address")} inet NULL",
            ProviderKind.MySql => $"{provider.Escape("Location")} POINT NULL",
            ProviderKind.Sqlite => $"{provider.Escape("Location")} GEOMETRY NULL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {providerColumnSql})");
    }

    private static void CleanupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        Execute(connection, DropTable(kind, tableName, table));
    }

    private static void SetupSafeProviderSpecificColumns(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupSafeProviderSpecificColumns(connection, provider, kind, tableName);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");

        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} UUID NOT NULL, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("XmlPayload")} XML NULL)");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {provider.Escape("XmlPayload")} xml NOT NULL)");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL)");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("FiscalYear")} YEAR NOT NULL, {provider.Escape("Status")} ENUM('draft','paid','cancelled') NOT NULL, {provider.Escape("Flags")} SET('read','write','admin') NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupSafeProviderSpecificColumns(
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
        var rawName = kind == ProviderKind.SqlServer ? "dbo." + tableName : tableName;
        Execute(connection, DropTable(kind, rawName, table));
    }

    private static void SetupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        CleanupWritableProviderSpecificDiagnostics(connection, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
            {
                var table = Qualified(provider, "dbo", tableName);
                var aliasType = Qualified(provider, "dbo", typeName);
                var decimalAliasType = Qualified(provider, "dbo", decimalTypeName);
                var binaryAliasType = Qualified(provider, "dbo", binaryTypeName);
                Execute(connection,
                    $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL",
                    $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL",
                    $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");
                break;
            }
            case ProviderKind.Postgres:
            {
                var table = Qualified(provider, "public", tableName);
                var domain = Qualified(provider, "public", typeName.ToLowerInvariant());
                var scoreDomain = Qualified(provider, "public", decimalTypeName.ToLowerInvariant());
                var scoreArrayDomain = Qualified(provider, "public", arrayTypeName);
                var statusEnum = Qualified(provider, "public", enumTypeName);
                var statusDomain = Qualified(provider, "public", enumDomainName);
                Execute(connection,
                    $"CREATE DOMAIN {domain} AS varchar(320) CHECK (VALUE LIKE '%@%')",
                    $"CREATE DOMAIN {scoreDomain} AS numeric(18,4) CHECK (VALUE >= 0)",
                    $"CREATE DOMAIN {scoreArrayDomain} AS integer[]",
                    $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active', 'archived')",
                    $"CREATE DOMAIN {statusDomain} AS {statusEnum}",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("Email")} {domain} NOT NULL, {provider.Escape("Score")} {scoreDomain} NOT NULL, {provider.Escape("Scores")} {scoreArrayDomain} NOT NULL, {provider.Escape("Status")} {statusDomain} NOT NULL)");
                break;
            }
            case ProviderKind.MySql:
            {
                var table = provider.Escape(tableName);
                Execute(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("UnsignedCount")} INT UNSIGNED NOT NULL, {provider.Escape("UnsignedTotal")} BIGINT UNSIGNED NOT NULL, {provider.Escape("UnsignedAmount")} DECIMAL(18,4) UNSIGNED NOT NULL)");
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
        }
    }

    private static void CleanupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    DropTable(kind, "dbo." + tableName, Qualified(provider, "dbo", tableName)),
                    $"IF TYPE_ID(N'dbo.{binaryTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", binaryTypeName)}",
                    $"IF TYPE_ID(N'dbo.{decimalTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", decimalTypeName)}",
                    $"IF TYPE_ID(N'dbo.{typeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", typeName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    DropTable(kind, "public." + tableName, Qualified(provider, "public", tableName)),
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", enumDomainName)}",
                    $"DROP TYPE IF EXISTS {Qualified(provider, "public", enumTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", arrayTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", decimalTypeName.ToLowerInvariant())}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", typeName.ToLowerInvariant())}");
                break;
            case ProviderKind.MySql:
                Execute(connection, DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
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
