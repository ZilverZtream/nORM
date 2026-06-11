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
    private static void SetupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        CleanupRoutineStub(connection, provider, kind, routineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT AS SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Routine <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'PROCEDURE', @level1name=" + SqlServerLiteral(routineName));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(tenantId integer) RETURNS TABLE(\"Id\" integer, \"Name\" text) LANGUAGE SQL AS $$ SELECT tenantId, 'ok'::text $$",
                    $"COMMENT ON FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(integer) IS {SqlLiteral("Routine <summary> & description")}");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT) COMMENT {SqlLiteral("Routine <summary> & description")} SELECT tenantId AS Id, 'ok' AS Name");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}",
            ProviderKind.Postgres => $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer)",
            ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.")
        });
    }

    private static void SetupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        CleanupAdvancedRoutineStub(connection, provider, kind, routineName, tableFunctionName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)} (@customerId INT) RETURNS INT AS BEGIN RETURN @customerId + 7; END",
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)} (@tenantId INT) RETURNS TABLE AS RETURN SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(ids integer[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) $$");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape(routineName)}(customer_id INT UNSIGNED, max_id BIGINT UNSIGNED, {provider.Escape("rank")} SMALLINT UNSIGNED, flag TINYINT UNSIGNED) RETURNS INT DETERMINISTIC NO SQL RETURN customer_id");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{tableFunctionName}', N'IF') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'FN') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer[], uuid)");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void SetupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        CleanupRoutineOutputAndNonQueryWrappers(connection, provider, kind, routineName, nonQueryRoutineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT, @total DECIMAL(18,2) OUTPUT, @message NVARCHAR(32) OUTPUT AS BEGIN SET @total = 12.34; SET @message = N'ok'; SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name; END",
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)} @tenantId INT, @status NVARCHAR(32) OUTPUT AS BEGIN SET NOCOUNT ON; SET @status = N'ok'; DECLARE @ignored INT = @tenantId; END");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT, OUT total DECIMAL(18,2), INOUT message VARCHAR(32)) BEGIN SET total = 12.34; SET message = CONCAT(COALESCE(message, ''), 'ok'); SELECT tenantId AS Id, 'ok' AS Name; END");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void CleanupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{nonQueryRoutineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void SetupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        CleanupSequenceStub(connection, provider, kind, sequenceName);

        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            ProviderKind.Postgres => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Sequence <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'SEQUENCE', @level1name=" + SqlServerLiteral(sequenceName),
            ProviderKind.Postgres => $"COMMENT ON SEQUENCE {qualifiedName} IS {SqlLiteral("Sequence <summary> & description")}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void CleanupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{sequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {qualifiedName}",
            ProviderKind.Postgres => $"DROP SEQUENCE IF EXISTS {qualifiedName}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void SetupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string dependentTable)
    {
        CleanupFilteredRelationship(connection, provider, principalTable, dependentTable);

        var principal = provider.Escape(principalTable);
        var dependent = provider.Escape(dependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {principal} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {dependent} ({id} int NOT NULL PRIMARY KEY, {parentId} int NOT NULL, {name} {text} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {principal} ({id}))");
    }

    private static void CleanupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string principalTable,
        string dependentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(dependentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(principalTable)}");
    }

    private static void SetupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        CleanupViewQueryArtifact(connection, provider, kind, baseTable, viewName);

        var table = provider.Escape(baseTable);
        var view = provider.Escape(viewName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void AssertViewQueryArtifactCommentDocumentation(ProviderKind kind, string viewCode)
    {
        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            Assert.Contains("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// Name &lt;view&gt; &amp; details", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// <remarks>Maps to column Name</remarks>", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Name <view> & details", viewCode, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("/// Maps to column Name", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// VIEW", viewCode, StringComparison.Ordinal);
        }
    }

    private static void CleanupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        Execute(connection,
            DropView(kind, viewName, provider.Escape(viewName)),
            $"DROP TABLE IF EXISTS {provider.Escape(baseTable)}");
    }

    private static void SetupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        CleanupProviderQueryArtifact(connection, provider, kind, baseName, artifactName);

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE VIRTUAL TABLE {provider.Escape(artifactName)} USING fts5({provider.Escape("Content")})");
                break;
            case ProviderKind.SqlServer:
            {
                var baseTable = Qualified(provider, "dbo", baseName);
                var synonym = Qualified(provider, "dbo", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName) + ", @level2type=N'COLUMN', @level2name=N'Name'",
                    $"CREATE SYNONYM {synonym} FOR {baseTable}");
                break;
            }
            case ProviderKind.Postgres:
            {
                var baseTable = Qualified(provider, "public", baseName);
                var matView = Qualified(provider, "public", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} integer NOT NULL PRIMARY KEY, {name} text NOT NULL)",
                    $"CREATE MATERIALIZED VIEW {matView} AS SELECT {id}, {name} FROM {baseTable}",
                    $"COMMENT ON MATERIALIZED VIEW {matView} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {matView}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            }
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {provider.Escape(baseName)} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) NOT NULL)",
                    $"CREATE VIEW {provider.Escape(artifactName)} AS SELECT {id}, {name} FROM {provider.Escape(baseName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, PostgreSQL, and MySQL.");
        }
    }

    private static void CleanupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection, DropTable(kind, artifactName, provider.Escape(artifactName)));
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{artifactName}', N'SN') IS NOT NULL DROP SYNONYM {Qualified(provider, "dbo", artifactName)}",
                    DropTable(kind, "dbo." + baseName, Qualified(provider, "dbo", baseName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP MATERIALIZED VIEW IF EXISTS {Qualified(provider, "public", artifactName)}",
                    DropTable(kind, "public." + baseName, Qualified(provider, "public", baseName)));
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    DropView(kind, artifactName, provider.Escape(artifactName)),
                    DropTable(kind, baseName, provider.Escape(baseName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, PostgreSQL, and MySQL.");
        }
    }

    private static void SetupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        CleanupDefaultDiscoveryViewQueryArtifact(connection, provider, kind, schemaName, baseTable, viewName);

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");

        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
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
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void CleanupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
        var rawTable = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + baseTable
            : baseTable;
        var rawView = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + viewName
            : viewName;

        Execute(connection,
            DropView(kind, rawView, view),
            DropTable(kind, rawTable, table));

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

}
