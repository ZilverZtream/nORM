#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task SetupWarningDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, KeylessTable, provider.Escape(KeylessTable)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var keyless = provider.Escape(KeylessTable);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");
        var externalId = provider.Escape("ExternalId");
        var payload = provider.Escape("Payload");
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape("DF_ScaffoldLiveWarning_Status")} DEFAULT ('new')"
            : "DEFAULT 'new'";

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL {defaultClause})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {keyless} ({externalId} {TextType(kind, 40)} NOT NULL, {payload} {TextType(kind, 80)} NOT NULL)");
    }

    private static async Task SetupKeylessDependentRelationshipAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownKeylessDependentRelationshipAsync(connection, provider, kind);

        var parent = provider.Escape(KeylessDependentParentTable);
        var dependent = provider.Escape(KeylessDependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var payload = provider.Escape("Payload");
        var fkName = provider.Escape(KeylessDependentFkName);

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {dependent} ({parentId} {IntType(kind)} NOT NULL, {payload} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {fkName} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static async Task SetupFeatureOwnedMetadataAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownFeatureOwnedMetadataAsync(connection, provider, kind);

        var table = provider.Escape(FeatureOwnedTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var status = provider.Escape("Status");
        var nameLength = provider.Escape("NameLength");
        var checkName = provider.Escape(FeatureOwnedCheckName);
        var defaultName = provider.Escape(FeatureOwnedDefaultName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE Latin1_General_BIN2 NOT NULL, {status} {TextType(kind, 32)} NOT NULL CONSTRAINT {defaultName} DEFAULT (LOWER(N'NEW')), {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {checkName} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE \"C\" NOT NULL, {status} {TextType(kind, 32)} NOT NULL DEFAULT lower('NEW'), {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {checkName} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, {status} VARCHAR(32) NOT NULL DEFAULT (LOWER('NEW')), {nameLength} INT GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {checkName} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE NOCASE NOT NULL, {status} {TextType(kind, 32)} NOT NULL DEFAULT (lower('NEW')), {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {checkName} CHECK (length({name}) > 0))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, createSql);
    }

    private static async Task SetupTriggerDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownTriggerDiagnosticsAsync(connection, provider, kind);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, TriggerDiagnosticsTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", TriggerDiagnosticsTable)
                : provider.Escape(TriggerDiagnosticsTable);
        var id = provider.Escape("Id");
        var touched = provider.Escape("Touched");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {touched} {IntType(kind)} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, TriggerDiagnosticsTrigger)}} ON {{table}}
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
                var function = Qualified(provider, "public", TriggerDiagnosticsPostgresFunction);
                await ExecuteAsync(connection, $$"""
                    CREATE FUNCTION {{function}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        NEW."Touched" := 1;
                        RETURN NEW;
                    END
                    $$
                    """);
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(TriggerDiagnosticsTrigger)} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(TriggerDiagnosticsTrigger)} BEFORE INSERT ON {table} FOR EACH ROW SET NEW.{touched} = 1");
                break;
            case ProviderKind.Sqlite:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(TriggerDiagnosticsTrigger)}} AFTER INSERT ON {{table}}
                    BEGIN
                        UPDATE {{table}} SET {{touched}} = 1 WHERE {{id}} = NEW.{{id}};
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static async Task TeardownFeatureOwnedMetadataAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, FeatureOwnedTable, provider.Escape(FeatureOwnedTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownTriggerDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            switch (kind)
            {
                case ProviderKind.SqlServer:
                    await ExecuteAsync(connection,
                        $"IF OBJECT_ID(N'dbo.{TriggerDiagnosticsTrigger}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, "dbo." + TriggerDiagnosticsTable, SqlServerQualified(provider, TriggerDiagnosticsTable)));
                    break;
                case ProviderKind.Postgres:
                    var table = Qualified(provider, "public", TriggerDiagnosticsTable);
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)} ON {table}");
                    await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {Qualified(provider, "public", TriggerDiagnosticsPostgresFunction)}()");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, table));
                    break;
                case ProviderKind.MySql:
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, provider.Escape(TriggerDiagnosticsTable)));
                    break;
                case ProviderKind.Sqlite:
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, provider.Escape(TriggerDiagnosticsTable)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
