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
    private static async Task SetupRoutineAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoutineAsync(connection, provider, kind);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineName)} @tenantId INT AS SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(RoutineName)}(tenantId integer) RETURNS TABLE(\"Id\" integer, \"Name\" text) LANGUAGE SQL AS $$ SELECT tenantId, 'ok'::text $$");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape(RoutineName)}(IN tenantId INT) SELECT tenantId AS Id, 'ok' AS Name");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffolding live test only targets providers with routine support.");
        }
    }

    private static async Task SetupRoutineWithOutputAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoutineWithOutputAsync(connection, provider, kind);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineOutputName)} @tenantId INT, @total DECIMAL(18,2) OUTPUT, @message NVARCHAR(32) OUTPUT AS BEGIN SET @total = 12.34; SET @message = N'ok'; SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name; END");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape(RoutineOutputName)}(IN tenantId INT, OUT total DECIMAL(18,2), INOUT message VARCHAR(32)) BEGIN SET total = 12.34; SET message = CONCAT(COALESCE(message, ''), 'ok'); SELECT tenantId AS Id, 'ok' AS Name; END");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output scaffolding live test only targets providers with OUT parameter support in this test harness.");
        }
    }

    private static async Task SetupSqlServerNonQueryRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerNonQueryRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineNonQueryName)} @tenantId INT, @status NVARCHAR(32) OUTPUT AS BEGIN SET NOCOUNT ON; SET @status = N'ok'; DECLARE @ignored INT = @tenantId; END");
    }

    private static async Task SetupSqlServerTableValuedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerTableValuedParameterRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE TYPE {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)} AS TABLE ({provider.Escape("ProductId")} INT NOT NULL, {provider.Escape("Quantity")} INT NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineTableValuedParameterName)} @tenantId INT, @items {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)} READONLY AS SELECT @tenantId AS Id, COUNT(*) AS LineCount FROM @items");
    }

    private static async Task SetupSqlServerFunctionRoutinesAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerFunctionRoutinesAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerScalarFunctionName)} (@customerId INT) RETURNS INT AS BEGIN RETURN @customerId + 7; END");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerTableValuedFunctionName)} (@tenantId INT) RETURNS TABLE AS RETURN SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
    }

    private static async Task SetupSequenceAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSequenceAsync(connection, provider, kind);

        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(SequenceName)
            : provider.Escape("public") + "." + provider.Escape(SequenceName);
        var sql = kind switch
        {
            ProviderKind.SqlServer => $"CREATE SEQUENCE {qualifiedName} AS BIGINT START WITH 42 INCREMENT BY 1",
            ProviderKind.Postgres => $"CREATE SEQUENCE {qualifiedName} AS integer START WITH 42 INCREMENT BY 1",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffolding live test only targets SQL Server and PostgreSQL.")
        };
        await ExecuteAsync(connection, sql);
    }

    private static async Task SetupPostgresSetReturningRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresSetReturningRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(PostgresSetReturningRoutineName)}(tenantId integer) RETURNS SETOF integer LANGUAGE SQL AS $$ SELECT tenantId $$");
    }

    private static async Task SetupPostgresTypedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresTypedRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(PostgresTypedRoutineName)}(ids integer[], ratings numeric(10,2)[], labels varchar(32)[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) + COALESCE(array_length(ratings, 1), 0) + COALESCE(array_length(labels, 1), 0) $$");
    }

    private static async Task SetupPostgresOverloadedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresOverloadedRoutineAsync(connection, provider);

        var routine = provider.Escape("public") + "." + provider.Escape(PostgresOverloadedRoutineName);
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}(value integer) RETURNS integer LANGUAGE SQL AS $$ SELECT value $$");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}(value text) RETURNS integer LANGUAGE SQL AS $$ SELECT char_length(value) $$");
    }

    private static async Task SetupPostgresQuotedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresQuotedParameterRoutineAsync(connection, provider);

        var routine = provider.Escape("public") + "." + provider.Escape(PostgresQuotedParameterRoutineName);
        var tenantId = provider.Escape("tenant-id");
        var searchText = provider.Escape("search text");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}({tenantId} integer, {searchText} text) RETURNS integer LANGUAGE SQL AS $$ SELECT {tenantId} + length({searchText}) $$");
    }

    private static async Task SetupMySqlUnsignedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsignedRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape(MySqlUnsignedRoutineName)}(customer_id INT UNSIGNED, max_id BIGINT UNSIGNED, {provider.Escape("rank")} SMALLINT UNSIGNED, flag TINYINT UNSIGNED) RETURNS INT DETERMINISTIC NO SQL RETURN customer_id");
    }

    private static async Task SetupPostgresTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresTypedColumnTableAsync(connection, provider);

        var table = Qualified(provider, "public", PostgresTypedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL, {provider.Escape("Ratings")} numeric(10,2)[] NULL, {provider.Escape("Aliases")} varchar(32)[] NULL)");
    }

    private static async Task SetupMySqlTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlTypedColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlTypedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("FiscalYear")} YEAR NOT NULL, {provider.Escape("Status")} ENUM('draft','paid','cancelled') NOT NULL, {provider.Escape("Flags")} SET('read','write','admin') NOT NULL)");
    }

    private static async Task SetupMySqlUnsafeSetColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsafeSetColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlUnsafeSetColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Flags")} SET('a','b','c','d','e','f','g','h','i') NOT NULL)");
    }

    private static async Task SetupMySqlUnsignedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsignedColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlUnsignedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("UnsignedCount")} INT UNSIGNED NOT NULL, {provider.Escape("UnsignedTotal")} BIGINT UNSIGNED NOT NULL, {provider.Escape("UnsignedAmount")} DECIMAL(18,4) UNSIGNED NOT NULL)");
    }

    private static async Task SetupProviderSpecificColumnDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, ProviderSpecificColumnDiagnosticsTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", ProviderSpecificColumnDiagnosticsTable)
                : provider.Escape(ProviderSpecificColumnDiagnosticsTable);
        var id = provider.Escape("Id");
        var providerColumnSql = kind switch
        {
            ProviderKind.SqlServer => $"{provider.Escape("Location")} geometry NULL",
            ProviderKind.Postgres => $"{provider.Escape("Address")} inet NULL",
            ProviderKind.MySql => $"{provider.Escape("Location")} POINT NULL",
            ProviderKind.Sqlite => $"{provider.Escape("Location")} GEOMETRY NULL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {providerColumnSql})");
    }

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
        var nameLength = provider.Escape("NameLength");
        var checkName = provider.Escape(FeatureOwnedCheckName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE Latin1_General_BIN2 NOT NULL, {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {checkName} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE \"C\" NOT NULL, {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {checkName} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, {nameLength} INT GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {checkName} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE NOCASE NOT NULL, {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {checkName} CHECK (length({name}) > 0))",
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

    private static async Task SetupSkippedViewAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropView(kind, WarningView, provider.Escape(WarningView)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var view = provider.Escape(WarningView);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE VIEW {view} AS SELECT {id}, {status} FROM {warning}");
    }

    private static async Task SetupPostgresMaterializedViewAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresMaterializedViewAsync(connection, provider);

        var warning = provider.Escape(WarningTable);
        var matView = provider.Escape(PostgresMaterializedView);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {status} {TextType(ProviderKind.Postgres, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE MATERIALIZED VIEW {matView} AS SELECT {id}, {status} FROM {warning}");
    }

    private static async Task SetupSqlServerSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerSynonymAsync(connection, provider);

        var warning = SqlServerQualified(provider, WarningTable);
        var synonym = SqlServerQualified(provider, SqlServerWarningSynonym);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(ProviderKind.SqlServer)} NOT NULL PRIMARY KEY, {status} {TextType(ProviderKind.SqlServer, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE SYNONYM {synonym} FOR {warning}");
    }

    private static async Task SetupSqlServerProcedureSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerProcedureSynonymAsync(connection, provider);

        var procedure = SqlServerQualified(provider, SqlServerSynonymProcedure);
        var synonym = SqlServerQualified(provider, SqlServerProcedureSynonym);
        await ExecuteAsync(connection, $"CREATE PROCEDURE {procedure} AS SELECT 1 AS {provider.Escape("Value")}");
        await ExecuteAsync(connection, $"CREATE SYNONYM {synonym} FOR {procedure}");
    }

    private static async Task SetupMySqlEventDiagnosticsAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlEventDiagnosticsAsync(connection, provider);

        var table = provider.Escape(MySqlEventDiagnosticsName);
        var id = provider.Escape("Id");
        await ExecuteAsync(connection, $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY)");
        await ExecuteAsync(connection,
            $"CREATE EVENT {provider.Escape(MySqlEventDiagnosticsName)} ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY DO UPDATE {table} SET {id} = {id}");
    }

    private static async Task SetupProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var activeType = kind == ProviderKind.SqlServer ? "BIT" : kind == ProviderKind.Postgres ? "BOOLEAN" : "INTEGER";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL, {active} {activeType} NOT NULL, {includedValue} {IntType(kind)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialIndex)} ON {table} ({name}) WHERE {activePredicate}");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderDescendingIndex)} ON {table} ({name} DESC)");

        if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
        {
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} (lower({name}))");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialExpressionIndex)} ON {table} (lower({name})) WHERE {activePredicate}");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionDescendingIndex)} ON {table} (lower({name}) DESC)");
        }

        if (kind is ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionLiteralDescIndex)} ON {table} (strpos({name}, ' DESC'))");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderIncludedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static async Task SetupMySqlPrefixIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPrefixIndex)} ON {table} ({name}(8))");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderFullPrefixIndex)} ON {table} ({name}(80))");
    }

    private static async Task SetupMySqlExpressionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL, {score} {IntType(ProviderKind.MySql)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} ((LOWER({name})), {score})");
    }

    private static async Task SetupProviderSpecificAccessMethodIndexAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 160)} NOT NULL, {score} {IntType(kind)} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $"CREATE NONCLUSTERED COLUMNSTORE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({score})");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} USING hash (lower({name}))");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection, $"CREATE FULLTEXT INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name})");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported provider-specific access-method index test provider.");
        }
    }

    private static async Task SetupPostgresExpressionIncludedIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL, {score} {IntType(ProviderKind.Postgres)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIncludedIndex)} ON {table} (lower({name})) INCLUDE ({score})");
    }

    private static async Task SetupPostgresProviderSpecificBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name} ASC NULLS FIRST)");
    }

    private static async Task SetupPostgresExpressionBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} (lower({name}) text_pattern_ops)");
    }

    private static async Task SetupPostgresNullsNotDistinctUniqueIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE UNIQUE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name}) NULLS NOT DISTINCT");
    }

    private static async Task SetupSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerNativeTemporalTableAsync(connection, provider);

        var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
        var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var validFrom = provider.Escape("ValidFrom");
        var validTo = provider.Escape("ValidTo");

        await ExecuteAsync(connection, $$"""
            CREATE TABLE {{table}} (
                {{id}} INT NOT NULL PRIMARY KEY,
                {{name}} NVARCHAR(80) NOT NULL,
                {{validFrom}} DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                {{validTo}} DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                PERIOD FOR SYSTEM_TIME ({{validFrom}}, {{validTo}})
            ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {{history}}))
            """);
    }

    private static async Task TeardownSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
            var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
            await ExecuteAsync(connection, $$"""
                IF OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}', N'U') IS NOT NULL
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM sys.tables
                        WHERE object_id = OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}')
                          AND temporal_type = 2
                    )
                    BEGIN
                        ALTER TABLE {{table}} SET (SYSTEM_VERSIONING = OFF);
                    END;

                    DROP TABLE {{table}};
                END;

                IF OBJECT_ID(N'dbo.{{SqlServerTemporalHistoryTable}}', N'U') IS NOT NULL
                    DROP TABLE {{history}};
                """);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

}
