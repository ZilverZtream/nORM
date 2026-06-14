#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

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
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(ids integer[], ratings numeric(10,2)[], labels varchar(32)[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) + COALESCE(array_length(ratings, 1), 0) + COALESCE(array_length(labels, 1), 0) $$");
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
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer[], numeric[], character varying[], uuid)");
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

}
