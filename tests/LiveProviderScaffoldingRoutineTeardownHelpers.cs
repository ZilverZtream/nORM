#nullable enable
using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task TeardownRoutineAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{RoutineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineName)}",
                ProviderKind.Postgres => $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(RoutineName)}(integer)",
                ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(RoutineName)}",
                _ => ""
            };

            if (!string.IsNullOrWhiteSpace(sql))
                await ExecuteAsync(connection, sql);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownRoutineWithOutputAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{RoutineOutputName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineOutputName)}",
                ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(RoutineOutputName)}",
                _ => ""
            };

            if (!string.IsNullOrWhiteSpace(sql))
                await ExecuteAsync(connection, sql);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerNonQueryRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{RoutineNonQueryName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineNonQueryName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerTableValuedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{RoutineTableValuedParameterName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineTableValuedParameterName)}");
            await ExecuteAsync(connection,
                $"IF TYPE_ID(N'dbo.{RoutineTableTypeName}') IS NOT NULL DROP TYPE {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerFunctionRoutinesAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerTableValuedFunctionName}', N'IF') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerTableValuedFunctionName)}");
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerScalarFunctionName}', N'FN') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerScalarFunctionName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSequenceAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var qualifiedName = kind == ProviderKind.SqlServer
                ? provider.Escape("dbo") + "." + provider.Escape(SequenceName)
                : provider.Escape("public") + "." + provider.Escape(SequenceName);
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{SequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {qualifiedName}",
                ProviderKind.Postgres => $"DROP SEQUENCE IF EXISTS {qualifiedName}",
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffolding live test only targets SQL Server and PostgreSQL.")
            };
            await ExecuteAsync(connection, sql);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresSetReturningRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresSetReturningRoutineName)}(integer)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresTypedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresTypedRoutineName)}(integer[], numeric[], character varying[], uuid)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresDomainRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var emailDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineEmailDomainName);
            var ratingsDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineRatingsDomainName);
            var statusEnum = provider.Escape("public") + "." + provider.Escape(PostgresRoutineStatusEnumName);
            var statusDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineStatusDomainName);
            var routine = provider.Escape("public") + "." + provider.Escape(PostgresDomainRoutineName);
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {routine}({emailDomain}, {ratingsDomain}, {statusDomain})");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {statusDomain}");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {ratingsDomain}");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {emailDomain}");
            await ExecuteAsync(connection, $"DROP TYPE IF EXISTS {statusEnum}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresOverloadedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var routine = provider.Escape("public") + "." + provider.Escape(PostgresOverloadedRoutineName);
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {routine}(integer)");
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {routine}(text)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresQuotedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresQuotedParameterRoutineName)}(integer, text)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlUnsignedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(MySqlUnsignedRoutineName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
