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

    private static async Task SetupPostgresDomainRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresDomainRoutineAsync(connection, provider);

        var emailDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineEmailDomainName);
        var ratingsDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineRatingsDomainName);
        var statusEnum = provider.Escape("public") + "." + provider.Escape(PostgresRoutineStatusEnumName);
        var statusDomain = provider.Escape("public") + "." + provider.Escape(PostgresRoutineStatusDomainName);
        var routine = provider.Escape("public") + "." + provider.Escape(PostgresDomainRoutineName);
        await ExecuteAsync(connection, $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active')");
        await ExecuteAsync(connection, $"CREATE DOMAIN {emailDomain} AS varchar(320) CHECK (VALUE LIKE '%@%')");
        await ExecuteAsync(connection, $"CREATE DOMAIN {ratingsDomain} AS numeric(10,2)[]");
        await ExecuteAsync(connection, $"CREATE DOMAIN {statusDomain} AS {statusEnum}");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}(email {emailDomain}, ratings {ratingsDomain}, status {statusDomain}) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ratings, 1), 0) $$");
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
}
