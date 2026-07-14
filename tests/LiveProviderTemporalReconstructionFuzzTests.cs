using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The temporal history-reconstruction fuzz machine against real servers: each
/// provider's own trigger implementation (MySQL UTC_TIMESTAMP(6), Postgres
/// now()-at-utc trigger function, SQL Server SYSUTCDATETIME triggers) must
/// reconstruct every checkpoint state through AsOf. Checkpoints read the
/// SERVER clock so client/server skew cannot produce false positives.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
[Collection(LiveTemporalProviderCollection.Name)]
public class LiveProviderTemporalReconstructionFuzzTests
{
    private const string Table = "ThrRow_Test";

    private static string ServerNowSql(ProviderKind kind) => kind switch
    {
        ProviderKind.MySql => "SELECT UTC_TIMESTAMP(6)",
        ProviderKind.Postgres => "SELECT now() at time zone 'utc'",
        ProviderKind.SqlServer => "SELECT SYSUTCDATETIME()",
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string CreateTableSql(ProviderKind kind, DatabaseProvider provider)
    {
        var table = provider.Escape(Table);
        var text = kind == ProviderKind.SqlServer ? "NVARCHAR(64)" : "VARCHAR(64)";
        return $"CREATE TABLE {table} (\"Id\" INT PRIMARY KEY, \"V\" INT NOT NULL, \"S\" {text} NOT NULL)"
            .Replace("\"Id\"", provider.Escape("Id"))
            .Replace("\"V\"", provider.Escape("V"))
            .Replace("\"S\"", provider.Escape("S"));
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task TeardownAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            if (kind == ProviderKind.Postgres)
            {
                await ExecuteAsync(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalTrigger")} ON {provider.Escape(Table)}");
                await ExecuteAsync(connection,
                    $"DROP FUNCTION IF EXISTS {provider.Escape(Table + "_TemporalFunction")}()");
            }
            else if (kind == ProviderKind.MySql)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_ai")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_au")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_ad")}");
            }
            else if (kind == ProviderKind.SqlServer)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalInsert")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalUpdate")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(Table + "_TemporalDelete")}");
            }

            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {provider.Escape(Table)}"
                : $"DROP TABLE IF EXISTS {provider.Escape(Table)}";
            var dropHistory = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}_History', N'U') IS NOT NULL DROP TABLE {provider.Escape(Table + "_History")}"
                : $"DROP TABLE IF EXISTS {provider.Escape(Table + "_History")}";
            await ExecuteAsync(connection, drop);
            await ExecuteAsync(connection, dropHistory);
        }
        catch
        {
            // Best-effort cleanup; the fuzz run itself reports real failures.
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task History_reconstruction_holds_on_live_server(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            // Temporal bootstrap refuses provider-owned databases (master/mysql/
            // postgres/...); the live environment must point at an application DB.
            var db = connection.Database?.Trim() ?? "";
            var isProtected = kind switch
            {
                ProviderKind.SqlServer => db.Equals("master", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("model", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("msdb", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
                ProviderKind.Postgres => db.Equals("postgres", StringComparison.OrdinalIgnoreCase)
                    || db.StartsWith("template", StringComparison.OrdinalIgnoreCase),
                ProviderKind.MySql => db.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("sys", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
                    || db.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
                _ => false
            };
            if (Skip.If(isProtected,
                $"Live temporal fuzz requires an application database; current {kind} database '{db}' is provider-owned."))
                return;

            var options = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<TemporalHistoryReconstructionFuzzTests.Row>()
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            await TeardownAsync(connection, provider, kind);
            await ExecuteAsync(connection, CreateTableSql(kind, provider));
            try
            {
                await TemporalHistoryReconstructionFuzzTests.RunReconstructionFuzzAsync(
                    ctx, seed: 20260714, rounds: 6, async () =>
                    {
                        await using var cmd = connection.CreateCommand();
                        cmd.CommandText = ServerNowSql(kind);
                        var value = await cmd.ExecuteScalarAsync();
                        return DateTime.SpecifyKind(Convert.ToDateTime(value, System.Globalization.CultureInfo.InvariantCulture), DateTimeKind.Utc);
                    });
            }
            finally
            {
                await TeardownAsync(connection, provider, kind);
            }
        }
    }
}
