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
        => await TeardownTableAsync(connection, provider, kind, Table);

    private static async Task TeardownTableAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind, string table)
    {
        try
        {
            if (kind == ProviderKind.Postgres)
            {
                await ExecuteAsync(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalTrigger")} ON {provider.Escape(table)}");
                await ExecuteAsync(connection,
                    $"DROP FUNCTION IF EXISTS {provider.Escape(table + "_TemporalFunction")}()");
            }
            else if (kind == ProviderKind.MySql)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_ai")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_au")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_ad")}");
            }
            else if (kind == ProviderKind.SqlServer)
            {
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalInsert")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalUpdate")}");
                await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(table + "_TemporalDelete")}");
            }

            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {provider.Escape(table)}"
                : $"DROP TABLE IF EXISTS {provider.Escape(table)}";
            var dropHistory = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{table}_History', N'U') IS NOT NULL DROP TABLE {provider.Escape(table + "_History")}"
                : $"DROP TABLE IF EXISTS {provider.Escape(table + "_History")}";
            await ExecuteAsync(connection, drop);
            await ExecuteAsync(connection, dropHistory);
        }
        catch
        {
            // Best-effort cleanup; the fuzz run itself reports real failures.
        }
    }

    /// <summary>
    /// The RELATIONAL reconstruction machine against real servers: every
    /// checkpoint replays through navigation projections and predicates,
    /// SelectMany, correlated aggregates, Include, a set operation, and a
    /// Contains subquery — proving each dialect executes the history-window
    /// derived tables correctly in JOIN and subquery positions, not just as
    /// the root FROM.
    /// </summary>
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Relational_graph_reconstruction_holds_on_live_server(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var db = connection.Database?.Trim() ?? "";
            if (Skip.If(IsProviderOwnedDatabase(kind, db),
                $"Live temporal fuzz requires an application database; current {kind} database '{db}' is provider-owned."))
                return;

            var options = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<TemporalHistoryReconstructionFuzzTests.Dept>().HasKey(d => d.Id);
                    mb.Entity<TemporalHistoryReconstructionFuzzTests.Emp>().HasKey(e => e.Id);
                    mb.Entity<TemporalHistoryReconstructionFuzzTests.Dept>()
                        .HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
                }
            };
            options.EnableTemporalVersioning();

            using var ctx = new DbContext(connection, provider, options);
            var text = kind == ProviderKind.SqlServer ? "NVARCHAR(64)" : "VARCHAR(64)";
            await TeardownTableAsync(connection, provider, kind, "ThrEmp_Test");
            await TeardownTableAsync(connection, provider, kind, "ThrDept_Test");
            await ExecuteAsync(connection,
                $"CREATE TABLE {provider.Escape("ThrDept_Test")} ({provider.Escape("Id")} INT PRIMARY KEY, {provider.Escape("Title")} {text} NOT NULL)");
            await ExecuteAsync(connection,
                $"CREATE TABLE {provider.Escape("ThrEmp_Test")} ({provider.Escape("Id")} INT PRIMARY KEY, {provider.Escape("Name")} {text} NOT NULL, {provider.Escape("DeptId")} INT NOT NULL)");
            try
            {
                await TemporalHistoryReconstructionFuzzTests.RunRelationalReconstructionFuzzAsync(
                    ctx, seed: 20260717, rounds: 5, async () =>
                    {
                        await using var cmd = connection.CreateCommand();
                        cmd.CommandText = ServerNowSql(kind);
                        var value = await cmd.ExecuteScalarAsync();
                        return DateTime.SpecifyKind(Convert.ToDateTime(value, System.Globalization.CultureInfo.InvariantCulture), DateTimeKind.Utc);
                    });
            }
            finally
            {
                await TeardownTableAsync(connection, provider, kind, "ThrEmp_Test");
                await TeardownTableAsync(connection, provider, kind, "ThrDept_Test");
            }
        }
    }

    private static bool IsProviderOwnedDatabase(ProviderKind kind, string db) => kind switch
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
