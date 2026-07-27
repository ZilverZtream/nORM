using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateTime.Second is a whole-second integer in .NET. PostgreSQL's EXTRACT(SECOND FROM ...) returns the
/// seconds field INCLUDING fractional seconds (e.g. 30.5), so a predicate or projection over .Second
/// diverged from .NET and from the other providers (SQLite strftime('%S'), SQL Server DATEPART(second),
/// MySQL SECOND) whenever the timestamp carried sub-second precision.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderDateSecondParityTests
{
    private const string Table = "SecPartRow";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string TsType(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DATETIME2(3)",
        ProviderKind.Postgres => "TIMESTAMP(3)",
        ProviderKind.MySql => "DATETIME(3)",
        _ => "TEXT"
    };

    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc = ctx.Provider.Escape(Table);
        var eId = ctx.Provider.Escape("Id");
        var eAt = ctx.Provider.Escape("OccurredAt");
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eAt} {TsType(kind)} NOT NULL)");
        // 10:15:30.500 — sub-second precision present; .Second is 30.
        await ExecuteAsync(ctx, $"INSERT INTO {esc} ({eId},{eAt}) VALUES (1, '2026-01-01 10:15:30.500')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class SecPartRow
    {
        [Key] public int Id { get; set; }
        public DateTime OccurredAt { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTime_Second_is_whole_seconds_ignoring_subsecond(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                // Predicate: .Second == 30 must match the row (30.5 truncates to whole 30 in .NET).
                var matched = (await ctx.Query<SecPartRow>()
                    .Where(r => r.OccurredAt.Second == 30)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();
                Assert.Equal(new[] { 1 }, matched);   // BUG on Postgres: EXTRACT(SECOND)=30.5 != 30 -> empty

                // Projection: .Second must materialize to whole seconds.
                var seconds = (await ctx.Query<SecPartRow>()
                    .Select(r => new { r.OccurredAt.Second })
                    .ToListAsync())
                    .Select(x => x.Second).ToArray();
                Assert.Equal(new[] { 30 }, seconds);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
