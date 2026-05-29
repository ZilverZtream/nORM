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
/// Live-provider parity for TimeSpan column member translations. The values stay
/// within one day because SQL Server TIME is a time-of-day type, while PostgreSQL
/// INTERVAL/MySQL TIME/SQLite text can represent broader durations.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderTimeSpanMemberParityTests
{
    private const string TableName = "TimeSpanMemberLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task TimeSpan_members_match_on_live_provider(ProviderKind kind)
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
                var projected = (await ctx.Query<TimeSpanMemberLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new
                    {
                        r.Id,
                        TotalSeconds = r.Duration.TotalSeconds,
                        TotalMinutes = r.Duration.TotalMinutes,
                        TotalHours = r.Duration.TotalHours,
                        Hours = r.Duration.Hours,
                        Minutes = r.Duration.Minutes,
                        Seconds = r.Duration.Seconds
                    })
                    .ToListAsync())
                    .ToArray();

                var totalMinuteIds = (await ctx.Query<TimeSpanMemberLiveRow>()
                    .Where(r => r.Duration.TotalMinutes >= 90)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var hourComponentIds = (await ctx.Query<TimeSpanMemberLiveRow>()
                    .Where(r => r.Duration.Hours >= 2)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(5, projected.Length);
                Assert.Equal(new[] { 1800.0, 5400.0, 7200.0, 45.0, 11730.0 }, projected.Select(r => r.TotalSeconds).ToArray());
                Assert.Equal(new[] { 30.0, 90.0, 120.0, 0.75, 195.5 }, projected.Select(r => r.TotalMinutes).ToArray());
                Assert.Equal(0.5, projected[0].TotalHours);
                Assert.Equal(45.0 / 3600.0, projected[3].TotalHours, 10);
                Assert.Equal(11730.0 / 3600.0, projected[4].TotalHours, 8);
                Assert.Equal(new[] { 0, 1, 2, 0, 3 }, projected.Select(r => r.Hours).ToArray());
                Assert.Equal(new[] { 30, 30, 0, 0, 15 }, projected.Select(r => r.Minutes).ToArray());
                Assert.Equal(new[] { 0, 0, 0, 45, 30 }, projected.Select(r => r.Seconds).ToArray());
                Assert.Equal(new[] { 2, 3, 5 }, totalMinuteIds);
                Assert.Equal(new[] { 3, 5 }, hourComponentIds);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(TableName);
        var id = ctx.Provider.Escape(nameof(TimeSpanMemberLiveRow.Id));
        var duration = ctx.Provider.Escape(nameof(TimeSpanMemberLiveRow.Duration));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {duration} {TimeSpanCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{duration}) VALUES " +
            $"(1,{TimeSpanLiteral(kind, "00:30:00")})," +
            $"(2,{TimeSpanLiteral(kind, "01:30:00")})," +
            $"(3,{TimeSpanLiteral(kind, "02:00:00")})," +
            $"(4,{TimeSpanLiteral(kind, "00:00:45")})," +
            $"(5,{TimeSpanLiteral(kind, "03:15:30")})");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string DropTable(ProviderKind kind, string escapedTable)
        => kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{TableName}', N'U') IS NOT NULL DROP TABLE {escapedTable}"
            : $"DROP TABLE IF EXISTS {escapedTable}";

    private static string TimeSpanCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "TIME(7)",
        ProviderKind.Postgres => "INTERVAL",
        ProviderKind.MySql => "TIME(6)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string TimeSpanLiteral(ProviderKind kind, string value) => kind switch
    {
        ProviderKind.SqlServer => $"CAST('{value}' AS TIME)",
        ProviderKind.Postgres => $"INTERVAL '{value}'",
        ProviderKind.MySql => $"'{value}'",
        ProviderKind.Sqlite => $"'{value}'",
        _ => throw new NotSupportedException()
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class TimeSpanMemberLiveRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
