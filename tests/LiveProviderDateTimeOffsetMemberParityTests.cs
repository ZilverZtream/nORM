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
/// Live-provider parity for DateTimeOffset member extraction and ordering
/// against DateTime literals. Values are UTC instants to avoid making this test
/// about historical time-zone rules.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderDateTimeOffsetMemberParityTests
{
    private const string TableName = "DtoMemberLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_members_and_DateTime_literal_ordering_match_on_live_provider(ProviderKind kind)
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
                var partIds = (await ctx.Query<DtoMemberLiveRow>()
                    .Where(r => r.Stamp.Year == 2026
                        && r.Stamp.Month == 5
                        && r.Stamp.Day == 27
                        && r.Stamp.Hour == 10)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var projected = (await ctx.Query<DtoMemberLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new
                    {
                        r.Id,
                        Year = r.Stamp.Year,
                        Day = r.Stamp.Day,
                        Hour = r.Stamp.Hour,
                        Minute = r.Stamp.Minute
                    })
                    .ToListAsync())
                    .ToArray();

                var afterCutoff = new DateTime(2026, 05, 27, 12, 00, 00, DateTimeKind.Utc);
                var afterIds = (await ctx.Query<DtoMemberLiveRow>()
                    .Where(r => r.Stamp > afterCutoff)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var beforeOrAtCutoff = new DateTime(2026, 05, 27, 10, 15, 30, DateTimeKind.Utc);
                var beforeOrAtIds = (await ctx.Query<DtoMemberLiveRow>()
                    .Where(r => r.Stamp <= beforeOrAtCutoff)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1 }, partIds);
                Assert.Equal(new[] { 2026, 2026, 2025 }, projected.Select(r => r.Year).ToArray());
                Assert.Equal(new[] { 27, 28, 31 }, projected.Select(r => r.Day).ToArray());
                Assert.Equal(new[] { 10, 13, 23 }, projected.Select(r => r.Hour).ToArray());
                Assert.Equal(new[] { 15, 45, 0 }, projected.Select(r => r.Minute).ToArray());
                Assert.Equal(new[] { 2 }, afterIds);
                Assert.Equal(new[] { 1, 3 }, beforeOrAtIds);
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
        var id = ctx.Provider.Escape(nameof(DtoMemberLiveRow.Id));
        var stamp = ctx.Provider.Escape(nameof(DtoMemberLiveRow.Stamp));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {stamp} {DtoCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{stamp}) VALUES " +
            $"(1,{DtoLiteral(kind, "2026-05-27 10:15:30")})," +
            $"(2,{DtoLiteral(kind, "2026-05-28 13:45:00")})," +
            $"(3,{DtoLiteral(kind, "2025-12-31 23:00:00")})");
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

    private static string DtoCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DATETIMEOFFSET",
        ProviderKind.Postgres => "TIMESTAMPTZ",
        ProviderKind.MySql => "DATETIME(6)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string DtoLiteral(ProviderKind kind, string utcText) => kind switch
    {
        ProviderKind.SqlServer => $"'{utcText} +00:00'",
        ProviderKind.Postgres => $"TIMESTAMPTZ '{utcText}+00'",
        ProviderKind.MySql => $"'{utcText}'",
        ProviderKind.Sqlite => $"'{utcText}+00:00'",
        _ => throw new NotSupportedException()
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class DtoMemberLiveRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }
}
