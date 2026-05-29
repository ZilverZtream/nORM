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
/// Live-provider parity for static value capture inside predicates. These shapes
/// bind as parameters rather than SQL server functions, so provider parameter
/// typing has to preserve Guid and DateTime semantics.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderStaticValuePredicateParityTests
{
    private const string TableName = "StaticValueLiveRow";

    private static readonly Guid TokenA = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Guid_and_current_DateTime_static_values_match_on_live_provider(ProviderKind kind)
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
                var emptyGuidIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => r.Token == Guid.Empty)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var staticGuidIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => r.Token == TokenA)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var utcPastIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => r.Stamp < DateTime.UtcNow)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var localFutureIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => r.Stamp > DateTime.Now)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var todayPastIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => r.Stamp < DateTime.Today)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var serverGuidPredicateIds = (await ctx.Query<StaticValueLiveRow>()
                    .Where(r => Guid.NewGuid() != Guid.Empty)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 3 }, emptyGuidIds);
                Assert.Equal(new[] { 1, 4 }, staticGuidIds);
                Assert.Equal(new[] { 1, 3, 4 }, utcPastIds.OrderBy(i => i).ToArray());
                Assert.Equal(new[] { 2 }, localFutureIds);
                Assert.Equal(new[] { 1, 3, 4 }, todayPastIds.OrderBy(i => i).ToArray());
                Assert.Equal(new[] { 1, 2, 3, 4 }, serverGuidPredicateIds);
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
        var id = ctx.Provider.Escape(nameof(StaticValueLiveRow.Id));
        var token = ctx.Provider.Escape(nameof(StaticValueLiveRow.Token));
        var stamp = ctx.Provider.Escape(nameof(StaticValueLiveRow.Stamp));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, " +
            $"{token} {GuidCol(kind)} NOT NULL, {stamp} {DateTimeCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{token},{stamp}) VALUES " +
            "(1,'11111111-1111-1111-1111-111111111111','2000-01-01 00:00:00')," +
            "(2,'22222222-2222-2222-2222-222222222222','2100-01-01 00:00:00')," +
            "(3,'00000000-0000-0000-0000-000000000000','2000-01-02 00:00:00')," +
            "(4,'11111111-1111-1111-1111-111111111111','2000-01-03 00:00:00')");
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

    private static string GuidCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "UNIQUEIDENTIFIER",
        ProviderKind.Postgres => "UUID",
        ProviderKind.MySql => "CHAR(36)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string DateTimeCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DATETIME2",
        ProviderKind.Postgres => "TIMESTAMP",
        ProviderKind.MySql => "DATETIME(6)",
        ProviderKind.Sqlite => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class StaticValueLiveRow
    {
        [Key] public int Id { get; set; }
        public Guid Token { get; set; }
        public DateTime Stamp { get; set; }
    }
}
