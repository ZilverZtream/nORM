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
/// Live-provider parity for CASE expressions and arithmetic expressions in
/// predicates, projections, and aggregate selectors.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderConditionalArithmeticParityTests
{
    private const string TableName = "CondArithLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Conditional_and_arithmetic_expressions_match_on_live_provider(ProviderKind kind)
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
                var conditionalWhereIds = (await ctx.Query<CondArithLiveRow>()
                    .Where(r => (r.Score > 50 ? r.Score : 0) >= 70)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var arithmeticWhereIds = (await ctx.Query<CondArithLiveRow>()
                    .Where(r => (r.Price + r.Quantity * 2 - r.Penalty) % 5 == 3)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var projected = (await ctx.Query<CondArithLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new
                    {
                        r.Id,
                        Net = r.Price + r.Quantity * 2 - r.Penalty,
                        Bucket = r.Score >= 80 ? "high" : (r.Score >= 50 ? "mid" : "low")
                    })
                    .ToListAsync())
                    .ToArray();

                var grouped = (await ctx.Query<CondArithLiveRow>()
                    .GroupBy(r => r.Category)
                    .Select(g => new
                    {
                        Cat = g.Key,
                        Total = g.Sum(x => x.Price * x.Quantity + (x.Score > 50 ? x.Penalty : -x.Penalty))
                    })
                    .OrderBy(g => g.Cat)
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(new[] { 1, 2 }, conditionalWhereIds);
                Assert.Equal(new[] { 1 }, arithmeticWhereIds);

                Assert.Equal(new[] { 13, 22, 17, 15 }, projected.Select(r => r.Net).ToArray());
                Assert.Equal(new[] { "high", "mid", "low", "low" }, projected.Select(r => r.Bucket).ToArray());

                Assert.Equal(2, grouped.Length);
                Assert.Equal("A", grouped[0].Cat);
                Assert.Equal(85, grouped[0].Total);
                Assert.Equal("B", grouped[1].Cat);
                Assert.Equal(70, grouped[1].Total);
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
        var id = ctx.Provider.Escape(nameof(CondArithLiveRow.Id));
        var category = ctx.Provider.Escape(nameof(CondArithLiveRow.Category));
        var price = ctx.Provider.Escape(nameof(CondArithLiveRow.Price));
        var quantity = ctx.Provider.Escape(nameof(CondArithLiveRow.Quantity));
        var penalty = ctx.Provider.Escape(nameof(CondArithLiveRow.Penalty));
        var score = ctx.Provider.Escape(nameof(CondArithLiveRow.Score));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(8)" : "VARCHAR(8)";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {category} {textType} NOT NULL, " +
            $"{price} {intType} NOT NULL, {quantity} {intType} NOT NULL, " +
            $"{penalty} {intType} NOT NULL, {score} {intType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{category},{price},{quantity},{penalty},{score}) VALUES " +
            "(1,'A',10,2,1,90)," +
            "(2,'A',20,3,4,70)," +
            "(3,'B',5,10,8,30)," +
            "(4,'B',7,4,0,10)");
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

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class CondArithLiveRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Price { get; set; }
        public int Quantity { get; set; }
        public int Penalty { get; set; }
        public int Score { get; set; }
    }
}
