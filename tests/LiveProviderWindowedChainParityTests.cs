using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for operators applied AFTER a Take/Skip window with
/// intervening operators: the translator wraps the whole windowed chain as a
/// derived table and composes filters, projections, aggregates, grouping and
/// resorts against the wrap alias. Derived-table paging syntax is dialect-
/// specific (SQL Server OFFSET/FETCH inside derived tables, MySQL/SQLite
/// LIMIT, PostgreSQL LIMIT/OFFSET), so the wrap shapes need direct execution
/// evidence on every provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderWindowedChainParityTests
{
    private const string Table = "WindowedChainParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Windowed_filter_chains_compose_correctly_on_live_provider(ProviderKind kind)
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
                // Window: OrderBy(Id).Take(3) → scores [50, 40, 30]; filter keeps [40, 30].
                var windowed = ctx.Query<WindowedChainParityRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Where(r => r.Score < 45);

                // Filter inside the window (rows outside the window match the predicate too).
                var scores = (await windowed.ToListAsync()).Select(r => r.Score).OrderBy(s => s).ToArray();
                Assert.Equal(new[] { 30, 40 }, scores);

                // Scalar terminals over the wrapped chain.
                Assert.Equal(2, await windowed.CountAsync());
                Assert.Equal(70, await windowed.SumAsync(r => r.Score));
                Assert.Equal(40, await windowed.MaxAsync(r => r.Score));
                Assert.True(await windowed.AnyAsync());

                // Resort of the filtered window.
                var resorted = (await windowed.OrderBy(r => r.Score).ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 3, 2 }, resorted);

                // Computed projection against the wrap alias.
                var projected = await windowed
                    .Select(r => new { r.Id, Double = r.Score * 2 })
                    .ToListAsync();
                Assert.Equal(80, projected.Single(x => x.Id == 2).Double);
                Assert.Equal(60, projected.Single(x => x.Id == 3).Double);

                // Grouping only the filtered window (keys 0 and 10 for scores 40, 30).
                var groups = await windowed
                    .GroupBy(r => r.Score % 20)
                    .Select(g => new { g.Key, Total = g.Sum(x => x.Score) })
                    .ToListAsync();
                Assert.Equal(2, groups.Count);
                Assert.Equal(40, groups.Single(g => g.Key == 0).Total);
                Assert.Equal(30, groups.Single(g => g.Key == 10).Total);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape(nameof(WindowedChainParityRow.Id));
        var score = ctx.Provider.Escape(nameof(WindowedChainParityRow.Score));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {score} {intType} NOT NULL)");
        // Rows 4 and 5 match the filter predicate but sit OUTSIDE the window —
        // a flat translation would leak them into every result below.
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{score}) VALUES " +
            "(1,50),(2,40),(3,30),(4,5),(5,1)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var table = ctx.Provider.Escape(Table);
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table}"
                : $"DROP TABLE IF EXISTS {table}";
            await ExecuteAsync(ctx, drop);
        }
        catch
        {
            // best-effort cleanup only
        }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(Table)]
    public sealed class WindowedChainParityRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
