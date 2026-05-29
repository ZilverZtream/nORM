using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for advertised JSON and window-function query features.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderJsonWindowParityTests
{
    private const string Table = "JwLiveRow";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string JsonCol(ProviderKind kind) => kind switch
    {
        ProviderKind.Postgres => "JSONB",
        ProviderKind.MySql => "JSON",
        ProviderKind.SqlServer => "NVARCHAR(MAX)",
        _ => "TEXT"
    };

    private static string DropDdl(ProviderKind kind, string escapedTable) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {escapedTable};"
        : $"DROP TABLE IF EXISTS {escapedTable};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape("Id");
        var name = ctx.Provider.Escape("Name");
        var score = ctx.Provider.Escape("Score");
        var payload = ctx.Provider.Escape("Payload");

        await ExecuteAsync(ctx, DropDdl(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 20)} NOT NULL, " +
            $"{score} {IntCol(kind)} NOT NULL, {payload} {JsonCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name},{score},{payload}) VALUES " +
            "(1,'alpha',10,'{\"category\":\"electronics\",\"qty\":7}')," +
            "(2,'beta',20,'{\"category\":\"books\",\"qty\":2}')," +
            "(3,'gamma',20,'{\"category\":\"electronics\",\"qty\":5}')," +
            "(4,'delta',30,'{\"category\":\"garden\",\"qty\":1}')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort cleanup */ }
    }

    [Table(Table)]
    private sealed class JwLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public string Payload { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Json_value_filter_matches_expected_rows_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<JwLiveRow>()
                    .Where(r => Json.Value<string>(r.Payload, "$.category") == "electronics")
                    .OrderBy(r => r.Id)
                    .ToListAsync();

                Assert.Equal(new[] { 1, 3 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task WithRowNumber_projects_deterministic_row_numbers_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<JwLiveRow>()
                    .OrderBy(r => r.Score)
                    .ThenBy(r => r.Id)
                    .WithRowNumber((r, rn) => new { r.Id, r.Name, RowNumber = rn })
                    .ToListAsync();

                Assert.Equal(new[] { 1, 2, 3, 4 }, rows.Select(r => r.Id).ToArray());
                Assert.Equal(new[] { 1, 2, 3, 4 }, rows.Select(r => r.RowNumber).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task WithRank_and_dense_rank_match_tie_semantics_on_live_provider(ProviderKind kind)
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
                var ranks = await ctx.Query<JwLiveRow>()
                    .OrderBy(r => r.Score)
                    .WithRank((r, rank) => new { r.Id, r.Score, Rank = rank })
                    .ToListAsync();

                var denseRanks = await ctx.Query<JwLiveRow>()
                    .OrderBy(r => r.Score)
                    .WithDenseRank((r, denseRank) => new { r.Id, r.Score, DenseRank = denseRank })
                    .ToListAsync();

                Assert.Equal(1, ranks.Single(r => r.Id == 1).Rank);
                Assert.Equal(2, ranks.Single(r => r.Id == 2).Rank);
                Assert.Equal(2, ranks.Single(r => r.Id == 3).Rank);
                Assert.Equal(4, ranks.Single(r => r.Id == 4).Rank);

                Assert.Equal(1, denseRanks.Single(r => r.Id == 1).DenseRank);
                Assert.Equal(2, denseRanks.Single(r => r.Id == 2).DenseRank);
                Assert.Equal(2, denseRanks.Single(r => r.Id == 3).DenseRank);
                Assert.Equal(3, denseRanks.Single(r => r.Id == 4).DenseRank);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task WithLag_and_lead_project_neighbor_values_on_live_provider(ProviderKind kind)
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
                var lag = await ctx.Query<JwLiveRow>()
                    .OrderBy(r => r.Id)
                    .WithLag(r => r.Score, 1, (r, previous) => new { r.Id, PreviousScore = previous }, r => -1)
                    .ToListAsync();

                var lead = await ctx.Query<JwLiveRow>()
                    .OrderBy(r => r.Id)
                    .WithLead(r => r.Score, 1, (r, next) => new { r.Id, NextScore = next }, r => -2)
                    .ToListAsync();

                Assert.Equal(-1, lag.Single(r => r.Id == 1).PreviousScore);
                Assert.Equal(10, lag.Single(r => r.Id == 2).PreviousScore);
                Assert.Equal(20, lag.Single(r => r.Id == 3).PreviousScore);
                Assert.Equal(20, lag.Single(r => r.Id == 4).PreviousScore);

                Assert.Equal(20, lead.Single(r => r.Id == 1).NextScore);
                Assert.Equal(20, lead.Single(r => r.Id == 2).NextScore);
                Assert.Equal(30, lead.Single(r => r.Id == 3).NextScore);
                Assert.Equal(-2, lead.Single(r => r.Id == 4).NextScore);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
