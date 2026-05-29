using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for standalone DefaultIfEmpty across all 4 providers.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderDefaultIfEmptyParityTests
{
    private const string Table = "DieLiveRow";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
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

        await ExecuteAsync(ctx, DropDdl(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 20)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name}) VALUES (1,'a'),(2,'b'),(3,'c')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort cleanup */ }
    }

    [Table(Table)]
    private sealed class DieLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DefaultIfEmpty_on_non_empty_source_returns_source_unchanged_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<DieLiveRow>()
                    .DefaultIfEmpty()
                    .OrderBy(r => r!.Id)
                    .ToListAsync();

                Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r!.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DefaultIfEmpty_on_empty_source_returns_single_null_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<DieLiveRow>()
                    .Where(r => r.Id > 9999)
                    .DefaultIfEmpty()
                    .ToListAsync();

                Assert.Single(rows);
                Assert.Null(rows[0]);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DefaultIfEmpty_with_value_on_empty_source_returns_fallback_on_live_provider(ProviderKind kind)
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
                var fallback = new DieLiveRow { Id = -1, Name = "fallback" };
                var rows = await ctx.Query<DieLiveRow>()
                    .Where(r => r.Name == "missing")
                    .DefaultIfEmpty(fallback)
                    .ToListAsync();

                Assert.Single(rows);
                Assert.Equal(-1, rows[0]!.Id);
                Assert.Equal("fallback", rows[0]!.Name);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
