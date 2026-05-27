using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for the Contains rows in docs/live-provider-linq-parity.md.
/// Covers both local collection Contains (including null values) and projected
/// query ContainsAsync(value), which translate through different code paths.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderContainsParityTests
{
    private const string Table = "ContainsParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Local_collection_Contains_filters_values_and_nulls_on_live_provider(ProviderKind kind)
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
                var ids = new[] { 1, 3, 99 };
                var idRows = await ctx.Query<ContainsParityRow>()
                    .Where(r => ids.Contains(r.Id))
                    .OrderBy(r => r.Id)
                    .ToListAsync();
                Assert.Equal(new[] { 1, 3 }, idRows.Select(r => r.Id).ToArray());

                string?[] notes = { null, "maybe" };
                var noteRows = await ctx.Query<ContainsParityRow>()
                    .Where(r => notes.Contains(r.Note))
                    .OrderBy(r => r.Id)
                    .ToListAsync();
                Assert.Equal(new[] { 1, 3, 4 }, noteRows.Select(r => r.Id).ToArray());
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Projected_query_ContainsAsync_respects_source_filter_on_live_provider(ProviderKind kind)
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
                var hasBeta = await ctx.Query<ContainsParityRow>()
                    .Where(r => r.Id > 1)
                    .Select(r => r.Code)
                    .ContainsAsync("beta");
                Assert.True(hasBeta);

                var hasAlphaAfterFilter = await ctx.Query<ContainsParityRow>()
                    .Where(r => r.Id > 1)
                    .Select(r => r.Code)
                    .ContainsAsync("alpha");
                Assert.False(hasAlphaAfterFilter);

                var hasMissing = await ctx.Query<ContainsParityRow>()
                    .Select(r => r.Code)
                    .ContainsAsync("zeta");
                Assert.False(hasMissing);
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
        var id = ctx.Provider.Escape(nameof(ContainsParityRow.Id));
        var code = ctx.Provider.Escape(nameof(ContainsParityRow.Code));
        var note = ctx.Provider.Escape(nameof(ContainsParityRow.Note));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {code} {textType} NOT NULL, {note} {textType} NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{code},{note}) VALUES " +
            "(1,'alpha',NULL),(2,'beta','other'),(3,'gamma','maybe'),(4,'delta',NULL)");
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
    private sealed class ContainsParityRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string? Note { get; set; }
    }
}
