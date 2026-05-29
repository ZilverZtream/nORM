using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for raw SQL LIKE / ILIKE helper functions. These helpers
/// intentionally do not escape patterns, so the test verifies wildcard behavior
/// and captured-pattern parameter binding on every provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderNormFunctionsLikeParityTests
{
    private const string TableName = "NormLikeLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Like_and_ILike_match_documented_semantics_on_live_provider(ProviderKind kind)
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
                var prefixIds = (await ctx.Query<NormLikeLiveRow>()
                    .Where(r => NormFunctions.Like(r.Name, "al%"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var singleCharIds = (await ctx.Query<NormLikeLiveRow>()
                    .Where(r => NormFunctions.Like(r.Name, "_amma"))
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var dangerousPattern = "a%' OR '1'='1";
                var dangerousIds = (await ctx.Query<NormLikeLiveRow>()
                    .Where(r => NormFunctions.Like(r.Name, dangerousPattern))
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var insensitiveIds = (await ctx.Query<NormLikeLiveRow>()
                    .Where(r => NormFunctions.ILike(r.Name, "omega") || NormFunctions.ILike(r.Name, "%PIE%"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var negatedIds = (await ctx.Query<NormLikeLiveRow>()
                    .Where(r => !NormFunctions.ILike(r.Name, "al%"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1, 5 }, prefixIds);
                Assert.Equal(new[] { 3 }, singleCharIds);
                Assert.Empty(dangerousIds);
                Assert.Equal(new[] { 6, 7 }, insensitiveIds);
                Assert.Equal(new[] { 2, 3, 4, 6, 7 }, negatedIds);
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
        var id = ctx.Provider.Escape(nameof(NormLikeLiveRow.Id));
        var name = ctx.Provider.Escape(nameof(NormLikeLiveRow.Name));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(80)" : "VARCHAR(80)";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {name} {textType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name}) VALUES " +
            "(1,'alpha'),(2,'bravo'),(3,'gamma'),(4,'delta')," +
            "(5,'alabama'),(6,'OMEGA'),(7,'Apple Pie')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; the test body reports operational failures.
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
    private sealed class NormLikeLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
