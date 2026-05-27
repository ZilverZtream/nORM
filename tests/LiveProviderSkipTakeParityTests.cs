using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for the base Skip/Take row. Paging syntax is highly
/// dialect-specific, and the compiled path has separate parameter binding, so
/// the row needs direct execution evidence on every provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderSkipTakeParityTests
{
    private const string Table = "SkipTakeParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Skip_Take_and_compiled_SkipTake_page_correctly_on_live_provider(ProviderKind kind)
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
                var takeOnly = (await ctx.Query<SkipTakeParityRow>()
                    .OrderBy(r => r.Score)
                    .Take(3)
                    .ToListAsync())
                    .Select(r => r.Score)
                    .ToArray();
                Assert.Equal(new[] { 10, 20, 30 }, takeOnly);

                var skipOnly = (await ctx.Query<SkipTakeParityRow>()
                    .OrderBy(r => r.Score)
                    .Skip(6)
                    .ToListAsync())
                    .Select(r => r.Score)
                    .ToArray();
                Assert.Equal(new[] { 70, 80 }, skipOnly);

                var skipTake = (await ctx.Query<SkipTakeParityRow>()
                    .OrderBy(r => r.Score)
                    .Skip(2)
                    .Take(4)
                    .ToListAsync())
                    .Select(r => r.Score)
                    .ToArray();
                Assert.Equal(new[] { 30, 40, 50, 60 }, skipTake);

                var takeSkip = (await ctx.Query<SkipTakeParityRow>()
                    .OrderBy(r => r.Score)
                    .Take(5)
                    .Skip(2)
                    .ToListAsync())
                    .Select(r => r.Score)
                    .ToArray();
                Assert.Equal(new[] { 30, 40, 50 }, takeSkip);

                var compiled = Norm.CompileQuery<DbContext, (int Skip, int Take), SkipTakeParityRow>(
                    (c, p) => c.Query<SkipTakeParityRow>()
                        .OrderBy(r => r.Score)
                        .Skip(p.Skip)
                        .Take(p.Take));
                var compiledScores = (await compiled(ctx, (3, 2))).Select(r => r.Score).ToArray();
                Assert.Equal(new[] { 40, 50 }, compiledScores);
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
        var id = ctx.Provider.Escape(nameof(SkipTakeParityRow.Id));
        var score = ctx.Provider.Escape(nameof(SkipTakeParityRow.Score));
        var label = ctx.Provider.Escape(nameof(SkipTakeParityRow.Label));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {score} {intType} NOT NULL, {label} {textType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{score},{label}) VALUES " +
            "(1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d')," +
            "(5,50,'e'),(6,60,'f'),(7,70,'g'),(8,80,'h')");
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
    public sealed class SkipTakeParityRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
