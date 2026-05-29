using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider evidence for the provider-mobile Regex.IsMatch subset. SQLite
/// executes through the deterministic managed regex functions registered by
/// <see cref="SqliteProvider"/> during provider initialization.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderRegexIsMatchParityTests
{
    private const string TableName = "RegexLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Regex_IsMatch_simple_subset_matches_same_rows_on_live_provider(ProviderKind kind)
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
                var lowerA = (await ctx.Query<RegexLiveRow>()
                    .Where(r => Regex.IsMatch(r.Text, "^a"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var ignoreCaseA = (await ctx.Query<RegexLiveRow>()
                    .Where(r => Regex.IsMatch(r.Text, "^a", RegexOptions.IgnoreCase))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var startsWithDigit = (await ctx.Query<RegexLiveRow>()
                    .Where(r => Regex.IsMatch(r.Text, "^\\d"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var literalReplace = (await ctx.Query<RegexLiveRow>()
                    .Where(r => Regex.Replace(r.Text, "a", "#") == "Bet#")
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var letterCode = (await ctx.Query<RegexLiveRow>()
                    .Where(r => Regex.IsMatch(r.Text, "^[A-Z]+\\d{2}$"))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                Assert.Equal(new[] { 1 }, lowerA);
                Assert.Equal(new[] { 1, 6, 7, 8, 9, 10, 11 }, ignoreCaseA);
                Assert.Equal(new[] { 5 }, startsWithDigit);
                Assert.Equal(new[] { 2 }, literalReplace);
                Assert.Equal(new[] { 7 }, letterCode);
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
        var id = ctx.Provider.Escape(nameof(RegexLiveRow.Id));
        var text = ctx.Provider.Escape(nameof(RegexLiveRow.Text));

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {text} {TextCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{text}) VALUES " +
            "(1,'alpha')," +
            "(2,'Beta')," +
            "(3,'gamma1')," +
            "(4,'delta2')," +
            "(5,'123-start')," +
            "(6,'Aardvark')," +
            "(7,'AB12')," +
            "(8,'A1')," +
            "(9,'ABC1')," +
            "(10,'AB123')," +
            "(11,'AB-12')");
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

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string TextCol(ProviderKind kind) => kind == ProviderKind.SqlServer ? "NVARCHAR(80)" : "VARCHAR(80)";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(TableName)]
    private sealed class RegexLiveRow
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = string.Empty;
    }
}
