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
/// Live-provider parity for the broader string translation row: trim variants,
/// replace, index lookup, concatenation, and string comparison helpers.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderStringFunctionParityTests
{
    private const string TableName = "StringFuncLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_functions_match_on_live_provider(ProviderKind kind)
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
                var trimIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => r.Name.Trim() == "alpha"
                        || r.Name.TrimStart() == "alpha  "
                        || r.Name.TrimEnd() == "  alpha")
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var replaceIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => r.Name.Replace("-", "_") == "delta_epsilon")
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var indexIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => r.Name.IndexOf("epsilon") == 6)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var notFoundCount = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => r.Name.IndexOf("not-there") == -1)
                    .ToListAsync())
                    .Count;

                var concatIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => string.Concat(r.Code, "!") == "bravo!")
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var compareLessIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => string.Compare(r.Code, "charlie") < 0)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var compareEqualIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => string.Compare(r.Code, "delta") == 0)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var compareToGreaterIds = (await ctx.Query<StringFuncLiveRow>()
                    .Where(r => r.Code.CompareTo("charlie") > 0)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1 }, trimIds);
                Assert.Equal(new[] { 4 }, replaceIds);
                Assert.Equal(new[] { 4 }, indexIds);
                Assert.Equal(5, notFoundCount);
                Assert.Equal(new[] { 2 }, concatIds);
                Assert.Equal(new[] { 1, 2 }, compareLessIds);
                Assert.Equal(new[] { 4 }, compareEqualIds);
                Assert.Equal(new[] { 4, 5 }, compareToGreaterIds);
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
        var id = ctx.Provider.Escape(nameof(StringFuncLiveRow.Id));
        var name = ctx.Provider.Escape(nameof(StringFuncLiveRow.Name));
        var code = ctx.Provider.Escape(nameof(StringFuncLiveRow.Code));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(80)" : "VARCHAR(80)";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {name} {textType} NOT NULL, {code} {textType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name},{code}) VALUES " +
            "(1,'  alpha  ','alpha')," +
            "(2,'bravo','bravo')," +
            "(3,'charlie','charlie')," +
            "(4,'delta-epsilon','delta')," +
            "(5,'zeta','zeta')");
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
    private sealed class StringFuncLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Code { get; set; } = string.Empty;
    }
}
