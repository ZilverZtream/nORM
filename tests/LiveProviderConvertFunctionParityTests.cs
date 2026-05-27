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
/// Live-provider parity for System.Convert translations over columns. This is
/// intentionally separate from materializer conversion tests: these conversions
/// must execute in SQL and use provider-valid cast syntax.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderConvertFunctionParityTests
{
    private const string TableName = "ConvertFuncLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Convert_functions_match_on_live_provider(ProviderKind kind)
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
                var intIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToInt32(r.NumText) > 50)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var longIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToInt64(r.NumText) == 10L)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var stringIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToString(r.Number) == "100")
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var doubleIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToDouble(r.DecText) > 12.25)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var decimalIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToDecimal(r.DecText) < 2m)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                var boolIds = (await ctx.Query<ConvertFuncLiveRow>()
                    .Where(r => Convert.ToBoolean(r.BoolText))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 3 }, intIds);
                Assert.Equal(new[] { 2 }, longIds);
                Assert.Equal(new[] { 3 }, stringIds);
                Assert.Equal(new[] { 2, 3 }, doubleIds);
                Assert.Equal(new[] { 1, 4 }, decimalIds);
                Assert.Equal(new[] { 1, 3 }, boolIds);
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
        var id = ctx.Provider.Escape(nameof(ConvertFuncLiveRow.Id));
        var number = ctx.Provider.Escape(nameof(ConvertFuncLiveRow.Number));
        var numText = ctx.Provider.Escape(nameof(ConvertFuncLiveRow.NumText));
        var decText = ctx.Provider.Escape(nameof(ConvertFuncLiveRow.DecText));
        var boolText = ctx.Provider.Escape(nameof(ConvertFuncLiveRow.BoolText));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {number} {intType} NOT NULL, " +
            $"{numText} {textType} NOT NULL, {decText} {textType} NOT NULL, {boolText} {textType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{number},{numText},{decText},{boolText}) VALUES " +
            "(1,1,'1','1.25','true')," +
            "(2,10,'10','12.50','false')," +
            "(3,100,'100','100.25','TRUE')," +
            "(4,50,'50','0.75','false')");
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
    private sealed class ConvertFuncLiveRow
    {
        [Key] public int Id { get; set; }
        public int Number { get; set; }
        public string NumText { get; set; } = string.Empty;
        public string DecText { get; set; } = string.Empty;
        public string BoolText { get; set; } = string.Empty;
    }
}
