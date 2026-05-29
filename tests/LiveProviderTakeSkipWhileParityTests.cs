using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderTakeSkipWhileParityTests
{
    private const string TableName = "TakeSkipWhileLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Ordered_TakeWhile_and_SkipWhile_match_sequence_prefix_suffix_semantics(ProviderKind kind)
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
                var takeIds = (await ctx.Query<TakeSkipWhileLiveRow>()
                    .OrderBy(r => r.Id)
                    .TakeWhile(r => r.Score < 40)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var skipIds = (await ctx.Query<TakeSkipWhileLiveRow>()
                    .OrderBy(r => r.Id)
                    .SkipWhile(r => r.Score < 40)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var indexTakeIds = (await ctx.Query<TakeSkipWhileLiveRow>()
                    .OrderBy(r => r.Id)
                    .TakeWhile((r, index) => index < 3 && r.Score < 40)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var indexSkipIds = (await ctx.Query<TakeSkipWhileLiveRow>()
                    .OrderBy(r => r.Id)
                    .SkipWhile((r, index) => index < 2)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                Assert.Equal(new[] { 1, 2, 3 }, takeIds);
                Assert.Equal(new[] { 4, 5 }, skipIds);
                Assert.Equal(new[] { 1, 2, 3 }, indexTakeIds);
                Assert.Equal(new[] { 3, 4, 5 }, indexSkipIds);
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
        var id = ctx.Provider.Escape(nameof(TakeSkipWhileLiveRow.Id));
        var cat = ctx.Provider.Escape(nameof(TakeSkipWhileLiveRow.Cat));
        var score = ctx.Provider.Escape(nameof(TakeSkipWhileLiveRow.Score));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(20)" : kind == ProviderKind.MySql ? "VARCHAR(20)" : "TEXT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {cat} {textType} NOT NULL, {score} {intType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{cat},{score}) VALUES " +
            "(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'a',10)");
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
    private sealed class TakeSkipWhileLiveRow
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
