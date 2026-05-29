using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderDistinctByParityTests
{
    private const string TableName = "DistinctByLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DistinctBy_keeps_first_row_per_key_on_live_provider(ProviderKind kind)
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
                var ids = (await ctx.Query<DistinctByLiveRow>()
                    .OrderBy(r => r.Id)
                    .DistinctBy(r => r.Category)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                Assert.Equal(new[] { 1, 3, 5 }, ids);
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
    public async Task ExceptBy_and_IntersectBy_keep_first_matching_key_rows_on_live_provider(ProviderKind kind)
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
                var excluded = new[] { "A" };
                var keep = new[] { "B" };

                var exceptIds = (await ctx.Query<DistinctByLiveRow>()
                    .OrderBy(r => r.Id)
                    .ExceptBy(excluded, r => r.Category)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                var intersectIds = (await ctx.Query<DistinctByLiveRow>()
                    .OrderBy(r => r.Id)
                    .IntersectBy(keep, r => r.Category)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                Assert.Equal(new[] { 3, 5 }, exceptIds);
                Assert.Equal(new[] { 3 }, intersectIds);
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
    public async Task UnionBy_appends_local_rows_by_new_key_on_live_provider(ProviderKind kind)
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
                var other = new[]
                {
                    new DistinctByLiveRow { Id = 98, Category = "A", Amount = 1 },
                    new DistinctByLiveRow { Id = 99, Category = "X", Amount = 2 },
                };

                var ids = (await ctx.Query<DistinctByLiveRow>()
                    .OrderBy(r => r.Id)
                    .UnionBy(other, r => r.Category)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();

                Assert.Equal(new[] { 1, 3, 5, 99 }, ids);
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
        var id = ctx.Provider.Escape(nameof(DistinctByLiveRow.Id));
        var category = ctx.Provider.Escape(nameof(DistinctByLiveRow.Category));
        var amount = ctx.Provider.Escape(nameof(DistinctByLiveRow.Amount));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(20)" : kind == ProviderKind.MySql ? "VARCHAR(20)" : "TEXT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {category} {textType} NOT NULL, {amount} {intType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{category},{amount}) VALUES " +
            "(1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50)");
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
    private sealed class DistinctByLiveRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Amount { get; set; }
    }
}
