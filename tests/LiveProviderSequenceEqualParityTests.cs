using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderSequenceEqualParityTests
{
    private const string TableName = "SequenceEqualLiveRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Ordered_queryable_SequenceEqual_matches_dotnet_sequence_semantics(ProviderKind kind)
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
                var left = ctx.Query<SequenceEqualLiveRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
                var same = ctx.Query<SequenceEqualLiveRow>().Where(r => r.Score < 40).OrderBy(r => r.Id);
                var differentOrder = ctx.Query<SequenceEqualLiveRow>().Where(r => r.Id <= 3).OrderByDescending(r => r.Id);
                var sameLocal = new[]
                {
                    new SequenceEqualLiveRow { Id = 1, Name = "a", Score = 10 },
                    new SequenceEqualLiveRow { Id = 2, Name = "b", Score = 20 },
                    new SequenceEqualLiveRow { Id = 3, Name = "c", Score = 30 }
                };
                var differentLocalOrder = new[]
                {
                    new SequenceEqualLiveRow { Id = 3, Name = "c", Score = 30 },
                    new SequenceEqualLiveRow { Id = 2, Name = "b", Score = 20 },
                    new SequenceEqualLiveRow { Id = 1, Name = "a", Score = 10 }
                };

                Assert.True(left.SequenceEqual(same));
                Assert.False(left.SequenceEqual(differentOrder));
                Assert.True(left.SequenceEqual(sameLocal));
                Assert.False(left.SequenceEqual(differentLocalOrder));
                Assert.True(ctx.Query<SequenceEqualLiveRow>().Where(r => r.Id > 99).OrderBy(r => r.Id).SequenceEqual(Array.Empty<SequenceEqualLiveRow>()));
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
        var id = ctx.Provider.Escape(nameof(SequenceEqualLiveRow.Id));
        var name = ctx.Provider.Escape(nameof(SequenceEqualLiveRow.Name));
        var score = ctx.Provider.Escape(nameof(SequenceEqualLiveRow.Score));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(20)" : kind == ProviderKind.MySql ? "VARCHAR(20)" : "TEXT";

        await ExecuteAsync(ctx, DropTable(kind, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {name} {textType} NOT NULL, {score} {intType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name},{score}) VALUES " +
            "(1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)");
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
    private sealed class SequenceEqualLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
