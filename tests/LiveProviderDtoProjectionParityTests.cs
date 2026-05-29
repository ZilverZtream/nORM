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
/// Live-provider parity for common DTO projection shapes: anonymous object,
/// parameterized constructor, init-only member init, and positional record.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderDtoProjectionParityTests
{
    private const string TableName = "DtoProjectionLiveRow";

    [Table(TableName)]
    public sealed class DtoProjectionLiveRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Amount { get; set; }
    }

    public sealed class CtorDto
    {
        public CtorDto(int id, string name, int amount)
        {
            Id = id;
            Name = name;
            Amount = amount;
        }

        public int Id { get; }
        public string Name { get; }
        public int Amount { get; }
    }

    public sealed class InitDto
    {
        public int Id { get; init; }
        public string Name { get; init; } = "";
        public int Amount { get; init; }
    }

    public sealed record RecordDto(int Id, string Name, int Amount);

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({len})"
        : $"VARCHAR({len})";

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(TableName);
        var id = ctx.Provider.Escape("Id");
        var name = ctx.Provider.Escape("Name");
        var amount = ctx.Provider.Escape("Amount");

        await ExecuteAsync(ctx, DropTable(kind, TableName, table));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {table} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL, " +
            $"{amount} {IntCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{name},{amount}) VALUES " +
            "(1,'alpha',10)," +
            "(2,'bravo',20)," +
            "(3,'charlie',30)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, TableName, ctx.Provider.Escape(TableName)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dto_projection_shapes_materialize_correctly_on_live_provider(ProviderKind kind)
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
                var anonymous = (await ctx.Query<DtoProjectionLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, r.Name, r.Amount })
                    .ToListAsync())
                    .ToArray();

                var ctor = (await ctx.Query<DtoProjectionLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new CtorDto(r.Id, r.Name, r.Amount))
                    .ToListAsync())
                    .ToArray();

                var init = (await ctx.Query<DtoProjectionLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new InitDto { Id = r.Id, Name = r.Name, Amount = r.Amount })
                    .ToListAsync())
                    .ToArray();

                var record = (await ctx.Query<DtoProjectionLiveRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new RecordDto(r.Id, r.Name, r.Amount))
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(new[] { 1, 2, 3 }, anonymous.Select(r => r.Id).ToArray());
                Assert.Equal("bravo", anonymous[1].Name);
                Assert.Equal(20, ctor[1].Amount);
                Assert.Equal("charlie", init[2].Name);
                Assert.Equal(new RecordDto(1, "alpha", 10), record[0]);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
