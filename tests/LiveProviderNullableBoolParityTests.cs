using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for nullable bool three-valued predicate semantics.
/// SQL boolean storage differs across providers, so this must execute against
/// each real provider rather than relying on dialect-shape tests.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderNullableBoolParityTests
{
    private const string Table = "NullableBoolParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Nullable_bool_predicates_match_linq_three_valued_logic_on_live_provider(ProviderKind kind)
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
                var trueIds = (await ctx.Query<NullableBoolParityRow>()
                    .Where(r => r.Flag == true)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 1, 4 }, trueIds);

                var falseIds = (await ctx.Query<NullableBoolParityRow>()
                    .Where(r => r.Flag == false)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 2, 5 }, falseIds);

                var nullIds = (await ctx.Query<NullableBoolParityRow>()
                    .Where(r => r.Flag == null)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 3 }, nullIds);

                var hasValueIds = (await ctx.Query<NullableBoolParityRow>()
                    .Where(r => r.Flag.HasValue)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 1, 2, 4, 5 }, hasValueIds);

                var compiledTrue = Norm.CompileQuery((DbContext c, int minId) =>
                    c.Query<NullableBoolParityRow>()
                        .Where(r => r.Id >= minId && r.Flag == true)
                        .OrderBy(r => r.Id));
                var compiledTrueIds = (await compiledTrue(ctx, 2)).Select(r => r.Id).ToArray();
                Assert.Equal(new[] { 4 }, compiledTrueIds);

                var compiledHasValue = Norm.CompileQuery((DbContext c, int minId) =>
                    c.Query<NullableBoolParityRow>()
                        .Where(r => r.Id >= minId && r.Flag.HasValue)
                        .OrderBy(r => r.Id));
                var compiledHasValueIds = (await compiledHasValue(ctx, 2)).Select(r => r.Id).ToArray();
                Assert.Equal(new[] { 2, 4, 5 }, compiledHasValueIds);
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
        var id = ctx.Provider.Escape(nameof(NullableBoolParityRow.Id));
        var flag = ctx.Provider.Escape(nameof(NullableBoolParityRow.Flag));
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var boolType = kind switch
        {
            ProviderKind.SqlServer => "BIT",
            ProviderKind.Postgres => "BOOLEAN",
            ProviderKind.MySql => "BOOLEAN",
            ProviderKind.Sqlite => "INTEGER",
            _ => "BOOLEAN"
        };

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {flag} {boolType} NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{flag}) VALUES " +
            $"(1,{ctx.Provider.BooleanTrueLiteral})," +
            $"(2,{ctx.Provider.BooleanFalseLiteral})," +
            "(3,NULL)," +
            $"(4,{ctx.Provider.BooleanTrueLiteral})," +
            $"(5,{ctx.Provider.BooleanFalseLiteral})");
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
    private sealed class NullableBoolParityRow
    {
        [Key] public int Id { get; set; }
        public bool? Flag { get; set; }
    }
}
