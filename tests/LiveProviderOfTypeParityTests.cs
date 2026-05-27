using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider proof for the constrained OfType/Cast LINQ row. SQLite-only
/// probes verify the local translator shape; this verifies the discriminator
/// predicate and derived materializer against each configured provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderOfTypeParityTests
{
    private const string Table = "Animal";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task OfType_TPH_derived_filter_materializes_only_matching_rows(ProviderKind kind)
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
                var dogs = await ctx.Query<Animal>()
                    .OfType<Dog>()
                    .Where(d => d.GoodBoy)
                    .OrderBy(d => d.Id)
                    .ToListAsync();

                Assert.Single(dogs);
                Assert.Equal(2, dogs[0].Id);
                Assert.True(dogs[0].GoodBoy);

                var cats = await ctx.Query<Animal>()
                    .OfType<Cat>()
                    .OrderBy(c => c.Id)
                    .ToListAsync();

                Assert.Single(cats);
                Assert.Equal(1, cats[0].Id);
                Assert.Equal(9, cats[0].Lives);
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
        var id = ctx.Provider.Escape(nameof(Animal.Id));
        var type = ctx.Provider.Escape(nameof(Animal.Type));
        var lives = ctx.Provider.Escape(nameof(Cat.Lives));
        var goodBoy = ctx.Provider.Escape(nameof(Dog.GoodBoy));

        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";
        var boolType = kind switch
        {
            ProviderKind.SqlServer => "BIT",
            ProviderKind.Postgres => "BOOLEAN",
            ProviderKind.MySql => "BOOLEAN",
            _ => "INTEGER"
        };
        var textType = kind == ProviderKind.Sqlite ? "TEXT" : "VARCHAR(20)";
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var trueLiteral = kind == ProviderKind.Postgres ? "TRUE" : "1";

        await ExecuteAsync(ctx, $"{drop} CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {type} {textType} NOT NULL, {lives} {intType} NULL, {goodBoy} {boolType} NULL)");
        await ExecuteAsync(ctx, $"INSERT INTO {table} ({id},{type},{lives},{goodBoy}) VALUES (1,'Cat',9,NULL),(2,'Dog',NULL,{trueLiteral})");
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
}
