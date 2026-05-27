using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for navigation collection aggregates translated as correlated subqueries.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderNavigationAggregateParityTests
{
    private const string AuthorTable = "NavAggLiveAuthor";
    private const string BookTable = "NavAggLiveBook";

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
        var author = ctx.Provider.Escape(AuthorTable);
        var book = ctx.Provider.Escape(BookTable);
        var id = ctx.Provider.Escape("Id");
        var name = ctx.Provider.Escape("Name");
        var authorId = ctx.Provider.Escape("AuthorId");
        var title = ctx.Provider.Escape("Title");
        var price = ctx.Provider.Escape("Price");
        var active = ctx.Provider.Escape("IsActive");

        await ExecuteAsync(ctx, DropTable(kind, BookTable, book));
        await ExecuteAsync(ctx, DropTable(kind, AuthorTable, author));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {author} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {book} ({id} {IntCol(kind)} PRIMARY KEY, {authorId} {IntCol(kind)} NOT NULL, " +
            $"{title} {VarCol(kind, 60)} NOT NULL, {price} {IntCol(kind)} NOT NULL, {active} {IntCol(kind)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {author} ({id},{name}) VALUES " +
            "(1,'Ada'),(2,'Betty'),(3,'Grace'),(4,'OrphanAuthor')");
        await ExecuteAsync(ctx,
            $"INSERT INTO {book} ({id},{authorId},{title},{price},{active}) VALUES " +
            "(1,1,'Ada-Cheap',5,1)," +
            "(2,1,'Ada-Mid',20,0)," +
            "(3,2,'Betty-Big',50,1)," +
            "(4,3,'Grace-Mid',25,0)," +
            "(5,3,'Grace-Big',60,1)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, BookTable, ctx.Provider.Escape(BookTable)));
            await ExecuteAsync(ctx, DropTable(kind, AuthorTable, ctx.Provider.Escape(AuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<NavAggLiveAuthor>().HasKey(a => a.Id);
            mb.Entity<NavAggLiveBook>().HasKey(b => b.Id);
            mb.Entity<NavAggLiveAuthor>().HasMany(a => a.Books).WithOne()
                .HasForeignKey(b => b.AuthorId, a => a.Id);
        }
    };

    [Table(AuthorTable)]
    public sealed class NavAggLiveAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<NavAggLiveBook> Books { get; set; } = new();
    }

    [Table(BookTable)]
    public sealed class NavAggLiveBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = "";
        public int Price { get; set; }
        public int IsActive { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Navigation_collection_aggregates_match_linq_semantics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider, Options()))
        {
            await SetupAsync(ctx, kind);
            try
            {
                var anyIds = (await ctx.Query<NavAggLiveAuthor>()
                    .Where(a => a.Books.Any(b => b.Price >= 40))
                    .OrderBy(a => a.Id)
                    .ToListAsync())
                    .Select(a => a.Id)
                    .ToArray();

                var allIds = (await ctx.Query<NavAggLiveAuthor>()
                    .Where(a => a.Books.All(b => b.Price >= 30))
                    .OrderBy(a => a.Id)
                    .ToListAsync())
                    .Select(a => a.Id)
                    .ToArray();

                var countIds = (await ctx.Query<NavAggLiveAuthor>()
                    .Where(a => a.Books.Count() >= 2)
                    .OrderBy(a => a.Id)
                    .ToListAsync())
                    .Select(a => a.Id)
                    .ToArray();

                var filteredCountIds = (await ctx.Query<NavAggLiveAuthor>()
                    .Where(a => a.Books.Count(b => b.Price >= 30) >= 1)
                    .OrderBy(a => a.Id)
                    .ToListAsync())
                    .Select(a => a.Id)
                    .ToArray();

                var projectedCounts = (await ctx.Query<NavAggLiveAuthor>()
                    .OrderBy(a => a.Id)
                    .Select(a => new
                    {
                        a.Id,
                        TotalBooks = a.Books.Count(),
                        ActiveBooks = a.Books.Where(b => b.IsActive == 1).Count()
                    })
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(new[] { 2, 3 }, anyIds);
                Assert.Equal(new[] { 2, 4 }, allIds);
                Assert.Equal(new[] { 1, 3 }, countIds);
                Assert.Equal(new[] { 2, 3 }, filteredCountIds);
                Assert.Equal(new[] { 2, 1, 2, 0 }, projectedCounts.Select(r => r.TotalBooks).ToArray());
                Assert.Equal(new[] { 1, 1, 1, 0 }, projectedCounts.Select(r => r.ActiveBooks).ToArray());
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
