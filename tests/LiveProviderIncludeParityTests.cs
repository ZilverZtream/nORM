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
/// Live-provider parity for Include/ThenInclude eager loading. The v1 contract
/// requires AsSplitQuery for eager loading; plain Include leaves navigation
/// collections at their default values.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderIncludeParityTests
{
    private const string AuthorTable = "IncludeLiveAuthor";
    private const string BookTable = "IncludeLiveBook";
    private const string ChapterTable = "IncludeLiveChapter";

    [Table(AuthorTable)]
    public sealed class IncludeLiveAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<IncludeLiveBook> Books { get; set; } = new();
    }

    [Table(BookTable)]
    public sealed class IncludeLiveBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = "";
        public List<IncludeLiveChapter> Chapters { get; set; } = new();
    }

    [Table(ChapterTable)]
    public sealed class IncludeLiveChapter
    {
        [Key] public int Id { get; set; }
        public int BookId { get; set; }
        public string Heading { get; set; } = "";
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({len})"
        : $"VARCHAR({len})";

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<IncludeLiveAuthor>().HasKey(a => a.Id);
            mb.Entity<IncludeLiveBook>().HasKey(b => b.Id);
            mb.Entity<IncludeLiveChapter>().HasKey(c => c.Id);
            mb.Entity<IncludeLiveAuthor>()
                .HasMany(a => a.Books)
                .WithOne()
                .HasForeignKey(b => b.AuthorId, a => a.Id);
            mb.Entity<IncludeLiveBook>()
                .HasMany(b => b.Chapters)
                .WithOne()
                .HasForeignKey(c => c.BookId, b => b.Id);
        }
    };

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
        var chapter = ctx.Provider.Escape(ChapterTable);
        var id = ctx.Provider.Escape("Id");
        var name = ctx.Provider.Escape("Name");
        var authorId = ctx.Provider.Escape("AuthorId");
        var title = ctx.Provider.Escape("Title");
        var bookId = ctx.Provider.Escape("BookId");
        var heading = ctx.Provider.Escape("Heading");

        await ExecuteAsync(ctx, DropTable(kind, ChapterTable, chapter));
        await ExecuteAsync(ctx, DropTable(kind, BookTable, book));
        await ExecuteAsync(ctx, DropTable(kind, AuthorTable, author));
        await ExecuteAsync(ctx, $"CREATE TABLE {author} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {book} ({id} {IntCol(kind)} PRIMARY KEY, {authorId} {IntCol(kind)} NOT NULL, " +
            $"{title} {VarCol(kind, 60)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {chapter} ({id} {IntCol(kind)} PRIMARY KEY, {bookId} {IntCol(kind)} NOT NULL, " +
            $"{heading} {VarCol(kind, 60)} NOT NULL)");
        await ExecuteAsync(ctx, $"INSERT INTO {author} ({id},{name}) VALUES (1,'Ada'),(2,'Grace')");
        await ExecuteAsync(ctx,
            $"INSERT INTO {book} ({id},{authorId},{title}) VALUES " +
            "(10,1,'Ada-B1')," +
            "(11,1,'Ada-B2')," +
            "(20,2,'Grace-B1')");
        await ExecuteAsync(ctx,
            $"INSERT INTO {chapter} ({id},{bookId},{heading}) VALUES " +
            "(100,10,'Ada-B1-C1')," +
            "(101,10,'Ada-B1-C2')," +
            "(110,11,'Ada-B2-C1')," +
            "(200,20,'Grace-B1-C1')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, ChapterTable, ctx.Provider.Escape(ChapterTable)));
            await ExecuteAsync(ctx, DropTable(kind, BookTable, ctx.Provider.Escape(BookTable)));
            await ExecuteAsync(ctx, DropTable(kind, AuthorTable, ctx.Provider.Escape(AuthorTable)));
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
    public async Task Include_and_ThenInclude_match_split_query_contract_on_live_provider(ProviderKind kind)
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
                var withoutSplit = await ((INormQueryable<IncludeLiveAuthor>)ctx.Query<IncludeLiveAuthor>())
                    .Include(a => a.Books)
                    .OrderBy(a => a.Id)
                    .ToListAsync();

                Assert.Equal(2, withoutSplit.Count);
                Assert.All(withoutSplit, a => Assert.Empty(a.Books));

                var authors = (await ((INormQueryable<IncludeLiveAuthor>)ctx.Query<IncludeLiveAuthor>())
                    .Include(a => a.Books)
                        .ThenInclude(b => b.Chapters)
                    .AsSplitQuery()
                    .OrderBy(a => a.Id)
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(2, authors.Length);

                var ada = authors[0];
                Assert.Equal("Ada", ada.Name);
                Assert.Equal(2, ada.Books.Count);
                Assert.Equal(2, ada.Books.Single(b => b.Title == "Ada-B1").Chapters.Count);
                Assert.Single(ada.Books.Single(b => b.Title == "Ada-B2").Chapters);

                var grace = authors[1];
                Assert.Equal("Grace", grace.Name);
                Assert.Single(grace.Books);
                Assert.Single(grace.Books[0].Chapters);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
