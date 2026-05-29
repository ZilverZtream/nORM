using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ProviderMobilityStrictCommonSurfaceTests
{
    [Fact]
    public async Task Strict_provider_mobility_allows_common_predicates_terminals_and_set_operators()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateCatalogSchemaAsync(connection);

        await using var ctx = new DbContext(connection, new SqliteProvider(), new DbContextOptions().UseStrictProviderMobility());

        var ids = new[] { 1, 3, 4, 6 };
        var filtered = await ctx.Query<StrictCatalogItem>()
            .Where(item =>
                ids.Contains(item.Id) &&
                item.Active &&
                !string.IsNullOrWhiteSpace(item.Name) &&
                item.Name.StartsWith("A") &&
                item.Sku.EndsWith("1"))
            .OrderBy(item => item.Id)
            .Select(item => new StrictCatalogIdDto { Id = item.Id })
            .ToListAsync();

        Assert.Equal(new[] { 1, 3 }, filtered.Select(item => item.Id));

        Assert.True(await ctx.Query<StrictCatalogItem>().AnyAsync(item => item.Price > 90m));
        Assert.True(await ctx.Query<StrictCatalogItem>().Where(item => item.Active).AllAsync(item => item.Price > 0m));
        Assert.Equal(4, await ctx.Query<StrictCatalogItem>().CountAsync(item => item.Active));
        Assert.Equal(100m, await ctx.Query<StrictCatalogItem>().MaxAsync(item => item.Price));
        Assert.Equal(10m, await ctx.Query<StrictCatalogItem>().MinAsync(item => item.Price));
        Assert.Equal("Alpha", (await ctx.Query<StrictCatalogItem>().OrderBy(item => item.Id).FirstAsync()).Name);
        Assert.Equal("Anvil", (await ctx.Query<StrictCatalogItem>().OrderBy(item => item.Id).LastAsync()).Name);
        Assert.Equal("Beta", (await ctx.Query<StrictCatalogItem>().SingleAsync(item => item.Id == 2)).Name);
        Assert.Equal("Archive", (await ctx.Query<StrictCatalogItem>().OrderBy(item => item.Name).ElementAtAsync(2)).Name);
        Assert.Equal("Alpha", (await ctx.Query<StrictCatalogItem>().MinByAsync(item => item.Price)).Name);
        Assert.Equal("Atlas", (await ctx.Query<StrictCatalogItem>().MaxByAsync(item => item.Price)).Name);

        var array = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Active)
            .OrderBy(item => item.Id)
            .ToArrayAsync();
        Assert.Equal(new[] { 1, 2, 3, 4 }, array.Select(item => item.Id));

        var dictionary = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Active)
            .ToDictionaryAsync(item => item.Id, item => item.Name);
        Assert.Equal("Archive", dictionary[4]);

        var hashSet = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Category == "tools")
            .ToHashSetAsync();
        Assert.Equal(3, hashSet.Count);

        var reversed = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .Reverse()
            .Take(2)
            .ToListAsync();
        Assert.Equal(new[] { 6, 5 }, reversed.Select(item => item.Id));

        var distinctCategories = await ctx.Query<StrictCatalogItem>()
            .Select(item => new StrictCategoryDto { Category = item.Category })
            .Distinct()
            .OrderBy(item => item.Category)
            .ToListAsync();
        Assert.Equal(new[] { "books", "games", "tools" }, distinctCategories.Select(item => item.Category));

        var distinctByCategories = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .DistinctBy(item => item.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 5 }, distinctByCategories.Select(item => item.Id));

        var takeWhile = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .TakeWhile(item => item.Price < 60m)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, takeWhile.Select(item => item.Id));

        var skipWhile = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .SkipWhile(item => item.Price < 60m)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5, 6 }, skipWhile.Select(item => item.Id));

        var exceptBy = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .ExceptBy(new[] { "tools" }, item => item.Category)
            .ToListAsync();
        Assert.Equal(new[] { 2, 5 }, exceptBy.Select(item => item.Id));

        var intersectBy = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .IntersectBy(new[] { "books" }, item => item.Category)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, intersectBy.Select(item => item.Id));

        var unionBy = await ctx.Query<StrictCatalogItem>()
            .OrderBy(item => item.Id)
            .UnionBy(new[]
            {
                new StrictCatalogItem { Id = 90, Category = "tools", Name = "Tool duplicate", Sku = "T-0", Price = 1m, Active = true },
                new StrictCatalogItem { Id = 91, Category = "garden", Name = "Garden", Sku = "G-9", Price = 9m, Active = true }
            }, item => item.Category)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 5, 91 }, unionBy.Select(item => item.Id));

        var leftSequence = ctx.Query<StrictCatalogItem>()
            .Where(item => item.Active)
            .OrderBy(item => item.Id);
        var rightSequence = ctx.Query<StrictCatalogItem>()
            .Where(item => item.Id <= 4)
            .OrderBy(item => item.Id);
        Assert.True(leftSequence.SequenceEqual(rightSequence));

        var union = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Category == "tools")
            .Union(ctx.Query<StrictCatalogItem>().Where(item => item.Price >= 90m))
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 6 }, union.Select(item => item.Id));

        var intersect = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Category == "tools")
            .Intersect(ctx.Query<StrictCatalogItem>().Where(item => item.Active))
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, intersect.Select(item => item.Id));

        var except = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Category == "tools")
            .Except(ctx.Query<StrictCatalogItem>().Where(item => item.Price >= 90m))
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, except.Select(item => item.Id));

        var concat = await ctx.Query<StrictCatalogItem>()
            .Where(item => item.Id == 1)
            .Concat(ctx.Query<StrictCatalogItem>().Where(item => item.Id == 2))
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, concat.Select(item => item.Id));
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_common_join_and_left_join_shapes()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateCatalogSchemaAsync(connection);

        await using var ctx = new DbContext(connection, new SqliteProvider(), new DbContextOptions().UseStrictProviderMobility());

        var joined = await ctx.Query<StrictOrder>()
            .Join(ctx.Query<StrictCatalogItem>(),
                order => order.ItemId,
                item => item.Id,
                (order, item) => new StrictOrderLineDto
                {
                    OrderId = order.Id,
                    ItemName = item.Name,
                    Quantity = order.Quantity,
                    LineTotal = item.Price * order.Quantity
                })
            .OrderBy(row => row.OrderId)
            .ToListAsync();

        Assert.Equal(4, joined.Count);
        Assert.Equal("Alpha", joined[0].ItemName);
        Assert.Equal(20m, joined[0].LineTotal);

        var leftJoined = await ctx.Query<StrictCatalogItem>()
            .GroupJoin(ctx.Query<StrictOrder>(),
                item => item.Id,
                order => order.ItemId,
                (item, orders) => new { item, orders })
            .SelectMany(group => group.orders.DefaultIfEmpty(),
                (group, order) => new StrictItemOrderDto
                {
                    ItemId = group.item.Id,
                    ItemName = group.item.Name,
                    OrderId = order == null ? 0 : order.Id
                })
            .OrderBy(row => row.ItemId)
            .ThenBy(row => row.OrderId)
            .ToListAsync();

        Assert.Contains(leftJoined, row => row.ItemId == 4 && row.OrderId == 0);
        Assert.Contains(leftJoined, row => row.ItemId == 1 && row.OrderId == 100);
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_temporal_scalar_types_in_normal_queries()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateTemporalSchemaAsync(connection);

        await using var ctx = new DbContext(connection, new SqliteProvider(), new DbContextOptions().UseStrictProviderMobility());

        var start = new DateOnly(2026, 1, 1);
        var open = new TimeOnly(8, 0);
        var rows = await ctx.Query<StrictTemporalRow>()
            .Where(row => row.BusinessDate >= start && row.OpenAt >= open && row.Duration > TimeSpan.FromMinutes(30))
            .OrderBy(row => row.BusinessDate)
            .Select(row => new StrictTemporalDto
            {
                Id = row.Id,
                BusinessDate = row.BusinessDate,
                OpenAt = row.OpenAt
            })
            .ToListAsync();

        Assert.Equal(new[] { 2, 3 }, rows.Select(row => row.Id));
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_evidence_backed_constrained_linq_shapes()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        connection.CreateFunction<string, string, bool>(
            "regexp",
            (pattern, input) => input is not null && Regex.IsMatch(input, pattern));
        await CreateConstrainedShapeSchemaAsync(connection);

        await using var ctx = new DbContext(connection, new SqliteProvider(), new DbContextOptions().UseStrictProviderMobility());

        var castRows = await ctx.Query<StrictCatalogItem>()
            .Cast<StrictCatalogItem>()
            .Where(item => item.Active)
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3, 4 }, castRows.Select(item => item.Id));

        var dogs = await ctx.Query<Animal>()
            .OfType<Dog>()
            .Where(dog => dog.GoodBoy)
            .OrderBy(dog => dog.Id)
            .ToListAsync();
        Assert.Single(dogs);
        Assert.Equal(2, dogs[0].Id);

        var literal = new DateTime(2026, 5, 25, 12, 30, 45, 123, DateTimeKind.Utc);
        var matchingInstants = await ctx.Query<StrictDateTimeOffsetRow>()
            .Where(row => row.StartsAt == literal)
            .OrderBy(row => row.Id)
            .Select(row => new StrictDateTimeOffsetDto
            {
                Id = row.Id,
                LocalStart = row.StartsAt.LocalDateTime
            })
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, matchingInstants.Select(row => row.Id));

        var regexRows = await ctx.Query<StrictCatalogItem>()
            .Where(item => Regex.IsMatch(item.Name, "^A"))
            .OrderBy(item => item.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 4, 6 }, regexRows.Select(row => row.Id));
    }

    [Fact]
    public async Task Strict_provider_mobility_allows_include_theninclude_split_query()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await CreateIncludeSchemaAsync(connection);

        var options = new DbContextOptions().UseStrictProviderMobility();
        options.OnModelCreating = mb =>
        {
            mb.Entity<StrictAuthor>().HasKey(author => author.Id);
            mb.Entity<StrictBook>().HasKey(book => book.Id);
            mb.Entity<StrictChapter>().HasKey(chapter => chapter.Id);
            mb.Entity<StrictAuthor>()
                .HasMany(author => author.Books)
                .WithOne()
                .HasForeignKey(book => book.AuthorId, author => author.Id);
            mb.Entity<StrictBook>()
                .HasMany(book => book.Chapters)
                .WithOne()
                .HasForeignKey(chapter => chapter.BookId, book => book.Id);
        };

        await using var ctx = new DbContext(connection, new SqliteProvider(), options);

        var authors = await ((INormQueryable<StrictAuthor>)ctx.Query<StrictAuthor>())
            .Include(author => author.Books)
                .ThenInclude(book => book.Chapters)
            .AsSplitQuery()
            .OrderBy(author => author.Id)
            .ToListAsync();

        Assert.Equal(2, authors.Count);
        Assert.Equal(2, authors[0].Books.Count);
        Assert.Equal(2, authors[0].Books.Single(book => book.Title == "A1").Chapters.Count);
        Assert.Single(authors[1].Books);
        Assert.Single(authors[1].Books[0].Chapters);
    }

    private static async Task CreateCatalogSchemaAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE StrictCatalogItem (
                Id INTEGER PRIMARY KEY,
                Category TEXT NOT NULL,
                Name TEXT NOT NULL,
                Sku TEXT NOT NULL,
                Price NUMERIC NOT NULL,
                Active INTEGER NOT NULL
            );
            CREATE TABLE StrictOrder (
                Id INTEGER PRIMARY KEY,
                ItemId INTEGER NOT NULL,
                Quantity INTEGER NOT NULL
            );
            INSERT INTO StrictCatalogItem VALUES
                (1, 'tools', 'Alpha', 'A-1', 10.00, 1),
                (2, 'books', 'Beta', 'B-2', 20.00, 1),
                (3, 'tools', 'Atlas', 'A-1', 100.00, 1),
                (4, 'books', 'Archive', 'A-2', 50.00, 1),
                (5, 'games', 'Gamma', 'G-1', 70.00, 0),
                (6, 'tools', 'Anvil', 'A-2', 95.00, 0);
            INSERT INTO StrictOrder VALUES
                (100, 1, 2),
                (101, 2, 1),
                (102, 3, 1),
                (103, 3, 3);
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateTemporalSchemaAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE StrictTemporalRow (
                Id INTEGER PRIMARY KEY,
                BusinessDate TEXT NOT NULL,
                OpenAt TEXT NOT NULL,
                Duration TEXT NOT NULL
            );
            INSERT INTO StrictTemporalRow VALUES
                (1, '2025-12-31', '07:00:00', '00:45:00'),
                (2, '2026-01-01', '08:30:00', '00:45:00'),
                (3, '2026-01-02', '09:15:00', '01:10:00');
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateConstrainedShapeSchemaAsync(SqliteConnection connection)
    {
        await CreateCatalogSchemaAsync(connection);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE Animal(Id INTEGER PRIMARY KEY, Type TEXT, Lives INTEGER, GoodBoy INTEGER);
            INSERT INTO Animal VALUES(1,'Cat',9,NULL);
            INSERT INTO Animal VALUES(2,'Dog',NULL,1);

            CREATE TABLE StrictDateTimeOffsetRow (
                Id INTEGER PRIMARY KEY,
                StartsAt TEXT NOT NULL
            );
            INSERT INTO StrictDateTimeOffsetRow VALUES
                (1,'2026-05-25 12:30:45.123+00:00'),
                (2,'2026-05-25 14:30:45.123+02:00'),
                (3,'2026-05-25 12:30:45.987+00:00');
            """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task CreateIncludeSchemaAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE StrictAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE StrictBook (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);
            CREATE TABLE StrictChapter (Id INTEGER PRIMARY KEY, BookId INTEGER NOT NULL, Heading TEXT NOT NULL);
            INSERT INTO StrictAuthor VALUES (1, 'Ada'), (2, 'Grace');
            INSERT INTO StrictBook VALUES (10, 1, 'A1'), (11, 1, 'A2'), (20, 2, 'G1');
            INSERT INTO StrictChapter VALUES (100, 10, 'A1-C1'), (101, 10, 'A1-C2'), (110, 11, 'A2-C1'), (200, 20, 'G1-C1');
            """;
        await command.ExecuteNonQueryAsync();
    }

    [Table("StrictCatalogItem")]
    public sealed class StrictCatalogItem
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = "";
        public string Name { get; set; } = "";
        public string Sku { get; set; } = "";
        public decimal Price { get; set; }
        public bool Active { get; set; }
    }

    [Table("StrictOrder")]
    public sealed class StrictOrder
    {
        [Key] public int Id { get; set; }
        public int ItemId { get; set; }
        public int Quantity { get; set; }
    }

    [Table("StrictTemporalRow")]
    public sealed class StrictTemporalRow
    {
        [Key] public int Id { get; set; }
        public DateOnly BusinessDate { get; set; }
        public TimeOnly OpenAt { get; set; }
        public TimeSpan Duration { get; set; }
    }

    [Table("StrictAuthor")]
    public sealed class StrictAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<StrictBook> Books { get; set; } = new();
    }

    [Table("StrictBook")]
    public sealed class StrictBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = "";
        public List<StrictChapter> Chapters { get; set; } = new();
    }

    [Table("StrictChapter")]
    public sealed class StrictChapter
    {
        [Key] public int Id { get; set; }
        public int BookId { get; set; }
        public string Heading { get; set; } = "";
    }

    public sealed class StrictOrderLineDto
    {
        public int OrderId { get; set; }
        public string ItemName { get; set; } = "";
        public int Quantity { get; set; }
        public decimal LineTotal { get; set; }
    }

    public sealed class StrictCatalogIdDto
    {
        public int Id { get; set; }
    }

    public sealed class StrictCategoryDto
    {
        public string Category { get; set; } = "";
    }

    public sealed class StrictItemOrderDto
    {
        public int ItemId { get; set; }
        public string ItemName { get; set; } = "";
        public int OrderId { get; set; }
    }

    public sealed class StrictTemporalDto
    {
        public int Id { get; set; }
        public DateOnly BusinessDate { get; set; }
        public TimeOnly OpenAt { get; set; }
    }

    [Table("StrictDateTimeOffsetRow")]
    public sealed class StrictDateTimeOffsetRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset StartsAt { get; set; }
    }

    public sealed class StrictDateTimeOffsetDto
    {
        public int Id { get; set; }
        public DateTime LocalStart { get; set; }
    }
}
