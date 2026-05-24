using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies aggregate calls on a navigation collection inside a predicate translate to a
/// correlated EXISTS / NOT EXISTS / scalar subquery instead of throwing. These are the
/// "users.Where(u => u.Orders.Any(...))" shapes that real applications reach for.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NavAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NavBook   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL, Price NUMERIC NOT NULL);
            INSERT INTO NavAuthor VALUES
                (1,'Ada'),
                (2,'Betty'),
                (3,'Grace'),
                (4,'OrphanAuthor');
            INSERT INTO NavBook VALUES
                (1, 1, 'Ada-Cheap',   5),
                (2, 1, 'Ada-Mid',    20),
                (3, 2, 'Betty-Big',  50),
                (4, 3, 'Grace-Mid',  25),
                (5, 3, 'Grace-Big',  60);
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NavAuthor>().HasKey(a => a.Id);
                mb.Entity<NavBook>().HasKey(b => b.Id);
                mb.Entity<NavAuthor>().HasMany(a => a.Books).WithOne()
                    .HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Any_over_navigation_with_predicate_returns_parents_having_a_match()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Any(b => b.Price >= 40))
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public async Task Any_over_navigation_without_predicate_returns_parents_with_any_child()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Any())
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        // OrphanAuthor (id=4) has no books, so excluded.
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task Any_over_navigation_returning_no_match_yields_empty_result()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Any(b => b.Price > 1000))
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        Assert.Empty(ids);
    }

    [Fact]
    public async Task All_over_navigation_returns_parents_whose_children_all_match()
    {
        // All books >= 30: Betty (single book at 50) passes, Grace fails (25), Ada fails (5).
        // EF semantics: a parent with NO children also satisfies All (vacuous truth).
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.All(b => b.Price >= 30))
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        Assert.Contains(2, ids);
        Assert.Contains(4, ids); // vacuous truth - no children
        Assert.DoesNotContain(1, ids);
        Assert.DoesNotContain(3, ids);
    }

    [Fact]
    public async Task Count_over_navigation_used_as_filter_compares_correctly()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Count() >= 2)
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        // Ada has 2, Grace has 2; Betty has 1; Orphan has 0.
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public async Task Count_over_navigation_with_predicate_filters_subquery_rows()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Count(b => b.Price >= 30) >= 1)
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public async Task Negated_Any_over_navigation_returns_parents_with_no_match()
    {
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => !a.Books.Any(b => b.Price >= 100))
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        // None of the authors have a book priced >= 100, so all match.
        Assert.Equal(new[] { 1, 2, 3, 4 }, ids);
    }

    [Fact]
    public async Task Any_with_closure_captured_threshold_runs_subquery_with_runtime_value()
    {
        // Regression: a closure-captured value inside a navigation aggregate predicate is
        // emitted as a compiled parameter by the sub-translator. If the sub-translator's
        // Dispose() wipes the outer's compiled-param list, BindPlanParameters binds the
        // placeholder DBNull and the query returns nothing. This test pins the live value.
        int threshold = 40;
        var ids = (await _ctx.Query<NavAuthor>()
            .Where(a => a.Books.Any(b => b.Price >= threshold))
            .OrderBy(a => a.Id)
            .ToListAsync())
            .Select(a => a.Id).ToArray();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Table("NavAuthor")]
    public sealed class NavAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NavBook> Books { get; set; } = new();
    }

    [Table("NavBook")]
    public sealed class NavBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }
}
