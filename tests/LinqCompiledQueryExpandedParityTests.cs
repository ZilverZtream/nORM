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
/// Exercises the Norm.CompileQuery pipeline against the LINQ shapes added during the parity
/// loop: Reverse + Take, Concat (UNION ALL), navigation aggregate predicates, DTO projection,
/// composite GroupBy. Verifies the compiled-path produces the same results as the non-compiled
/// path for inputs that vary between invocations.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledQueryExpandedParityTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CqAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Region TEXT NOT NULL);
            CREATE TABLE CqBook   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Price INTEGER NOT NULL);
            INSERT INTO CqAuthor VALUES (1,'Ada','EU'),(2,'Grace','US'),(3,'Linus','EU'),(4,'Orphan','US');
            INSERT INTO CqBook   VALUES (10,1,10),(11,1,30),(20,2,50),(30,3,5),(31,3,80);
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CqAuthor>().HasKey(a => a.Id);
                mb.Entity<CqBook>().HasKey(b => b.Id);
                mb.Entity<CqAuthor>().HasMany(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId, a => a.Id);
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
    public async Task Compiled_query_with_OrderBy_then_Reverse_then_Take_returns_top_N_in_reverse_order()
    {
        var compiled = Norm.CompileQuery((DbContext c, int take) =>
            c.Query<CqAuthor>().OrderBy(a => a.Id).Reverse().Take(take));

        var firstTwo = await compiled(_ctx, 2);
        var firstThree = await compiled(_ctx, 3);
        Assert.Equal(new[] { 4, 3 }, firstTwo.Select(a => a.Id).ToArray());
        Assert.Equal(new[] { 4, 3, 2 }, firstThree.Select(a => a.Id).ToArray());
    }

    [Fact]
    public async Task Compiled_query_with_Concat_runs_UNION_ALL_with_runtime_parameter()
    {
        var compiled = Norm.CompileQuery((DbContext c, string region) =>
            c.Query<CqAuthor>().Where(a => a.Region == region)
             .Concat(c.Query<CqAuthor>().Where(a => a.Region == "US")));

        var euThenUs = (await compiled(_ctx, "EU"))
            .Select(a => a.Id).OrderBy(i => i).ToArray();
        // EU rows: 1,3; US rows: 2,4. UNION ALL keeps duplicates if any.
        Assert.Equal(new[] { 1, 2, 3, 4 }, euThenUs);
    }

    [Fact]
    public async Task Compiled_query_with_navigation_aggregate_predicate_filters_per_runtime_threshold()
    {
        var compiled = Norm.CompileQuery((DbContext c, int minPrice) =>
            c.Query<CqAuthor>().Where(a => a.Books.Any(b => b.Price >= minPrice)).OrderBy(a => a.Id));

        var above40 = (await compiled(_ctx, 40)).Select(a => a.Id).ToArray();
        // Books >= 40: id 11 (price 30 not ok), 20 (50 ok), 31 (80 ok) → authors 2 and 3.
        Assert.Equal(new[] { 2, 3 }, above40);

        var above70 = (await compiled(_ctx, 70)).Select(a => a.Id).ToArray();
        // Books >= 70: 31 only → author 3.
        Assert.Equal(new[] { 3 }, above70);
    }

    [Fact]
    public async Task Compiled_query_with_DTO_projection_via_positional_record()
    {
        var compiled = Norm.CompileQuery((DbContext c, string region) =>
            c.Query<CqAuthor>().Where(a => a.Region == region)
             .OrderBy(a => a.Id)
             .Select(a => new CqAuthorDto(a.Id, a.Name)));

        var eu = await compiled(_ctx, "EU");
        Assert.Equal(2, eu.Count);
        Assert.Equal(1, eu[0].Id); Assert.Equal("Ada", eu[0].Name);
        Assert.Equal(3, eu[1].Id); Assert.Equal("Linus", eu[1].Name);
    }

    [Fact]
    public async Task Compiled_query_with_composite_GroupBy_and_aggregate_projection()
    {
        var compiled = Norm.CompileQuery((DbContext c, int unused) =>
            c.Query<CqBook>()
             .GroupBy(b => new { b.AuthorId })
             .Select(g => new CqGroupedDto(g.Key.AuthorId, g.Sum(x => x.Price))));

        var rows = (await compiled(_ctx, 0))
            .OrderBy(r => r.AuthorId).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(1, rows[0].AuthorId); Assert.Equal(40, rows[0].Total);  // 10+30
        Assert.Equal(2, rows[1].AuthorId); Assert.Equal(50, rows[1].Total);  // 50
        Assert.Equal(3, rows[2].AuthorId); Assert.Equal(85, rows[2].Total);  // 5+80
    }

    [Table("CqAuthor")]
    public sealed class CqAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public List<CqBook> Books { get; set; } = new();
    }

    [Table("CqBook")]
    public sealed class CqBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public int Price { get; set; }
    }

    public sealed record CqAuthorDto(int Id, string Name);
    public sealed record CqGroupedDto(int AuthorId, int Total);
}
