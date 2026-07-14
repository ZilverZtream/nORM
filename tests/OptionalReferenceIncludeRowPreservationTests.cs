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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Eager-loading an OPTIONAL reference navigation (nullable FK) must LEFT-JOIN so parents with a
/// null FK survive — especially when combined with paging, where a dropped null-FK row would
/// silently shift the page window, and when the nav is filtered in WHERE.
/// </summary>
[Trait("Category", "Fast")]
public class OptionalReferenceIncludeRowPreservationTests
{
    [Table("OriCategory")]
    public class Category { [Key] public int Id { get; set; } public string Title { get; set; } = ""; }

    [Table("OriParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? CategoryId { get; set; }
        [ForeignKey(nameof(CategoryId))] public Category? Category { get; set; }
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OriCategory (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE OriParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, CategoryId INTEGER NULL);
                INSERT INTO OriCategory VALUES (1,'Books'),(2,'Toys');
                INSERT INTO OriParent VALUES (1,'has-cat',1),(2,'no-cat',NULL),(3,'has-cat2',2),(4,'no-cat2',NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Category>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasKey(p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Include_optional_reference_with_paging_keeps_null_fk_rows()
    {
        await using var ctx = Make();
        // Page [Skip 1, Take 2] over ordered ids => 2,3. Row 2 has a null FK: a null-dropping
        // INNER JOIN would page 3,4 (or 1,3) instead, silently shifting the window.
        var parents = (await ((INormQueryable<Parent>)ctx.Query<Parent>())
                .AsNoTracking().Include(p => p.Category!)
                .OrderBy(p => p.Id).Skip(1).Take(2).ToListAsync())
            .ToList();

        Assert.Equal(new[] { 2, 3 }, parents.Select(p => p.Id));
        Assert.Null(parents[0].Category);            // Id 2, no category
        Assert.Equal("Toys", parents[1].Category?.Title);
    }

    [Fact]
    public async Task Include_optional_reference_take_from_start_keeps_null_fk_row()
    {
        await using var ctx = Make();
        // Take the first 2: 1 (has cat), 2 (null cat). The null-FK row must occupy its slot.
        var parents = (await ((INormQueryable<Parent>)ctx.Query<Parent>())
                .AsNoTracking().Include(p => p.Category!)
                .OrderBy(p => p.Id).Take(2).ToListAsync())
            .ToList();

        Assert.Equal(new[] { 1, 2 }, parents.Select(p => p.Id));
        Assert.Equal("Books", parents[0].Category?.Title);
        Assert.Null(parents[1].Category);
    }

    [Fact]
    public async Task Filter_on_optional_nav_member_matches_oracle()
    {
        await using var ctx = Make();
        // Category title starting 'B' => only Id 1. Null-FK parents must neither match nor throw.
        var got = (await ctx.Query<Parent>()
                .Where(p => p.Category != null && p.Category.Title.StartsWith("B"))
                .Select(p => new { p.Id }).ToListAsync())
            .Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1 }, got);
    }
}
