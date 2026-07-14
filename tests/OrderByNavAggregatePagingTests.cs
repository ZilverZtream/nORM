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
/// Ordering parent rows by a correlated aggregate (<c>OrderBy(p => p.Children.Count())</c>) and
/// then paging over that order must apply Skip/Take to the aggregate-sorted sequence, not to some
/// intermediate row order. A subquery-in-ORDER-BY that the paging layer mishandles would silently
/// return the wrong page. Verified against the LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", "Fast")]
public class OrderByNavAggregatePagingTests
{
    [Table("ObnParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("ObnChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } }

    // Child counts: P1=3, P2=1, P3=3, P4=0, P5=2
    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ObnParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE ObnChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
                INSERT INTO ObnParent VALUES (1,'p1'),(2,'p2'),(3,'p3'),(4,'p4'),(5,'p5');
                INSERT INTO ObnChild VALUES
                    (1,1),(2,1),(3,1),      -- P1: 3
                    (4,2),                  -- P2: 1
                    (5,3),(6,3),(7,3),      -- P3: 3
                    (8,5),(9,5);            -- P5: 2  (P4: 0)
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Parent> Oracle()
    {
        var counts = new Dictionary<int, int> { [1] = 3, [2] = 1, [3] = 3, [4] = 0, [5] = 2 };
        return Enumerable.Range(1, 5).Select(id => new Parent
        {
            Id = id,
            Name = $"p{id}",
            Children = Enumerable.Range(0, counts[id]).Select(_ => new Child()).ToList(),
        });
    }

    [Fact]
    public async Task OrderBy_nav_count_then_page_matches_oracle()
    {
        await using var ctx = Make();
        // Sorted by (count asc, id asc): p4(0), p2(1), p5(2), p1(3), p3(3). Skip 1 Take 3 => p2,p5,p1.
        var got = (await ctx.Query<Parent>()
                .OrderBy(p => p.Children.Count()).ThenBy(p => p.Id)
                .Skip(1).Take(3)
                .Select(p => new { p.Id }).ToListAsync())
            .Select(x => x.Id).ToArray();

        var oracle = Oracle().AsQueryable()
            .OrderBy(p => p.Children.Count()).ThenBy(p => p.Id)
            .Skip(1).Take(3).Select(p => p.Id).ToArray();

        Assert.Equal(oracle, got);
        Assert.Equal(new[] { 2, 5, 1 }, got);
    }

    [Fact]
    public async Task OrderByDescending_nav_count_then_page_matches_oracle()
    {
        await using var ctx = Make();
        // Sorted by (count desc, id asc): p1(3), p3(3), p5(2), p2(1), p4(0). Skip 2 Take 2 => p5,p2.
        var got = (await ctx.Query<Parent>()
                .OrderByDescending(p => p.Children.Count()).ThenBy(p => p.Id)
                .Skip(2).Take(2)
                .Select(p => new { p.Id }).ToListAsync())
            .Select(x => x.Id).ToArray();

        var oracle = Oracle().AsQueryable()
            .OrderByDescending(p => p.Children.Count()).ThenBy(p => p.Id)
            .Skip(2).Take(2).Select(p => p.Id).ToArray();

        Assert.Equal(oracle, got);
        Assert.Equal(new[] { 5, 2 }, got);
    }
}
