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
/// Eager loading and navigation flattening over a windowed-and-filtered
/// principal chain must key on exactly the windowed principals — a flat
/// translation would load or flatten children for rows outside the window.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class WindowedChainNavigationTests
{
    [Table("WcnParent")]
    private class WcnParent
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public List<WcnChild> Children { get; set; } = new();
    }

    [Table("WcnChild")]
    private class WcnChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<WcnToy> Toys { get; set; } = new();
    }

    [Table("WcnToy")]
    private class WcnToy
    {
        [Key] public int Id { get; set; }
        public int ChildId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WcnParent (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
                CREATE TABLE WcnChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Name TEXT NOT NULL);
                INSERT INTO WcnParent VALUES (1,50),(2,40),(3,30),(4,5),(5,1);
                INSERT INTO WcnChild VALUES
                    (10,1,'c1a'),(11,2,'c2a'),(12,2,'c2b'),(13,3,'c3a'),
                    (14,4,'c4a'),(15,5,'c5a');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<WcnParent>().HasKey(p => p.Id)
                    .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<WcnChild>().HasKey(c => c.Id)
                    .HasMany(c => c.Toys).WithOne().HasForeignKey(t => t.ChildId, c => c.Id);
                mb.Entity<WcnToy>().HasKey(t => t.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Include_over_windowed_filter_chain_loads_children_for_windowed_principals_only()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Window: OrderBy(Id).Take(3) → parents 1,2,3; Where(V < 45) keeps 2,3.
        // Parents 4 and 5 match the predicate but sit outside the window.
        var parents = await ((INormQueryable<WcnParent>)ctx.Query<WcnParent>()
            .OrderBy(p => p.Id)
            .Take(3)
            .Where(p => p.V < 45))
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Equal(2, parents.Count);
        Assert.Equal(2, parents.Single(p => p.Id == 2).Children.Count);
        Assert.Single(parents.Single(p => p.Id == 3).Children);
    }

    [Fact]
    public async Task Then_include_after_windowed_filter_chain_loads_the_second_level()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WcnToy (Id INTEGER PRIMARY KEY, ChildId INTEGER NOT NULL, Label TEXT NOT NULL);
                INSERT INTO WcnToy VALUES (100,11,'t-c2a-1'),(101,11,'t-c2a-2'),(102,13,'t-c3a-1'),(103,14,'outside');
                """;
            cmd.ExecuteNonQuery();
        }

        var parents = await ((INormQueryable<WcnParent>)ctx.Query<WcnParent>()
            .OrderBy(p => p.Id)
            .Take(3)
            .Where(p => p.V < 45))
            .Include(p => p.Children)
            .ThenInclude(c => c.Toys)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Equal(2, parents.Count);
        var childWithToys = parents.Single(p => p.Id == 2).Children.Single(c => c.Id == 11);
        Assert.Equal(2, childWithToys.Toys.Count);
        Assert.Single(parents.Single(p => p.Id == 3).Children.Single(c => c.Id == 13).Toys);
    }

    [Fact]
    public async Task Select_many_over_windowed_filter_chain_flattens_only_windowed_principals()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var names = (await ctx.Query<WcnParent>()
            .OrderBy(p => p.Id)
            .Take(3)
            .Where(p => p.V < 45)
            .SelectMany(p => p.Children)
            .ToListAsync())
            .Select(c => c.Name)
            .OrderBy(n => n)
            .ToArray();

        Assert.Equal(new[] { "c2a", "c2b", "c3a" }, names);
    }
}
