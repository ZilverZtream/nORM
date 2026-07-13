using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// TPH hierarchies compose with navigations and projections: OfType&lt;Derived&gt;()
/// keeps its discriminator predicate under a downstream Select (previously the
/// predicate was only injected when the derived type was the query's element type,
/// so a projection either threw or would have leaked other subtypes), navigation
/// predicates work from base queries and OfType-filtered queries, Include
/// materializes the correct derived types, and the streaming GroupJoin path
/// segments per outer row like the buffered paths.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TphNavigationAndStreamingCompositionTests
{
    [Table("TphNav_Owner")]
    private class Owner
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [DiscriminatorColumn(nameof(Kind))]
    [Table("TphNav_Pet")]
    private class Pet
    {
        [Key] public int Id { get; set; }
        public string Kind { get; set; } = "";
        public string PetName { get; set; } = "";
        public int? OwnerId { get; set; }
        [ForeignKey(nameof(OwnerId))] public Owner? Owner { get; set; }
    }

    [DiscriminatorValue("dog")]
    private class Dog : Pet
    {
    }

    [DiscriminatorValue("cat")]
    private class Cat : Pet
    {
    }

    [Table("TphNav_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("TphNav_Badge")]
    private class Badge
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TphNav_Owner (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE TphNav_Pet (Id INTEGER PRIMARY KEY, Kind TEXT NOT NULL, PetName TEXT NOT NULL, OwnerId INTEGER NULL);
                CREATE TABLE TphNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE TphNav_Badge (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                INSERT INTO TphNav_Owner VALUES (1, 'ann'), (2, 'bob');
                INSERT INTO TphNav_Pet VALUES (1, 'dog', 'rex', 1), (2, 'cat', 'tom', 1), (3, 'dog', 'fido', NULL);
                INSERT INTO TphNav_Emp VALUES (1, 'ann'), (2, 'bob'), (3, 'ann');
                INSERT INTO TphNav_Badge VALUES (1, 'ann'), (2, 'ann');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Pet>().HasKey(p => p.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Badge>().HasKey(b => b.Id);
            }
        });
    }

    [Fact]
    public void tph_nav_predicate_from_base_query()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var names = ctx.Query<Pet>().Where(p => p.Owner!.Name == "ann")
            .Select(p => p.PetName).ToList().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "rex", "tom" }, names);
    }

    [Fact]
    public void tph_oftype_with_nav_predicate()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var dogs = ctx.Query<Pet>().OfType<Dog>().Where(d => d.Owner!.Name == "ann")
            .Select(d => d.PetName).ToList();
        Assert.Equal(new[] { "rex" }, dogs);
    }

    [Fact]
    public void tph_oftype_where_plain_tolist()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var dogs = ctx.Query<Pet>().OfType<Dog>().Where(d => d.PetName != "zzz").ToList();
        Assert.Equal(2, dogs.Count);
    }

    [Fact]
    public void tph_oftype_where_plain_select()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var names = ctx.Query<Pet>().OfType<Dog>().Where(d => d.PetName != "zzz")
            .Select(d => d.PetName).ToList().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "fido", "rex" }, names);
    }

    [Fact]
    public void tph_oftype_select_excludes_other_subtypes()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Cat 'tom' must never leak into a Dog projection.
        var names = ctx.Query<Pet>().OfType<Dog>().Select(d => d.PetName)
            .ToList().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "fido", "rex" }, names);
    }

    [Fact]
    public void tph_oftype_where_nav_tolist()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var dogs = ctx.Query<Pet>().OfType<Dog>().Where(d => d.Owner!.Name == "ann").ToList();
        Assert.Single(dogs);
    }

    [Fact]
    public void tph_include_materializes_derived_types()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var pets = ((INormQueryable<Pet>)ctx.Query<Pet>()).Include(p => p.Owner!)
            .ToList().OrderBy(p => p.Id).ToList();
        Assert.IsType<Dog>(pets[0]);
        Assert.IsType<Cat>(pets[1]);
        Assert.Equal("ann", pets[0].Owner?.Name);
        Assert.Null(pets[2].Owner);
    }

    private class GjRow
    {
        public int Id { get; set; }
        public int Count { get; set; }
    }

    [Fact]
    public async Task streaming_groupjoin_duplicate_outer_keys()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Streaming path (AsAsyncEnumerable) must segment per outer row too.
        var q = (INormQueryable<GjRow>)ctx.Query<Emp>()
            .GroupJoin(ctx.Query<Badge>(), e => e.Name, b => b.Label,
                (e, bs) => new GjRow { Id = e.Id, Count = bs.Count() });
        var results = new List<GjRow>();
        await foreach (var r in q.AsAsyncEnumerable())
            results.Add(r);
        Assert.Equal(3, results.Count);
        Assert.Equal(2, results.Single(r => r.Id == 1).Count);
        Assert.Equal(0, results.Single(r => r.Id == 2).Count);
        Assert.Equal(2, results.Single(r => r.Id == 3).Count);
    }
}
