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
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A collection navigation to a TPH-derived type — <c>Owner.Dogs</c> where <c>Dog : Pet</c> share the Pet
/// table with a discriminator — must restrict every navigation emit (projection aggregate, split-query load,
/// Include, lazy load, WHERE-side aggregate) to rows of the derived type. Without the subtype discriminator a
/// Cat sharing the owner's FK silently counts and materializes as a Dog.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationToDerivedTypeTests
{
    [Table("NtdOwner")]
    private class Owner
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        // Uninitialized ICollection so nORM's lazy loader installs its proxy; Include / split-query still
        // assign a concrete list. One model exercises both the eager and lazy navigation emit paths.
        public ICollection<Dog> Dogs { get; set; } = null!;
    }

    [DiscriminatorColumn(nameof(Kind))]
    [Table("NtdPet")]
    private class Pet
    {
        [Key] public int Id { get; set; }
        public string Kind { get; set; } = "";
        public string PetName { get; set; } = "";
        public int Age { get; set; }
        public int? OwnerId { get; set; }
        [ForeignKey(nameof(OwnerId))] public Owner? Owner { get; set; }
    }

    [DiscriminatorValue("dog")]
    private class Dog : Pet { }

    [DiscriminatorValue("cat")]
    private class Cat : Pet { }

    // Owner 1 has 2 dogs (rex age 3, fido age 5) and 1 cat (tom); Owner 2 has 0 dogs and 1 cat (sy).
    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NtdOwner (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE NtdPet (Id INTEGER PRIMARY KEY, Kind TEXT NOT NULL, PetName TEXT NOT NULL, Age INTEGER NOT NULL, OwnerId INTEGER NULL);
                INSERT INTO NtdOwner VALUES (1, 'ann'), (2, 'bob');
                INSERT INTO NtdPet VALUES (1, 'dog', 'rex', 3, 1), (2, 'cat', 'tom', 9, 1), (3, 'dog', 'fido', 5, 1), (4, 'cat', 'sy', 2, 2);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Pet>().HasKey(p => p.Id);
            }
        });
    }

    [Fact]
    public void Projection_count_over_derived_collection_excludes_other_subtypes()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Owner>()
            .Select(o => new { o.Id, DogCount = o.Dogs.Count() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(2, rows.Single(r => r.Id == 1).DogCount);   // rex + fido, NOT tom
        Assert.Equal(0, rows.Single(r => r.Id == 2).DogCount);   // sy is a cat, not a dog
    }

    [Fact]
    public void Projection_sum_over_derived_collection_excludes_other_subtypes()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Owner>()
            .Select(o => new { o.Id, TotalAge = o.Dogs.Sum(d => d.Age) })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(8, rows.Single(r => r.Id == 1).TotalAge);   // rex 3 + fido 5, NOT tom's 9
        Assert.Equal(0, rows.Single(r => r.Id == 2).TotalAge);   // no dogs
    }

    [Fact]
    public void Where_side_any_over_derived_collection_excludes_other_subtypes()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ownersWithDogs = ctx.Query<Owner>().Where(o => o.Dogs.Any())
            .Select(o => o.Id).ToList().OrderBy(x => x).ToList();
        Assert.Equal(new[] { 1 }, ownersWithDogs);   // owner 2 has only a cat → excluded
    }

    [Fact]
    public void Split_query_collection_projection_loads_only_derived_type()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Owner>()
            .Select(o => new { o.Id, Dogs = o.Dogs.ToList() })
            .ToList().OrderBy(r => r.Id).ToList();
        var ann = rows.Single(r => r.Id == 1);
        Assert.Equal(new[] { "fido", "rex" }, ann.Dogs.Select(d => d.PetName).OrderBy(n => n));
        Assert.All(ann.Dogs, d => Assert.IsType<Dog>(d));
        Assert.Empty(rows.Single(r => r.Id == 2).Dogs);
    }

    [Fact]
    public void Include_collection_of_derived_type_loads_only_derived_type()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var owners = ((INormQueryable<Owner>)ctx.Query<Owner>()).Include(o => o.Dogs)
            .ToList().OrderBy(o => o.Id).ToList();
        Assert.Equal(new[] { "fido", "rex" }, owners[0].Dogs.Select(d => d.PetName).OrderBy(n => n));
        Assert.All(owners[0].Dogs, d => Assert.IsType<Dog>(d));
        Assert.Empty(owners[1].Dogs);
    }

    [Fact]
    public async Task Explicit_load_of_derived_collection_loads_only_derived_type()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ann = ctx.Query<Owner>().First(o => o.Id == 1);
        await ann.LoadAsync(o => o.Dogs);
        Assert.Equal(new[] { "fido", "rex" }, ann.Dogs.Select(d => d.PetName).OrderBy(n => n));
        Assert.All(ann.Dogs, d => Assert.IsType<Dog>(d));
    }

    [Fact]
    public void Lazy_load_of_derived_collection_loads_only_derived_type()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ann = ctx.Query<Owner>().First(o => o.Id == 1);
        // Accessing the uninitialized ICollection triggers nORM's lazy proxy → BatchedNavigationLoader.
        Assert.Equal(new[] { "fido", "rex" }, ann.Dogs.Select(d => d.PetName).OrderBy(n => n));
        Assert.All(ann.Dogs, d => Assert.IsType<Dog>(d));
    }
}
