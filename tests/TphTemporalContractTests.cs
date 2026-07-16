using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the TPH x temporal interaction, probed differentially: a discriminated hierarchy
/// reconstructs correctly under AsOf — base-type queries return era values, the
/// discriminator materializes DERIVED instances from history rows, and OfType&lt;TDerived&gt;
/// composes with AsOf (the discriminator predicate applies inside the history window).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TphTemporalContractTests
{
    [DiscriminatorColumn(nameof(Kind))]
    [Table("TphT_Pet")]
    public class Pet
    {
        [Key] public int Id { get; set; }
        public string Kind { get; set; } = "";
        public string PetName { get; set; } = "";
    }

    [DiscriminatorValue("dog")]
    public class Dog : Pet { }

    [DiscriminatorValue("cat")]
    public class Cat : Pet { }

    [Fact]
    public async Task Tph_hierarchy_under_as_of_probe()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TphT_Pet (Id INTEGER PRIMARY KEY, Kind TEXT NOT NULL, PetName TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Pet>() };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        var dog = new Dog { Id = 1, Kind = "dog", PetName = "rex" };
        var cat = new Cat { Id = 2, Kind = "cat", PetName = "tom" };
        ctx.Add(dog);
        ctx.Add(cat);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // rex + tom
        await Task.Delay(60);

        dog.PetName = "rex2";              // derived-type update after t1
        await ctx.SaveChangesAsync();

        // Base-type AsOf: both rows reconstruct at era values.
        var basePets = await ctx.Query<Pet>().AsOf(t1).ToListAsync();
        Assert.Equal(new[] { "dog:rex", "cat:tom" },
            basePets.OrderBy(p => p.Id).Select(p => $"{p.Kind}:{p.PetName}").ToArray());

        // The discriminator materializes DERIVED instances from history rows too.
        Assert.Equal(new[] { "Dog", "Cat" },
            basePets.OrderBy(p => p.Id).Select(p => p.GetType().Name).ToArray());

        // Derived-type filter composes with AsOf: OfType<Dog> reconstructs rex at t1.
        var dogs = await ctx.Query<Pet>().AsOf(t1).OfType<Dog>().ToListAsync();
        Assert.Equal(new[] { "rex" }, dogs.Select(d => d.PetName).ToArray());

        // Without AsOf the live values come back.
        var liveDogs = await ctx.Query<Pet>().OfType<Dog>().ToListAsync();
        Assert.Equal(new[] { "rex2" }, liveDogs.Select(d => d.PetName).ToArray());
    }
}
