using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[DiscriminatorColumn(nameof(Species))]
[Table("DiscGuardAnimal")]
public class DiscGuardAnimal
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
    public int Species { get; set; } // TPH discriminator
    public string Name { get; set; } = "";
}

[DiscriminatorValue(1)]
[Table("DiscGuardAnimal")]
public class DiscGuardCat : DiscGuardAnimal { public int Whiskers { get; set; } }

[DiscriminatorValue(2)]
[Table("DiscGuardAnimal")]
public class DiscGuardDog : DiscGuardAnimal { public int Barks { get; set; } }

/// <summary>
/// Security regression (mass-assignment / subtype masquerade): the TPH discriminator identifies a row's
/// concrete subtype and is stamped only on INSERT. It must be immutable on UPDATE — writing a different
/// discriminator would relabel the row as a sibling subtype. Previously the discriminator sat in
/// UpdateColumns with no update guard, so a tracked update or ExecuteUpdate.SetProperty could change it
/// (EF Core rejects this). Now: excluded from UpdateColumns + fail-loud on both the tracked and set-based
/// write paths, while ordinary subtype-property updates still work and leave the discriminator intact.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class TphDiscriminatorImmutableOnUpdateTests
{
    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DiscGuardAnimal(Id INTEGER PRIMARY KEY AUTOINCREMENT, Species INTEGER, Name TEXT, Whiskers INTEGER, Barks INTEGER);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Changing_discriminator_on_tracked_entity_is_rejected()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DiscGuardCat { Name = "Felix", Whiskers = 12 });
        await ctx.SaveChangesAsync();

        var cat = (DiscGuardCat)ctx.Query<DiscGuardAnimal>().Single(a => a.Name == "Felix");
        cat.Species = 2; // attempt to masquerade as a Dog

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("Discriminator", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Setting_discriminator_via_ExecuteUpdate_is_rejected()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DiscGuardCat { Name = "Felix", Whiskers = 12 });
        await ctx.SaveChangesAsync();

        var ex = await Assert.ThrowsAsync<NormQueryException>(() =>
            NormAsyncExtensions.ExecuteUpdateAsync(
                ctx.Query<DiscGuardAnimal>().Where(a => a.Id == 1),
                s => s.SetProperty(p => p.Species, 2)));
        Assert.Contains("discriminator", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Ordinary_subtype_update_succeeds_and_leaves_discriminator_intact()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DiscGuardCat { Name = "Felix", Whiskers = 12 });
        await ctx.SaveChangesAsync();

        var cat = (DiscGuardCat)ctx.Query<DiscGuardAnimal>().Single(a => a.Name == "Felix");
        cat.Whiskers = 20; // subtype property, not the discriminator
        cat.Name = "Felix II";
        await ctx.SaveChangesAsync();

        using var cn2 = cn; // same in-memory db
        var reloaded = (DiscGuardCat)((INormQueryable<DiscGuardAnimal>)ctx.Query<DiscGuardAnimal>())
            .AsNoTracking().Single(a => a.Id == 1);
        Assert.IsType<DiscGuardCat>(reloaded); // still a Cat — discriminator preserved
        Assert.Equal(20, reloaded.Whiskers);
        Assert.Equal("Felix II", reloaded.Name);
    }
}
