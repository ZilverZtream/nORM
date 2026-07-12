using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[DiscriminatorColumn(nameof(Kind))]
[Table("TphIdent")]
public class TphIdentBase
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
    public int Kind { get; set; }
}

[DiscriminatorValue(1)]
[Table("TphIdent")]
public class TphIdentCar : TphIdentBase { public int Doors { get; set; } }

/// <summary>
/// After the TPH insert fix, updating and deleting a derived entity must also work. The key
/// data-loss risk: a Car materialized from a base-typed query must track under the Car mapping
/// so its subtype column (Doors) participates in UPDATE — otherwise a change to a subtype
/// property is silently dropped. Reuses the TphVehicle/TphCar/TphTruck hierarchy.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TphUpdateDeleteDerivedTests
{
    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TphVehicle(Id INTEGER PRIMARY KEY, Kind INTEGER, Doors INTEGER, Axles INTEGER);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Updating_subtype_property_of_derived_entity_persists()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new TphCar { Id = 1, Doors = 4 });
        await ctx.SaveChangesAsync();

        // Load through the BASE-typed query (materializes as Car), change a SUBTYPE property.
        var car = (TphCar)ctx.Query<TphVehicle>().Single(v => v.Id == 1);
        car.Doors = 8;
        await ctx.SaveChangesAsync();

        var reloaded = (TphCar)((INormQueryable<TphVehicle>)ctx.Query<TphVehicle>())
            .AsNoTracking().Single(v => v.Id == 1);
        Assert.Equal(8, reloaded.Doors); // subtype change must survive, not be dropped
    }

    [Fact]
    public async Task Deleting_derived_entity_removes_the_row()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new TphCar { Id = 1, Doors = 4 });
        ctx.Add(new TphTruck { Id = 2, Axles = 6 });
        await ctx.SaveChangesAsync();

        var car = (TphCar)ctx.Query<TphVehicle>().Single(v => v.Id == 1);
        ctx.Remove(car);
        await ctx.SaveChangesAsync();

        var remaining = ((INormQueryable<TphVehicle>)ctx.Query<TphVehicle>())
            .AsNoTracking().OrderBy(v => v.Id).ToList();
        var truck = Assert.Single(remaining);
        Assert.IsType<TphTruck>(truck);
        Assert.Equal(2, truck.Id);
    }

    [Fact]
    public async Task Inserting_derived_entity_with_identity_key_hydrates_key_and_stamps_discriminator()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TphIdent(Id INTEGER PRIMARY KEY AUTOINCREMENT, Kind INTEGER, Doors INTEGER);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var car = new TphIdentCar { Doors = 4 }; // Id left unset — DB assigns it
        ctx.Add(car);
        await ctx.SaveChangesAsync();

        Assert.True(car.Id > 0, "DB-generated key should be hydrated back onto the derived entity");

        var reloaded = (TphIdentCar)((INormQueryable<TphIdentBase>)ctx.Query<TphIdentBase>())
            .AsNoTracking().Single(v => v.Id == car.Id);
        Assert.Equal(4, reloaded.Doors);
    }
}
