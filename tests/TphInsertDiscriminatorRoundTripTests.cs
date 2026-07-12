using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Inserting a derived TPH entity must persist the discriminator value that identifies its
/// subtype, or the row reads back as the base type with its subtype data stranded — a silent
/// data-integrity loss. Probes the write side of TPH, which the read-only materialization
/// tests do not exercise.
/// </summary>
[DiscriminatorColumn(nameof(Kind))]
[Xunit.Trait("Category", "Fast")]
public class TphVehicle
{
    public int Id { get; set; }
    public int Kind { get; set; }
}

[DiscriminatorValue(1)]
[Xunit.Trait("Category", "Fast")]
public class TphCar : TphVehicle
{
    public int Doors { get; set; }
}

[DiscriminatorValue(2)]
[Xunit.Trait("Category", "Fast")]
public class TphTruck : TphVehicle
{
    public int Axles { get; set; }
}

[Xunit.Trait("Category", "Fast")]
public class TphInsertDiscriminatorRoundTripTests
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
    public async Task Inserting_derived_entity_persists_discriminator_and_round_trips_as_subtype()
    {
        // One connection/context for the whole test: closing an in-memory SQLite connection
        // destroys the database, so a second context cannot share it after the first disposes.
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new TphCar { Id = 1, Doors = 4 });
        ctx.Add(new TphTruck { Id = 2, Axles = 6 });
        await ctx.SaveChangesAsync();

        // Read the raw discriminator the DB actually stored — proves it was stamped on insert.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Kind FROM TphVehicle ORDER BY Id";
            using var rdr = cmd.ExecuteReader();
            rdr.Read(); Assert.Equal(1L, rdr.GetInt64(0)); // Car
            rdr.Read(); Assert.Equal(2L, rdr.GetInt64(0)); // Truck
        }

        // Round-trip through the materializer with AsNoTracking so rows are rebuilt from the
        // database (not returned from the identity map): subtypes and their data survive.
        var vehicles = ((INormQueryable<TphVehicle>)ctx.Query<TphVehicle>())
            .AsNoTracking()
            .OrderBy(v => v.Id)
            .ToList();
        Assert.Equal(2, vehicles.Count);
        var car = Assert.IsType<TphCar>(vehicles[0]);
        Assert.Equal(4, car.Doors);
        var truck = Assert.IsType<TphTruck>(vehicles[1]);
        Assert.Equal(6, truck.Axles);
    }

    [Fact]
    public async Task Bulk_inserting_derived_entities_stamps_discriminator()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        await ctx.BulkInsertAsync(new[]
        {
            new TphCar { Id = 1, Doors = 2 },
            new TphCar { Id = 2, Doors = 4 },
        });

        var cars = ((INormQueryable<TphVehicle>)ctx.Query<TphVehicle>())
            .AsNoTracking()
            .OrderBy(v => v.Id)
            .ToList();
        Assert.Equal(2, cars.Count);
        Assert.Equal(2, Assert.IsType<TphCar>(cars[0]).Doors);
        Assert.Equal(4, Assert.IsType<TphCar>(cars[1]).Doors);
    }
}
