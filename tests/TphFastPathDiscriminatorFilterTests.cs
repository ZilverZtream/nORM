using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The read fast path (<see cref="nORM.Query.FastPathQueryExecutor"/>) must apply the TPH discriminator
/// filter when the queried entity is a subtype, exactly as the full pipeline does. If it does not, a
/// subtype query silently returns and counts sibling subtypes sharing the base table — the worst kind of
/// silent-wrong (wrong rows, wrong counts, and rows of one subtype materialized as another). These tests
/// put ONLY a <see cref="TphCar"/> in the table and assert every fast-path shape of a <see cref="TphTruck"/>
/// query comes back empty / zero. (Types reused from TphInsertDiscriminatorRoundTripTests.)
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TphFastPathDiscriminatorFilterTests
{
    private static SqliteConnection NewDbWithOneCar()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TphVehicle(Id INTEGER PRIMARY KEY, Kind INTEGER, Doors INTEGER, Axles INTEGER); INSERT INTO TphVehicle(Id, Kind, Doors) VALUES (1, 1, 4);";
            cmd.ExecuteNonQuery();
        }
        return cn;
    }

    [Fact]
    public async Task Subtype_count_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Only a Car exists; a Truck count must be 0. The bug counts the Car (COUNT(*) over the base table).
        var truckCount = await ctx.Query<TphTruck>().CountAsync();
        Assert.Equal(0, truckCount);
    }

    [Fact]
    public void Subtype_sync_count_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var truckCount = ctx.Query<TphTruck>().Count();
        Assert.Equal(0, truckCount);
    }

    [Fact]
    public void Subtype_count_with_predicate_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Id 1 is a Car; counting Trucks with that Id must be 0.
        var truckCount = ctx.Query<TphTruck>().Count(t => t.Id == 1);
        Assert.Equal(0, truckCount);
    }

    [Fact]
    public void Subtype_where_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Id 1 is a Car, not a Truck. A Truck query filtered on that Id must be empty.
        var trucks = ctx.Query<TphTruck>().Where(t => t.Id == 1).ToList();
        Assert.Empty(trucks);
    }

    [Fact]
    public void Subtype_take_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var trucks = ctx.Query<TphTruck>().Take(10).ToList();
        Assert.Empty(trucks);
    }

    [Fact]
    public void Subtype_ordered_paging_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var trucks = ((INormQueryable<TphTruck>)ctx.Query<TphTruck>())
            .OrderBy(t => t.Id)
            .Skip(0)
            .Take(5)
            .ToList();
        Assert.Empty(trucks);
    }

    [Fact]
    public void Subtype_firstordefault_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Only a Car exists; FirstOrDefault of a Truck must be null (not the Car row).
        var truck = ctx.Query<TphTruck>().FirstOrDefault();
        Assert.Null(truck);
    }

    [Fact]
    public void Subtype_firstordefault_with_predicate_excludes_sibling_subtype_rows()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Id 1 is a Car; a Truck matching that Id does not exist.
        var truck = ctx.Query<TphTruck>().FirstOrDefault(t => t.Id == 1);
        Assert.Null(truck);
    }

    [Fact]
    public void Base_type_query_still_returns_all_rows_via_fast_path()
    {
        using var cn = NewDbWithOneCar();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // A base-type query has no discriminator filter and must keep working (and materialize the subtype).
        var all = ctx.Query<TphVehicle>().ToList();
        Assert.Single(all);
        Assert.IsType<TphCar>(all[0]);
    }
}
