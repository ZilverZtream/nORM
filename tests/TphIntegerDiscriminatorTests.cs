using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    // TPH hierarchy keyed by an INTEGER discriminator. SQLite returns Int64 for
    // INTEGER columns, but the derived-materializer dictionary is keyed by the
    // discriminator's declared type (int, boxed). A boxed (long)1 does not equal
    // a boxed (int)1, so the lookup missed and every derived row silently
    // materialized as the BASE type with its subtype properties lost. Failing-first
    // pin for the widening-coercion fix.
    [DiscriminatorColumn(nameof(Kind))]
    [Xunit.Trait("Category", "Fast")]
    public class Vehicle
    {
        public int Id { get; set; }
        public int Kind { get; set; }
    }

    [DiscriminatorValue(1)]
    [Xunit.Trait("Category", "Fast")]
    public class Car : Vehicle
    {
        public int Doors { get; set; }
    }

    [DiscriminatorValue(2)]
    [Xunit.Trait("Category", "Fast")]
    public class Truck : Vehicle
    {
        public int Axles { get; set; }
    }

    [Xunit.Trait("Category", "Fast")]
    public class TphIntegerDiscriminatorTests
    {
        [Fact]
        public void Integer_discriminator_materializes_derived_types()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"CREATE TABLE Vehicle(Id INTEGER, Kind INTEGER, Doors INTEGER, Axles INTEGER);
                                    INSERT INTO Vehicle VALUES(1,1,4,NULL);
                                    INSERT INTO Vehicle VALUES(2,2,NULL,6);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());
            var vehicles = ctx.Query<Vehicle>().OrderBy(v => v.Id).ToList();

            Assert.Equal(2, vehicles.Count);
            Assert.IsType<Car>(vehicles[0]);
            Assert.Equal(4, ((Car)vehicles[0]).Doors);
            Assert.IsType<Truck>(vehicles[1]);
            Assert.Equal(6, ((Truck)vehicles[1]).Axles);
        }
    }
}
