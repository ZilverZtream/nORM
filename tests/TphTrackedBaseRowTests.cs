using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    /// <summary>
    /// A TRACKED base-typed query must not crash on a row that materializes as the base type — a concrete
    /// base row or a row whose discriminator matches no subtype. The base mapping merges every subtype's
    /// columns, whose getters hard-cast to that subtype; CaptureOriginalValues invoked them on a base
    /// instance and threw InvalidCastException. The untracked path already degrades gracefully (materializes
    /// as base), so the tracked path must too. Uses the shared Vehicle/Car/Truck hierarchy.
    /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public class TphTrackedBaseRowTests
    {
        [Fact]
        public void Tracked_base_query_with_unknown_discriminator_row_does_not_crash()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE Vehicle(Id INTEGER, Kind INTEGER, Doors INTEGER, Axles INTEGER);" +
                                  "INSERT INTO Vehicle VALUES(1,1,4,NULL);" +   // Car
                                  "INSERT INTO Vehicle VALUES(2,99,NULL,NULL);"; // unknown discriminator -> base
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());

            // Tracked (default) query over the base type. Must not throw on the unknown-discriminator row.
            var all = ctx.Query<Vehicle>().OrderBy(v => v.Id).ToList();
            Assert.Equal(2, all.Count);
            Assert.IsType<Car>(all[0]);         // Kind=1 -> Car
            Assert.IsType<Vehicle>(all[1]);     // Kind=99 -> base
            Assert.Equal(2, all[1].Id);
        }

        [Fact]
        public async Task Concrete_base_insert_does_not_crash_on_merged_subtype_columns()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE Vehicle(Id INTEGER PRIMARY KEY, Kind INTEGER, Doors INTEGER, Axles INTEGER);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());

            // Inserting a concrete base instance invokes the base mapping's merged Car/Truck getters; they must
            // yield NULL (not throw) for the sibling-subtype columns.
            ctx.Add(new Vehicle { Id = 5, Kind = 0 });
            await ctx.SaveChangesAsync();

            using var cmd2 = cn.CreateCommand();
            cmd2.CommandText = "SELECT Doors, Axles FROM Vehicle WHERE Id = 5";
            using var rdr = cmd2.ExecuteReader();
            Assert.True(rdr.Read());
            Assert.True(rdr.IsDBNull(0));   // no Car.Doors
            Assert.True(rdr.IsDBNull(1));   // no Truck.Axles
        }
    }
}
