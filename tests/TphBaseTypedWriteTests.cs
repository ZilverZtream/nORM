using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    /// <summary>
    /// Adding a derived TPH instance through a BASE-typed reference (`ctx.Add&lt;Base&gt;(derived)` — an
    /// EF-idiomatic pattern) must resolve the mapping from the entity's RUNTIME type so the discriminator is
    /// stamped and the subtype's own columns are written. The change-tracking verbs resolved the mapping from
    /// the compile-time `typeof(T)` (the base), whose ApplyDiscriminator is a no-op: in a single-subtype
    /// hierarchy this silently wrote discriminator = 0 and the row read back as the base type; in a
    /// multi-subtype hierarchy it threw. Reads already resolve from the runtime type.
    /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public class TphBaseTypedWriteTests
    {
        [DiscriminatorColumn(nameof(Kind))]
        [Table("SoloTph")]
        public class SoloBase { [Key] public int Id { get; set; } public int Kind { get; set; } public string Name { get; set; } = ""; }

        [DiscriminatorValue(1)]
        [Table("SoloTph")]
        public class SoloDerived : SoloBase { public int Extra { get; set; } }

        [Fact]
        public async Task Base_typed_add_of_single_subtype_stamps_discriminator_and_columns()
        {
            var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE SoloTph(Id INTEGER PRIMARY KEY, Kind INTEGER, Name TEXT, Extra INTEGER);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());

            SoloBase entity = new SoloDerived { Id = 1, Name = "x", Extra = 42 };
            ctx.Add(entity);                 // T inferred as SoloBase
            await ctx.SaveChangesAsync();

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "SELECT Kind, Extra FROM SoloTph WHERE Id = 1";
                using var rdr = cmd.ExecuteReader(); Assert.True(rdr.Read());
                Assert.Equal(1L, rdr.GetInt64(0));   // discriminator stamped
                Assert.Equal(42L, rdr.GetInt64(1));  // subtype column written
            }
            var reloaded = ((INormQueryable<SoloBase>)ctx.Query<SoloBase>()).AsNoTracking().Single(a => a.Id == 1);
            Assert.IsType<SoloDerived>(reloaded);
        }

        [Fact]
        public async Task Base_typed_add_of_multi_subtype_stamps_correct_discriminator()
        {
            var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE Vehicle(Id INTEGER PRIMARY KEY, Kind INTEGER, Doors INTEGER, Axles INTEGER);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());

            Vehicle a = new Car { Id = 1, Doors = 4 };
            Vehicle b = new Truck { Id = 2, Axles = 6 };
            ctx.Add(a);
            ctx.Add(b);
            await ctx.SaveChangesAsync();

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "SELECT Kind, Doors FROM Vehicle WHERE Id = 1";
                using var rdr = cmd.ExecuteReader(); Assert.True(rdr.Read());
                Assert.Equal(1L, rdr.GetInt64(0));   // Car discriminator
                Assert.Equal(4L, rdr.GetInt64(1));
            }
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "SELECT Kind, Axles FROM Vehicle WHERE Id = 2";
                using var rdr = cmd.ExecuteReader(); Assert.True(rdr.Read());
                Assert.Equal(2L, rdr.GetInt64(0));   // Truck discriminator
                Assert.Equal(6L, rdr.GetInt64(1));
            }
        }
    }
}
