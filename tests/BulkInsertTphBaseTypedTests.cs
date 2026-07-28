using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    [DiscriminatorColumn(nameof(Kind))]
    public class BitAnimal { [Key] public int Id { get; set; } public int Kind { get; set; } }
    [DiscriminatorValue(1)] public class BitDog : BitAnimal { public int Bones { get; set; } }
    [DiscriminatorValue(2)] public class BitCat : BitAnimal { public int Whiskers { get; set; } }

    /// <summary>
    /// BulkInsert of derived TPH entities through a BASE-typed batch (the EF-idiomatic
    /// List&lt;Animal&gt; { new Dog(), new Cat() } pattern) must stamp each row's discriminator from its RUNTIME
    /// type. The bulk path resolved the mapping from the compile-time typeof(T)=base, whose ApplyDiscriminator
    /// is a no-op, so every row got discriminator=0 and read back as the base type — subtype identity lost.
    /// The tracked (ctx.Add) path already resolves per-entity runtime type.
    /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public class BulkInsertTphBaseTypedTests
    {
        private static SqliteConnection NewDb()
        {
            var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "CREATE TABLE BitAnimal(Id INTEGER PRIMARY KEY, Kind INTEGER, Bones INTEGER, Whiskers INTEGER);";
            cmd.ExecuteNonQuery();
            return cn;
        }

        [Fact]
        public async Task Bulk_insert_base_typed_mixed_subtypes_stamps_per_row_discriminator()
        {
            using var cn = NewDb();
            using var ctx = new DbContext(cn, new SqliteProvider());
            var animals = new List<BitAnimal>
            {
                new BitDog { Id = 1, Bones = 3 },
                new BitCat { Id = 2, Whiskers = 6 },
            };
            await ctx.BulkInsertAsync(animals);

            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT Kind, Bones, Whiskers FROM BitAnimal ORDER BY Id";
            using var rdr = cmd.ExecuteReader();
            Assert.True(rdr.Read());
            Assert.Equal(1L, rdr.GetInt64(0));   // Dog discriminator
            Assert.Equal(3L, rdr.GetInt64(1));   // Bones
            Assert.True(rdr.Read());
            Assert.Equal(2L, rdr.GetInt64(0));   // Cat discriminator
            Assert.Equal(6L, rdr.GetInt64(2));   // Whiskers
        }
    }
}
