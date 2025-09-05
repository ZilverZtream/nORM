using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    [Owned]
    public class Address
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
    }

    public class User
    {
        public int Id { get; set; }
        public Address Address { get; set; } = new();
    }

    public class OwnedTypesTests
    {
        [Fact]
        public void Owned_type_materializes_and_tracks_changes()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE User(Id INTEGER, Address_Street TEXT, Address_City TEXT);" +
                                 "INSERT INTO User VALUES(1, '123 Main', 'Metropolis');";
                cmd.ExecuteNonQuery();
            }

            using var ctx = new DbContext(cn, new SqliteProvider());
            var user = ctx.Query<User>().ToList().Single();
            Assert.NotNull(user.Address);
            Assert.Equal("123 Main", user.Address.Street);
            Assert.Equal("Metropolis", user.Address.City);

            user.Address.City = "Gotham";
            var entry = ctx.ChangeTracker.Entries.Single();
            var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
            var detect = typeof(ChangeTracker).GetMethod("DetectChanges", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            detect!.Invoke(ctx.ChangeTracker, null);
            var state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Modified, state);
        }
    }
}
