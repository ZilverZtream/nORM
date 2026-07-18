using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    [Owned]
    [Xunit.Trait("Category", "Fast")]
    public class Address
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
    }

    [Xunit.Trait("Category", "Fast")]
    public class User
    {
        public int Id { get; set; }
        public Address Address { get; set; } = new();
    }

    [Xunit.Trait("Category", "Fast")]
    public class OwnedCityCollisionUser
    {
        public int Id { get; set; }
        public string City { get; set; } = string.Empty;
        public Address Address { get; set; } = new();
    }

    [Xunit.Trait("Category", "Fast")]
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
            var detect = typeof(ChangeTracker).GetMethod("DetectChangesDirtyOnly", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            detect!.Invoke(ctx.ChangeTracker, null);
            var state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Modified, state);
        }

        [Fact]
        public void Owned_type_leaf_property_can_collide_with_owner_property_name()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE OwnedCityCollisionUser(Id INTEGER, City TEXT, Address_Street TEXT, Address_City TEXT);" +
                                  "INSERT INTO OwnedCityCollisionUser VALUES(1, 'Root', '123 Main', 'Nested');" +
                                  "INSERT INTO OwnedCityCollisionUser VALUES(2, 'Nested', '456 Side', 'Other');";
                cmd.ExecuteNonQuery();
            }

            using var ctx = new DbContext(cn, new SqliteProvider());
            var mapping = ctx.GetMapping(typeof(OwnedCityCollisionUser));

            Assert.Equal("City", mapping.ColumnsByName["City"].PropName);
            Assert.Equal("Address_City", mapping.ColumnsByName["Address_City"].PropName);

            var byOwnedCity = ctx.Query<OwnedCityCollisionUser>()
                .Where(u => u.Address.City == "Nested")
                .ToList()
                .Single();
            Assert.Equal(1, byOwnedCity.Id);
            Assert.Equal("Root", byOwnedCity.City);
            Assert.Equal("Nested", byOwnedCity.Address.City);
            Assert.Equal(1, ctx.Query<OwnedCityCollisionUser>().Count(u => u.Address.City == "Nested"));

            var projection = ctx.Query<OwnedCityCollisionUser>()
                .Where(u => u.Id == 1)
                .Select(u => new { RootCity = u.City, AddressCity = u.Address.City })
                .ToList()
                .Single();
            Assert.Equal("Root", projection.RootCity);
            Assert.Equal("Nested", projection.AddressCity);
        }
    }
}
