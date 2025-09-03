using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Mapping;
using Xunit;

namespace nORM.Tests
{
    public record Person(int Id, string Name);

    [DiscriminatorColumn(nameof(Type))]
    public class Animal
    {
        public int Id { get; set; }
        public string Type { get; set; } = string.Empty;
    }

    [DiscriminatorValue("Cat")]
    public class Cat : Animal
    {
        public int Lives { get; set; }
    }

    [DiscriminatorValue("Dog")]
    public class Dog : Animal
    {
        public bool GoodBoy { get; set; }
    }

    public class AdvancedMappingTests
    {
        [Fact]
        public void Record_type_materializes_correctly()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "CREATE TABLE Person(Id INTEGER, Name TEXT); INSERT INTO Person VALUES(1,'Alice');";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());
            var people = ctx.Query<Person>().ToList();
            Assert.Single(people);
            Assert.Equal(new Person(1, "Alice"), people[0]);
        }

        [Fact]
        public void Tph_mapping_creates_derived_instances()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"CREATE TABLE Animal(Id INTEGER, Type TEXT, Lives INTEGER, GoodBoy INTEGER);
                                    INSERT INTO Animal VALUES(1,'Cat',9,NULL);
                                    INSERT INTO Animal VALUES(2,'Dog',NULL,1);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());
            var animals = ctx.Query<Animal>().OrderBy(a => a.Id).ToList();
            Assert.Equal(2, animals.Count);
            Assert.IsType<Cat>(animals[0]);
            Assert.Equal(9, ((Cat)animals[0]).Lives);
            Assert.IsType<Dog>(animals[1]);
            Assert.True(((Dog)animals[1]).GoodBoy);
        }

        [Fact]
        public void Querying_derived_type_filters_by_discriminator()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"CREATE TABLE Animal(Id INTEGER, Type TEXT, Lives INTEGER, GoodBoy INTEGER);
                                    INSERT INTO Animal VALUES(1,'Cat',9,NULL);
                                    INSERT INTO Animal VALUES(2,'Dog',NULL,1);";
                cmd.ExecuteNonQuery();
            }
            using var ctx = new DbContext(cn, new SqliteProvider());
            var dogs = ctx.Query<Dog>().OrderBy(d => d.Id).ToList();
            Assert.Single(dogs);
            Assert.True(dogs[0].GoodBoy);
        }
    }
}
