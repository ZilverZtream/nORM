using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SQL set operations match columns POSITIONALLY, so two Union arms projecting the same DTO with member
/// initializers in DIFFERENT member order must still align by member — otherwise the right arm's values
/// land under the left arm's aliases (a silent field swap). Each arm's projected column order must be
/// normalized to a canonical (declaration) order.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpMemberInitColumnOrderTests
{
    [Table("SomcRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string City { get; set; } = "";
    }

    public class Dto
    {
        public string Name { get; set; } = "";
        public string City { get; set; } = "";
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SomcRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, City TEXT NOT NULL);
                INSERT INTO SomcRow (Id, Name, City) VALUES (1, 'Alice', 'NYC'), (2, 'Bob', 'LA');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Union_arms_with_different_member_init_order_align_by_member()
    {
        await using var ctx = CreateDb();

        var arm1 = ctx.Query<Row>().Where(r => r.Id == 1)
            .Select(r => new Dto { Name = r.Name, City = r.City });     // arm 1: Name, City
        var arm2 = ctx.Query<Row>().Where(r => r.Id == 2)
            .Select(r => new Dto { City = r.City, Name = r.Name });     // arm 2: City, Name (swapped order)

        var rows = (await arm1.Union(arm2).ToListAsync())
            .OrderBy(d => d.Name)
            .ToList();

        // Both rows must keep Name/City paired correctly regardless of the per-arm binding order.
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal("NYC", rows[0].City);
        Assert.Equal("Bob", rows[1].Name); Assert.Equal("LA", rows[1].City);   // BUG: right arm swapped -> Name="LA"
    }
}
