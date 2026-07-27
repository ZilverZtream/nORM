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
/// A terminal projection with a nested anonymous member — Select(e => new { e.Id, Info = new { e.Salary,
/// e.Name } }) — is a resolvable shape: the read side already materializes it. The plain-Select SQL emit
/// double-aliased the nested columns ("... AS Name AS Info"), producing invalid SQL; it must emit one flat
/// prefixed column per nested member, as the GroupBy projection path does.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TerminalNestedAnonProjectionTests
{
    [Table("TnaEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int Salary { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TnaEmp (Id INTEGER PRIMARY KEY, Salary INTEGER NOT NULL, Name TEXT NOT NULL);
                INSERT INTO TnaEmp (Id, Salary, Name) VALUES (1, 100, 'Alice'), (2, 200, 'Bob');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Emp>().HasKey(e => e.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Terminal_nested_anonymous_projection_materializes()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Emp>()
            .OrderBy(e => e.Id)
            .Select(e => new { e.Id, Info = new { e.Salary, e.Name } })
            .ToListAsync())
            .ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(100, rows[0].Info.Salary);
        Assert.Equal("Alice", rows[0].Info.Name);
        Assert.Equal(2, rows[1].Id);
        Assert.Equal(200, rows[1].Info.Salary);
        Assert.Equal("Bob", rows[1].Info.Name);
    }
}
