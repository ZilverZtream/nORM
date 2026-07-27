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
/// GroupBy with a ValueTuple key — GroupBy(s => (s.A, s.B)) — projected whole (g.Key) is a resolvable
/// shape that must materialize, exactly like an anonymous composite key. The SQL emits one column per tuple
/// component (Key__Item1/Item2), but the nested-projection detection only recognized anonymous types, so a
/// ValueTuple key took the flat one-column path and threw.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ValueTupleGroupByKeyProjectionTests
{
    [Table("VtgRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int V { get; set; }
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE VtgRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, V INTEGER NOT NULL);
                INSERT INTO VtgRow (Id, A, B, V) VALUES
                    (1, 1, 10, 5), (2, 1, 10, 7), (3, 2, 20, 9), (4, 2, 20, 1), (5, 2, 30, 4);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task GroupBy_valuetuple_key_projected_whole_materializes()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Row>()
            .GroupBy(r => new ValueTuple<int, int>(r.A, r.B))
            .Select(g => new { g.Key, C = g.Count(), Sum = g.Sum(x => x.V) })
            .ToListAsync())
            .OrderBy(r => r.Key.Item1).ThenBy(r => r.Key.Item2)
            .ToList();

        // Groups: (1,10)->{5,7} count2 sum12; (2,20)->{9,1} count2 sum10; (2,30)->{4} count1 sum4.
        Assert.Equal(3, rows.Count);
        Assert.Equal((1, 10), rows[0].Key); Assert.Equal(2, rows[0].C); Assert.Equal(12, rows[0].Sum);
        Assert.Equal((2, 20), rows[1].Key); Assert.Equal(2, rows[1].C); Assert.Equal(10, rows[1].Sum);
        Assert.Equal((2, 30), rows[2].Key); Assert.Equal(1, rows[2].C); Assert.Equal(4, rows[2].Sum);
    }
}
