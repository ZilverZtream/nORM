using System;
using System.Collections.Generic;
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
/// A GroupBy HAVING (Where after GroupBy) whose aggregate differs from the projected aggregates,
/// and ordering by yet another aggregate, must produce the same groups/values as LINQ-to-Objects
/// — guarding against HAVING-vs-SELECT aggregate alias mixups.
/// </summary>
[Trait("Category", "Fast")]
public class GroupByHavingMismatchedAggregateTests
{
    [Table("GhmRow")]
    public class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int K { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly (int K, int A, int B)[] Rows =
    {
        (1, 10, 100), (1, 20, 200), (1, 5, 50),
        (2, 3, 30), (2, 4, 40),
        (3, 50, 1), (3, 60, 2),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GhmRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, K INTEGER NOT NULL, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (k, a, b) in Rows)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO GhmRow (K, A, B) VALUES ({k}, {a}, {b})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() =>
        Rows.Select((r, i) => new Row { Id = i + 1, K = r.K, A = r.A, B = r.B });

    [Fact]
    public async Task Having_on_sum_projecting_a_different_aggregate()
    {
        await using var ctx = Make();
        // HAVING Sum(A) > 30, project { Key, Avg(B), Max(A), Count }. HAVING aggregate (Sum A)
        // differs from every projected aggregate — a common source of alias/HAVING mixups.
        var got = (await ctx.Query<Row>()
            .GroupBy(r => r.K)
            .Where(g => g.Sum(x => x.A) > 30)
            .Select(g => new { Key = g.Key, AvgB = g.Average(x => x.B), MaxA = g.Max(x => x.A), Cnt = g.Count() })
            .ToListAsync())
            .OrderBy(x => x.Key).ToList();

        var oracle = Oracle()
            .GroupBy(r => r.K)
            .Where(g => g.Sum(x => x.A) > 30)
            .Select(g => new { Key = g.Key, AvgB = g.Average(x => x.B), MaxA = g.Max(x => x.A), Cnt = g.Count() })
            .OrderBy(x => x.Key).ToList();

        Assert.Equal(oracle.Count, got.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Key, got[i].Key);
            Assert.Equal(oracle[i].AvgB, got[i].AvgB, 5);
            Assert.Equal(oracle[i].MaxA, got[i].MaxA);
            Assert.Equal(oracle[i].Cnt, got[i].Cnt);
        }
    }

    [Fact]
    public async Task Having_with_two_conditions_and_ordered_by_aggregate()
    {
        await using var ctx = Make();
        // HAVING Count >= 2 AND Max(A) > 15, ordered by Sum(A) desc, projecting Sum(A).
        var got = (await ctx.Query<Row>()
            .GroupBy(r => r.K)
            .Where(g => g.Count() >= 2 && g.Max(x => x.A) > 15)
            .OrderByDescending(g => g.Sum(x => x.A))
            .Select(g => new { Key = g.Key, SumA = g.Sum(x => x.A) })
            .ToListAsync());

        var oracle = Oracle()
            .GroupBy(r => r.K)
            .Where(g => g.Count() >= 2 && g.Max(x => x.A) > 15)
            .OrderByDescending(g => g.Sum(x => x.A))
            .Select(g => new { Key = g.Key, SumA = g.Sum(x => x.A) })
            .ToList();

        Assert.Equal(oracle.Select(x => x.Key).ToArray(), got.Select(x => x.Key).ToArray());
        Assert.Equal(oracle.Select(x => x.SumA).ToArray(), got.Select(x => x.SumA).ToArray());
    }
}
