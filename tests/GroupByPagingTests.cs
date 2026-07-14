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

[Trait("Category", "Fast")]
public class GroupByPagingTests
{
    [Table("GbpRow")]
    public class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int K { get; set; }
        public int Val { get; set; }
    }

    private static readonly (int K, int Val)[] Data =
    {
        (1, 10), (1, 20), (1, 5),
        (2, 3), (2, 4),
        (3, 50), (3, 60), (3, 1),
        (4, 7),
        (5, 100), (5, 200),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GbpRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, K INTEGER NOT NULL, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (k, v) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO GbpRow (K, Val) VALUES ({k}, {v})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() =>
        Data.Select((d, i) => new Row { Id = i + 1, K = d.K, Val = d.Val });

    [Fact]
    public async Task Paged_group_summaries_match_oracle()
    {
        await using var ctx = Make();
        // Group summaries ordered by Key, then page (skip 1 group, take 2 groups).
        var got = (await ctx.Query<Row>()
            .GroupBy(r => r.K)
            .Select(g => new { Key = g.Key, Sum = g.Sum(x => x.Val), Cnt = g.Count() })
            .OrderBy(x => x.Key)
            .Skip(1).Take(2)
            .ToListAsync());

        var oracle = Oracle()
            .GroupBy(r => r.K)
            .Select(g => new { Key = g.Key, Sum = g.Sum(x => x.Val), Cnt = g.Count() })
            .OrderBy(x => x.Key)
            .Skip(1).Take(2)
            .ToList();

        Assert.Equal(oracle.Count, got.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Key, got[i].Key);
            Assert.Equal(oracle[i].Sum, got[i].Sum);
            Assert.Equal(oracle[i].Cnt, got[i].Cnt);
        }
    }

    [Fact]
    public async Task Paged_groups_ordered_by_aggregate_match_oracle()
    {
        await using var ctx = Make();
        // Order groups by descending Sum, then take the top 3.
        var got = (await ctx.Query<Row>()
            .GroupBy(r => r.K)
            .Select(g => new { Key = g.Key, Sum = g.Sum(x => x.Val) })
            .OrderByDescending(x => x.Sum).ThenBy(x => x.Key)
            .Take(3)
            .ToListAsync());

        var oracle = Oracle()
            .GroupBy(r => r.K)
            .Select(g => new { Key = g.Key, Sum = g.Sum(x => x.Val) })
            .OrderByDescending(x => x.Sum).ThenBy(x => x.Key)
            .Take(3)
            .ToList();

        Assert.Equal(oracle.Select(x => x.Key).ToArray(), got.Select(x => x.Key).ToArray());
        Assert.Equal(oracle.Select(x => x.Sum).ToArray(), got.Select(x => x.Sum).ToArray());
    }
}
