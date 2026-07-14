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
/// Conditional expressions inside aggregate selectors — the common reporting pattern
/// Sum(x =&gt; cond ? x.A : 0) (CASE inside SUM) and Count(predicate), grouped and whole-table —
/// must match LINQ-to-Objects.
/// </summary>
[Trait("Category", "Fast")]
public class ConditionalAggregateTests
{
    [Table("CagRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int K { get; set; }
        public int A { get; set; }
        public bool Flag { get; set; }
    }

    private static readonly (int K, int A, bool Flag)[] Data =
    {
        (1, 3, true), (1, 8, false), (1, 12, true),
        (2, 1, false), (2, 20, true),
        (3, 6, false), (3, 6, false),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CagRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, K INTEGER NOT NULL, A INTEGER NOT NULL, Flag INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (k, a, flag) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO CagRow (K, A, Flag) VALUES ({k}, {a}, {(flag ? 1 : 0)})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() =>
        Data.Select((d, i) => new Row { Id = i + 1, K = d.K, A = d.A, Flag = d.Flag });

    [Fact]
    public async Task Conditional_sum_and_predicate_count_per_group()
    {
        await using var ctx = Make();
        try
        {
            var got = (await ctx.Query<Row>()
                .GroupBy(r => r.K)
                .Select(g => new
                {
                    Key = g.Key,
                    CondSum = g.Sum(x => x.A > 5 ? x.A : 0),       // CASE inside SUM
                    FlagCount = g.Count(x => x.Flag),               // COUNT with predicate
                    BigCount = g.Count(x => x.A >= 10),
                })
                .ToListAsync())
                .OrderBy(x => x.Key).ToList();

            var oracle = Oracle()
                .GroupBy(r => r.K)
                .Select(g => new
                {
                    Key = g.Key,
                    CondSum = g.Sum(x => x.A > 5 ? x.A : 0),
                    FlagCount = g.Count(x => x.Flag),
                    BigCount = g.Count(x => x.A >= 10),
                })
                .OrderBy(x => x.Key).ToList();

            Assert.Equal(oracle.Count, got.Count);
            for (int i = 0; i < oracle.Count; i++)
            {
                Assert.Equal(oracle[i].Key, got[i].Key);
                Assert.Equal(oracle[i].CondSum, got[i].CondSum);
                Assert.Equal(oracle[i].FlagCount, got[i].FlagCount);
                Assert.Equal(oracle[i].BigCount, got[i].BigCount);
            }
        }
        catch (Exception ex) { Assert.Fail($"GROUPED THREW: {ex.GetType().Name}: {ex.Message}"); }
    }

    [Fact]
    public async Task Conditional_sum_whole_table()
    {
        await using var ctx = Make();
        try
        {
            var got = await ctx.Query<Row>().SumAsync(x => x.Flag ? x.A : 0);
            var oracle = Oracle().Sum(x => x.Flag ? x.A : 0);
            Assert.Equal(oracle, got);
        }
        catch (Exception ex) { Assert.Fail($"WHOLE THREW: {ex.GetType().Name}: {ex.Message}"); }
    }
}
