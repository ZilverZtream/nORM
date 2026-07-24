using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Regression: a HAVING (and its projected aggregate) over a set-op-sourced GroupBy must qualify the
/// aggregate column against the compound's derived-table wrap alias — not a phantom <c>T{n}</c>. The shape
/// <c>q.SetOp(q2).GroupBy(k).Where(g =&gt; g.Max(col) &lt;op&gt; v).Select(g =&gt; new { g.Key, V = g.Max(col) })</c>
/// previously emitted <c>MAX("T1"."B")</c> against a compound wrapped as <c>… AS "__wgb0"</c>, throwing
/// <c>no such column: T1.B</c>. Count() hid it (no column operand). Found by the query-IR HAVING fuzzer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpGroupByHavingTests
{
    [Table("SetOpHaving_Test")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE SetOpHaving_Test (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
                "INSERT INTO SetOpHaving_Test VALUES (1,1,5),(2,1,2),(3,2,7),(4,2,1),(5,3,4),(6,3,9);";
            c.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static readonly Row[] Reference =
    {
        new() { Id = 1, A = 1, B = 5 }, new() { Id = 2, A = 1, B = 2 },
        new() { Id = 3, A = 2, B = 7 }, new() { Id = 4, A = 2, B = 1 },
        new() { Id = 5, A = 3, B = 4 }, new() { Id = 6, A = 3, B = 9 },
    };

    [Fact]
    public async Task Having_max_column_over_a_union_group_matches_oracle()
    {
        using var ctx = NewCtx();
        var norm = (await ctx.Query<Row>()
            .Union(ctx.Query<Row>().Where(r => r.A >= 2))
            .GroupBy(r => r.A)
            .Where(g => g.Max(r => r.B) > 6)
            .Select(g => new { g.Key, V = g.Max(r => r.B) })
            .ToListAsync())
            .OrderBy(x => x.Key).Select(x => (x.Key, x.V)).ToList();

        var oracle = Reference
            .Union(Reference.Where(r => r.A >= 2))
            .GroupBy(r => r.A)
            .Where(g => g.Max(r => r.B) > 6)
            .Select(g => new { g.Key, V = g.Max(r => r.B) })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.V)).ToList();

        Assert.Equal(oracle, norm);
        Assert.NotEmpty(oracle); // guards against a vacuously-passing empty result
    }

    [Theory]
    [InlineData("Sum")]
    [InlineData("Min")]
    [InlineData("Max")]
    public async Task Having_column_aggregate_over_each_setop_matches_oracle(string agg)
    {
        using var ctx = NewCtx();

        foreach (var kind in new[] { "Union", "Intersect", "Except" })
        {
            IQueryable<Row> Left() => ctx.Query<Row>();
            IQueryable<Row> Right() => ctx.Query<Row>().Where(r => r.B >= 4);
            var combined = kind switch
            {
                "Union" => Left().Union(Right()),
                "Intersect" => Left().Intersect(Right()),
                _ => Left().Except(Right()),
            };
            var grouped = combined.GroupBy(r => r.A);
            var normPairs = agg switch
            {
                "Sum" => await grouped.Where(g => g.Sum(r => r.B) > 3).Select(g => new { g.Key, V = g.Sum(r => r.B) }).ToListAsync(),
                "Min" => await grouped.Where(g => g.Min(r => r.B) > 1).Select(g => new { g.Key, V = g.Min(r => r.B) }).ToListAsync(),
                _ => await grouped.Where(g => g.Max(r => r.B) > 3).Select(g => new { g.Key, V = g.Max(r => r.B) }).ToListAsync(),
            };
            var norm = normPairs.OrderBy(x => x.Key).Select(x => (x.Key, x.V)).ToList();

            var rLeft = Reference.AsEnumerable();
            var rRight = Reference.Where(r => r.B >= 4);
            var rCombined = kind switch
            {
                "Union" => rLeft.Union(rRight),
                "Intersect" => rLeft.Intersect(rRight),
                _ => rLeft.Except(rRight),
            };
            var rGrouped = rCombined.GroupBy(r => r.A);
            var oracle = (agg switch
            {
                "Sum" => rGrouped.Where(g => g.Sum(r => r.B) > 3).Select(g => new { g.Key, V = g.Sum(r => r.B) }),
                "Min" => rGrouped.Where(g => g.Min(r => r.B) > 1).Select(g => new { g.Key, V = g.Min(r => r.B) }),
                _ => rGrouped.Where(g => g.Max(r => r.B) > 3).Select(g => new { g.Key, V = g.Max(r => r.B) }),
            }).OrderBy(x => x.Key).Select(x => (x.Key, x.V)).ToList();

            Assert.Equal(oracle, norm);
        }
    }
}
