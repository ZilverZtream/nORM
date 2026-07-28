using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// `g.Select(x =&gt; sel).Distinct().Count()` inside a GROUP projection must emit COUNT(DISTINCT sel), not
/// COUNT(*). The group-aggregate translator fell back to COUNT(*) when it could not extract a Where filter,
/// silently discarding the Select+Distinct and returning the group's total row count. Uses a direct
/// {Key, D} anonymous shape (no trailing untranslatable projection, no intermediate group OrderBy) so the
/// pure-SQL group path — not the client-side streaming path — is exercised.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByDistinctCountSqlPathTests
{
    [Table("GdcsRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int G { get; set; }
        public int A { get; set; }
    }

    private static DbContext Ctx(out Row[] seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        // group 1: A={10,10,30} -> distinct 2 ; group 2: A={5,5,5} -> distinct 1 ; group 3: A={1,2,3} -> distinct 3
        seed = new[]
        {
            new Row{Id=1,G=1,A=10}, new Row{Id=2,G=1,A=10}, new Row{Id=3,G=1,A=30},
            new Row{Id=4,G=2,A=5},  new Row{Id=5,G=2,A=5},  new Row{Id=6,G=2,A=5},
            new Row{Id=7,G=3,A=1},  new Row{Id=8,G=3,A=2},  new Row{Id=9,G=3,A=3},
        };
        var s = seed;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GdcsRow (Id INTEGER PRIMARY KEY, G INTEGER NOT NULL, A INTEGER NOT NULL);";
            foreach (var r in s) cmd.CommandText += $"INSERT INTO GdcsRow VALUES ({r.Id},{r.G},{r.A});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Distinct_count_in_group_projection_matches_oracle()
    {
        using var ctx = Ctx(out var seed);
        var expected = seed.GroupBy(r => r.G)
            .Select(g => new { g.Key, D = g.Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.D).ToList();

        var actual = ctx.Query<Row>().GroupBy(r => r.G)
            .Select(g => new { g.Key, D = g.Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.D).ToList();

        Assert.Equal(expected, actual);   // 1:2, 2:1, 3:3
    }

    [Fact]
    public void Distinct_count_bare_value_matches_oracle()
    {
        using var ctx = Ctx(out var seed);
        var expected = seed.GroupBy(r => r.G).Select(g => g.Select(x => x.A).Distinct().Count())
            .OrderBy(x => x).ToList();
        var actual = ctx.Query<Row>().GroupBy(r => r.G).Select(g => g.Select(x => x.A).Distinct().Count())
            .ToList().OrderBy(x => x).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_count_filtered_source_matches_oracle()
    {
        using var ctx = Ctx(out var seed);
        var expected = seed.GroupBy(r => r.G)
            .Select(g => new { g.Key, D = g.Where(x => x.A > 4).Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.D).ToList();
        var actual = ctx.Query<Row>().GroupBy(r => r.G)
            .Select(g => new { g.Key, D = g.Where(x => x.A > 4).Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => x.Key + ":" + x.D).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Distinct_count_nested_anon_with_sum_matches_oracle()
    {
        using var ctx = Ctx(out var seed);
        var expected = seed.GroupBy(r => r.G)
            .Select(g => new { g.Key, Total = g.Sum(x => x.A), D = g.Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Total}:{x.D}").ToList();
        var actual = ctx.Query<Row>().GroupBy(r => r.G)
            .Select(g => new { g.Key, Total = g.Sum(x => x.A), D = g.Select(x => x.A).Distinct().Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Total}:{x.D}").ToList();
        Assert.Equal(expected, actual);
    }
}
