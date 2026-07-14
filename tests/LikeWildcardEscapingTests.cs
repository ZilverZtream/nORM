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
public class LikeWildcardEscapingTests
{
    [Table("LweRow")]
    public class Row { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    // Names containing literal LIKE wildcards (% and _) plus decoys.
    private static readonly (int Id, string Name)[] Data =
    {
        (1, "50%off"), (2, "a_b"), (3, "plain"), (4, "100percent"), (5, "x_y_z"), (6, "no wildcard"),
        (7, "%leading"), (8, "trailing_"),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE LweRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, name) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = "INSERT INTO LweRow VALUES (@i, @n)";
                ins.Parameters.AddWithValue("@i", id);
                ins.Parameters.AddWithValue("@n", name);
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, Name = d.Name });

    private static async Task Check(DbContext ctx, System.Linq.Expressions.Expression<Func<Row, bool>> pred, string label)
    {
        var got = (await ctx.Query<Row>().Where(pred).Select(r => new { r.Id }).ToListAsync())
            .Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().AsQueryable().Where(pred).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.True(oracle.SequenceEqual(got),
            $"{label}: expected [{string.Join(",", oracle)}] got [{string.Join(",", got)}]");
    }

    [Fact]
    public async Task Contains_percent_matches_literal_percent_only()
    {
        await using var ctx = Make();
        await Check(ctx, r => r.Name.Contains("%"), "Contains(%)");            // {1,7}
        await Check(ctx, r => r.Name.Contains("_"), "Contains(_)");            // {2,5,8}
        await Check(ctx, r => r.Name.Contains("50%"), "Contains(50%)");        // {1}
    }

    [Fact]
    public async Task StartsWith_EndsWith_escape_wildcards()
    {
        await using var ctx = Make();
        await Check(ctx, r => r.Name.StartsWith("50%"), "StartsWith(50%)");    // {1}
        await Check(ctx, r => r.Name.StartsWith("%"), "StartsWith(%)");        // {7}
        await Check(ctx, r => r.Name.EndsWith("_"), "EndsWith(_)");            // {8}
        await Check(ctx, r => r.Name.EndsWith("off"), "EndsWith(off)");        // {1}
    }

    [Fact]
    public async Task Underscore_is_not_a_single_char_wildcard()
    {
        await using var ctx = Make();
        // "a_b" must NOT match a query for "aXb"-style single-char wildcard; Contains("a_b") is literal.
        await Check(ctx, r => r.Name.Contains("a_b"), "Contains(a_b)");        // {2}
        await Check(ctx, r => r.Name.Contains("x_y"), "Contains(x_y)");        // {5}
    }
}
