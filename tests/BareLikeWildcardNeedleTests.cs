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
/// The starkest wildcard-escape leak detector: a needle that is *only* a LIKE metacharacter.
/// If <c>%</c> or <c>_</c> are not escaped, <c>Contains("%")</c> silently matches every row and
/// <c>Contains("_")</c> matches every single-or-more-char row, rather than only rows containing a
/// literal <c>%</c>/<c>_</c>. Complements <see cref="LikeWildcardEscapingTests"/> which uses
/// metacharacters embedded in longer literals.
/// </summary>
[Trait("Category", "Fast")]
public class BareLikeWildcardNeedleTests
{
    [Table("BareLw")]
    private class Row { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    //  1 "50%off"(has %)  2 "a_b"(has _)  3 "plain"  4 "%lead"(has %)  5 "trail_"(has _)  6 "clean"
    private static readonly (int Id, string Name)[] Data =
    {
        (1, "50%off"), (2, "a_b"), (3, "plain"), (4, "%lead"), (5, "trail_"), (6, "clean"),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BareLw (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        foreach (var (id, name) in Data)
        {
            using var ins = cn.CreateCommand();
            ins.CommandText = "INSERT INTO BareLw VALUES (@i, @n)";
            ins.Parameters.AddWithValue("@i", id);
            ins.Parameters.AddWithValue("@n", name);
            ins.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, Name = d.Name });

    private static void Check(DbContext ctx, System.Linq.Expressions.Expression<Func<Row, bool>> pred, string label)
    {
        var got = ctx.Query<Row>().Where(pred).Select(r => new { r.Id }).ToList()
            .Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().AsQueryable().Where(pred).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.True(oracle.SequenceEqual(got),
            $"{label}: expected [{string.Join(",", oracle)}] got [{string.Join(",", got)}]");
    }

    [Fact]
    public void Bare_percent_needle_matches_only_literal_percent_rows()
    {
        using var ctx = Make();
        Check(ctx, r => r.Name.Contains("%"), "Contains(%)");        // {1,4}
        Check(ctx, r => r.Name.StartsWith("%"), "StartsWith(%)");    // {4}
        Check(ctx, r => r.Name.EndsWith("%"), "EndsWith(%)");        // {} (none end with %)
    }

    [Fact]
    public void Bare_underscore_needle_matches_only_literal_underscore_rows()
    {
        using var ctx = Make();
        Check(ctx, r => r.Name.Contains("_"), "Contains(_)");        // {2,5}
        Check(ctx, r => r.Name.EndsWith("_"), "EndsWith(_)");        // {5}
        Check(ctx, r => r.Name.StartsWith("_"), "StartsWith(_)");    // {} (none start with _)
    }

    [Fact]
    public void Bare_wildcard_needle_from_closure_matches_literal_only()
    {
        using var ctx = Make();
        var pctPattern = "%";
        var underscorePattern = "_";
        Check(ctx, r => r.Name.Contains(pctPattern), "Contains(closure %)");       // {1,4}
        Check(ctx, r => r.Name.Contains(underscorePattern), "Contains(closure _)"); // {2,5}
    }
}
