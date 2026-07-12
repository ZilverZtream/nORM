using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A projected string-match (Select(x => x.Name.Contains("abc"))) must compute the same booleans
/// LINQ-to-Objects would: ordinal (case-sensitive), with LIKE metacharacters in the search text
/// matching literally. The WHERE path already does both; a projection that disagrees silently
/// returns wrong booleans — and wrong WHERE-vs-projection inconsistency for the same expression.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class StringMatchProjectionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("Sp")]
    private class Sp
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    //  1 abc   2 ABC   3 xabcy   4 50%off   5 a_b
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Sp (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "INSERT INTO Sp (Name) VALUES ('abc'),('ABC'),('xabcy'),('50%off'),('a_b');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Sp[] Reference =
    {
        new Sp { Id = 1, Name = "abc" },    new Sp { Id = 2, Name = "ABC" },
        new Sp { Id = 3, Name = "xabcy" },  new Sp { Id = 4, Name = "50%off" },
        new Sp { Id = 5, Name = "a_b" },
    };

    private static void AssertMatchesLinq(Func<IQueryable<Sp>, IQueryable<bool>> apply)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = apply(Reference.AsQueryable().OrderBy(x => x.Id)).ToList();
        var actual = apply(ctx.Query<Sp>().OrderBy(x => x.Id)).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Projected_contains_is_case_sensitive()
        => AssertMatchesLinq(q => q.Select(x => x.Name.Contains("abc")));

    [Fact] public void Projected_startswith_is_case_sensitive()
        => AssertMatchesLinq(q => q.Select(x => x.Name.StartsWith("ab")));

    [Fact] public void Projected_endswith_is_case_sensitive()
        => AssertMatchesLinq(q => q.Select(x => x.Name.EndsWith("bc")));

    [Fact] public void Projected_contains_escapes_percent()
        => AssertMatchesLinq(q => q.Select(x => x.Name.Contains("0%")));

    [Fact] public void Projected_contains_escapes_underscore()
        => AssertMatchesLinq(q => q.Select(x => x.Name.Contains("a_")));

    [Fact] public void Projected_contains_in_anonymous_type_is_case_sensitive()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.OrderBy(x => x.Id)
            .Select(x => new { x.Id, Hit = x.Name.Contains("abc") }).ToList();
        var actual = ctx.Query<Sp>().OrderBy(x => x.Id)
            .Select(x => new { x.Id, Hit = x.Name.Contains("abc") }).ToList();
        Assert.Equal(expected.Select(e => (e.Id, e.Hit)), actual.Select(a => (a.Id, a.Hit)));
    }
}
