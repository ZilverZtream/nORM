using System;
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
/// StartsWith/Contains/EndsWith over a literal that itself contains a LIKE metacharacter
/// (<c>%</c>, <c>_</c>) must match only the literal text, not treat the metacharacter as a wildcard.
/// <c>Name.Contains("50%")</c> must NOT match "500"/"5000". Failing to escape silently returns the
/// wrong rows — a data-correctness bug that also mirrors an injection-shaped mistake.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class LikeWildcardEscapingTests
{
    [Table("Lw")]
    private class Lw
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // Names chosen so a wildcard leak is visible:
    //  1 "50%off"   2 "500off"   3 "a_b"   4 "axb"   5 "100%"   6 "1000"
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Lw (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "INSERT INTO Lw (Name) VALUES ('50%off'),('500off'),('a_b'),('axb'),('100%'),('1000');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Lw[] Reference =
    {
        new Lw { Id = 1, Name = "50%off" }, new Lw { Id = 2, Name = "500off" },
        new Lw { Id = 3, Name = "a_b" },    new Lw { Id = 4, Name = "axb" },
        new Lw { Id = 5, Name = "100%" },   new Lw { Id = 6, Name = "1000" },
    };

    private static void AssertMatchesLinq(Func<IQueryable<Lw>, IQueryable<Lw>> apply)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = apply(Reference.AsQueryable()).Select(x => x.Id).OrderBy(i => i).ToList();
        var actual = apply(ctx.Query<Lw>()).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Contains_percent_literal_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => x.Name.Contains("50%")));

    [Fact] public void StartsWith_percent_literal_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => x.Name.StartsWith("100%")));

    [Fact] public void EndsWith_percent_literal_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => x.Name.EndsWith("0%")));

    [Fact] public void Contains_underscore_literal_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => x.Name.Contains("a_")));

    [Fact] public void StartsWith_underscore_literal_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => x.Name.StartsWith("a_")));

    [Fact] public void Contains_percent_variable_matches_linq()
    {
        var p = "50%";
        AssertMatchesLinq(q => q.Where(x => x.Name.Contains(p)));
    }

    [Fact] public void Contains_underscore_variable_matches_linq()
    {
        var p = "a_";
        AssertMatchesLinq(q => q.Where(x => x.Name.Contains(p)));
    }

    [Fact] public void Projection_of_contains_percent_matches_linq()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.OrderBy(x => x.Id).Select(x => x.Name.Contains("50%")).ToList();
        var actual = ctx.Query<Lw>().OrderBy(x => x.Id).Select(x => x.Name.Contains("50%")).ToList();
        Assert.Equal(expected, actual);
    }
}
