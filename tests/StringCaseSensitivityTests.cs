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
/// string.Contains/StartsWith/EndsWith without a StringComparison are ordinal (case-SENSITIVE) in
/// .NET. SQLite's LIKE is case-INSENSITIVE for ASCII by default, so a naive LIKE translation would
/// silently match rows LINQ-to-Objects would not (Name.Contains("abc") pulling in "ABC"). Equality
/// (==) is case-sensitive on both sides and must agree. The explicit OrdinalIgnoreCase overloads
/// must instead match case-insensitively. Every case here is checked against the LINQ result.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class StringCaseSensitivityTests
{
    [Table("Sc")]
    private class Sc
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    //  1 abc   2 ABC   3 AbC   4 xabcy   5 xABCy
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Sc (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "INSERT INTO Sc (Name) VALUES ('abc'),('ABC'),('AbC'),('xabcy'),('xABCy');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Sc[] Reference =
    {
        new Sc { Id = 1, Name = "abc" },   new Sc { Id = 2, Name = "ABC" },
        new Sc { Id = 3, Name = "AbC" },   new Sc { Id = 4, Name = "xabcy" },
        new Sc { Id = 5, Name = "xABCy" },
    };

    private static void AssertMatchesLinq(Func<IQueryable<Sc>, IQueryable<Sc>> apply)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = apply(Reference.AsQueryable()).Select(x => x.Id).OrderBy(i => i).ToList();
        var actual = apply(ctx.Query<Sc>()).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Contains_is_case_sensitive()
        => AssertMatchesLinq(q => q.Where(x => x.Name.Contains("abc")));

    [Fact] public void StartsWith_is_case_sensitive()
        => AssertMatchesLinq(q => q.Where(x => x.Name.StartsWith("ab")));

    [Fact] public void EndsWith_is_case_sensitive()
        => AssertMatchesLinq(q => q.Where(x => x.Name.EndsWith("bc")));

    [Fact] public void Equality_is_case_sensitive()
        => AssertMatchesLinq(q => q.Where(x => x.Name == "abc"));

    [Fact] public void Contains_ordinal_ignorecase_matches_insensitively()
        => AssertMatchesLinq(q => q.Where(x => x.Name.Contains("abc", StringComparison.OrdinalIgnoreCase)));

    [Fact] public void StartsWith_ordinal_ignorecase_matches_insensitively()
        => AssertMatchesLinq(q => q.Where(x => x.Name.StartsWith("ab", StringComparison.OrdinalIgnoreCase)));
}
