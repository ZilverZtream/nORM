using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for a <c>char</c>-to-integer cast (the character code point), e.g.
/// <c>(int)r.Name[0]</c>. A char is represented in nORM as a single-character string
/// (<c>s[i] -&gt; SUBSTR(col, i+1, 1)</c>), so casting it to an integer must emit the provider's
/// char-code function (SQLite <c>unicode()</c>) — previously it left the substr text and the
/// materializer CAST it to 0 (silent-wrong). Covers projection, predicate, ordering and GroupBy-key
/// positions plus arithmetic and a long cast; char comparisons (a separate re-fold path) are included
/// as a no-regression guard. Each case runs the identical LINQ expression against nORM (SQLite) and
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class CharToIntCastTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CticRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly string[] Names = { "Alpha", "beta", "Zulu", "9nine", "Mango", "kiwi" };
    private static readonly Row[] Rows = Names.Select((n, i) => new Row { Id = i + 1, Name = n }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CticRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        foreach (var r in Rows) cmd.CommandText += $"INSERT INTO CticRow VALUES ({r.Id},'{r.Name}');";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    private static void Assert_<T>(Func<IQueryable<Row>, IEnumerable<T>> q)
    {
        var expected = q(Rows.AsQueryable()).ToList();
        using var ctx = Ctx();
        var actual = q(ctx.Query<Row>().AsQueryable()).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Projection_int_char_code() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Name[0]));
    [Fact] public void Projection_long_char_code() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (long)r.Name[0]));
    [Fact] public void Projection_char_code_arithmetic() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Name[0] + 1));
    [Fact] public void Projection_char_code_at_index_one() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Name[1]));
    [Fact] public void Projection_char_code_conditional() => Assert_(q => q.OrderBy(r => r.Id).Select(r => (int)r.Name[0] >= 97 ? 1 : 0));

    [Fact] public void Predicate_char_code_gt() => Assert_(q => q.Where(r => (int)r.Name[0] > 90).OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Predicate_char_code_eq() => Assert_(q => q.Where(r => (int)r.Name[0] == 65).OrderBy(r => r.Id).Select(r => r.Id));

    [Fact] public void OrderBy_char_code() => Assert_(q => q.OrderBy(r => (int)r.Name[0]).ThenBy(r => r.Id).Select(r => r.Id));

    [Fact]
    public void GroupBy_char_code() => Assert_(q => q.GroupBy(r => (int)r.Name[0] % 2)
        .Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.C}"));

    // No-regression guards: char comparison uses the lifted-char re-fold, not the code-point cast.
    [Fact] public void Char_comparison_gt() => Assert_(q => q.Where(r => r.Name[0] > 'M').OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Char_comparison_eq() => Assert_(q => q.Where(r => r.Name[0] == 'A').OrderBy(r => r.Id).Select(r => r.Id));
    [Fact] public void Char_to_string() => Assert_(q => q.OrderBy(r => r.Id).Select(r => r.Name[0].ToString()));
}
