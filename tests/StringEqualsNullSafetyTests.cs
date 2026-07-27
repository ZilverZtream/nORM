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
/// string.Equals(a, b, StringComparison) is null-safe in C#: Equals(null, null, ...) is true. The SQL
/// predicate (a = b) yields UNKNOWN for two NULLs, silently dropping the both-null row. The plain ==
/// path already null-expands; the string.Equals(StringComparison) overload must too.
/// </summary>
[Trait("Category", "Fast")]
public class StringEqualsNullSafetyTests
{
    [Table("StrEqNull")]
    private sealed class Row
    {
        [Key] public int Id { get; set; }
        public string? A { get; set; }
        public string? B { get; set; }
    }

    private static readonly Row[] Seed =
    {
        new Row { Id = 1, A = null, B = null },   // Equals(null,null) == true
        new Row { Id = 2, A = "x", B = "x" },     // true
        new Row { Id = 3, A = "x", B = "y" },     // false
        new Row { Id = 4, A = "x", B = null },    // false (one null)
    };

    [Fact]
    public void String_equals_ordinal_is_null_safe_for_two_nulls()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE StrEqNull (Id INTEGER PRIMARY KEY, A TEXT NULL, B TEXT NULL);" +
                            "INSERT INTO StrEqNull VALUES (1,NULL,NULL),(2,'x','x'),(3,'x','y'),(4,'x',NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var norm = ctx.Query<Row>()
            .Where(x => string.Equals(x.A, x.B, StringComparison.Ordinal))
            .Select(x => x.Id).ToList().OrderBy(i => i).ToList();

        var oracle = Seed
            .Where(x => string.Equals(x.A, x.B, StringComparison.Ordinal))
            .Select(x => x.Id).OrderBy(i => i).ToList();

        Assert.Equal(oracle, norm);          // oracle = [1, 2]
        Assert.Contains(1, norm);            // the both-null row must be included
    }
}
