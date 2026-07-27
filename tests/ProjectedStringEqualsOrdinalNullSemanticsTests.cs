using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A projected <c>string.Equals(a, b, StringComparison.Ordinal)</c> must materialize the SAME boolean that
/// LINQ-to-Objects produces. The static three-argument overload is null-safe — <c>Equals(null, null, ...)</c>
/// is <c>true</c> — but the projection emitted a bare <c>(a = b)</c>, which is UNKNOWN when either operand is
/// NULL and materializes the both-NULL row as <c>false</c>. (On MySQL / SQL Server the bare <c>=</c>
/// additionally folds case under the default collation, contradicting Ordinal; that half is covered by the
/// provider ordinal wrap and exercised live elsewhere.)
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ProjectedStringEqualsOrdinalNullSemanticsTests
{
    private class Row
    {
        public int Id { get; set; }
        public string? A { get; set; }
        public string? B { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Row (Id INTEGER PRIMARY KEY, A TEXT NULL, B TEXT NULL);" +
                "INSERT INTO Row (Id, A, B) VALUES (1,'x','x'),(2,'x','y'),(3,NULL,NULL),(4,'x',NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, A = "x",  B = "x"  },
        new Row { Id = 2, A = "x",  B = "y"  },
        new Row { Id = 3, A = null, B = null },
        new Row { Id = 4, A = "x",  B = null },
    };

    [Fact]
    public void Projected_static_string_equals_ordinal_matches_linq_including_both_null()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var expected = Reference.AsQueryable()
            .Select(r => new { r.Id, Eq = string.Equals(r.A, r.B, StringComparison.Ordinal) })
            .OrderBy(r => r.Id).ToList();
        var actual = ctx.Query<Row>()
            .Select(r => new { r.Id, Eq = string.Equals(r.A, r.B, StringComparison.Ordinal) })
            .OrderBy(r => r.Id).ToList();

        // Row 3 (both NULL) must be Eq=true; row 1 true; rows 2/4 false.
        Assert.Equal(expected.Select(r => (r.Id, r.Eq)), actual.Select(r => (r.Id, r.Eq)));
    }
}
