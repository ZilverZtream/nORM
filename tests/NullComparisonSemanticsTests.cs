using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A LINQ predicate over a nullable column must return the SAME rows the equivalent
/// LINQ-to-Objects predicate would. SQL uses three-valued logic (NULL comparisons yield
/// UNKNOWN, which filters the row out), whereas C# treats <c>null != 5</c> as <c>true</c>.
/// Bridging that gap requires emitting <c>(col &lt;&gt; 5 OR col IS NULL)</c> for inequality and
/// compensating negated equalities. Getting it wrong silently returns the wrong row set — the
/// worst kind of ORM bug because the query "succeeds".
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NullComparisonSemanticsTests
{
    [Table("Nc")]
    private class Nc
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? Value { get; set; }
        public string? Name { get; set; }
    }

    // Row 1: Value=5,Name="a"  Row 2: Value=7,Name="b"  Row 3: Value=NULL,Name=NULL
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Nc (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER, Name TEXT);" +
                "INSERT INTO Nc (Value, Name) VALUES (5,'a'),(7,'b'),(NULL,NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // The in-memory reference rows, so each assertion compares nORM to LINQ-to-Objects exactly.
    private static readonly Nc[] Reference =
    {
        new Nc { Id = 1, Value = 5, Name = "a" },
        new Nc { Id = 2, Value = 7, Name = "b" },
        new Nc { Id = 3, Value = null, Name = null },
    };

    private static void AssertMatchesLinq(Func<IQueryable<Nc>, IQueryable<Nc>> apply)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = apply(Reference.AsQueryable()).Select(x => x.Id).OrderBy(i => i).ToList();
        var actual = apply(ctx.Query<Nc>()).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void NotEqual_constant_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => x.Value != 5));

    [Fact] public void Equal_constant_excludes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => x.Value == 5));

    [Fact] public void Negated_equality_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value == 5)));

    [Fact] public void Negated_inequality_excludes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value != 5)));

    [Fact] public void String_notequal_constant_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => x.Name != "b"));

    [Fact] public void String_negated_equality_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => !(x.Name == "b")));

    [Fact] public void NotEqual_null_closure_matches_linq()
    {
        int? v = null;
        AssertMatchesLinq(q => q.Where(x => x.Value != v));
    }

    [Fact] public void Equal_null_closure_matches_linq()
    {
        int? v = null;
        AssertMatchesLinq(q => q.Where(x => x.Value == v));
    }

    [Fact] public void NotEqual_nonnull_closure_includes_null_rows()
    {
        int? v = 5;
        AssertMatchesLinq(q => q.Where(x => x.Value != v));
    }

    [Fact] public void GreaterThan_constant_excludes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => x.Value > 5));

    [Fact] public void Negated_greaterthan_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value > 5)));

    [Fact] public void Negated_lessthanorequal_includes_null_rows()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value <= 5)));

    [Fact] public void Negated_conjunction_of_equalities_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value == 5 && x.Name == "a")));

    [Fact] public void Negated_disjunction_matches_linq()
        => AssertMatchesLinq(q => q.Where(x => !(x.Value == 5 || x.Name == "b")));
}
