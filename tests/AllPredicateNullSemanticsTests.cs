using System;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A top-level <c>All(predicate)</c> over a nullable column must return the SAME boolean that
/// LINQ-to-Objects returns. SQL evaluates <c>All</c> as <c>NOT EXISTS(rows that violate the predicate)</c>,
/// so the predicate must be negated with three-valued-logic awareness: a NULL row that makes the predicate
/// false in C# (e.g. <c>null &gt; 5</c>) must be counted as a violation. Negating the predicate as a bare
/// textual <c>NOT (...)</c> leaves it UNKNOWN for NULL rows, so they escape the violation subquery and
/// <c>All</c> silently flips from <c>false</c> to <c>true</c> — a silent-wrong result on a query that
/// "succeeds".
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class AllPredicateNullSemanticsTests
{
    private class Row
    {
        public int Id { get; set; }
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
                "CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER, Name TEXT);" +
                "INSERT INTO Row (Value, Name) VALUES (5,'a'),(7,'b'),(NULL,NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Value = 5, Name = "a" },
        new Row { Id = 2, Value = 7, Name = "b" },
        new Row { Id = 3, Value = null, Name = null },
    };

    private static void AssertAllMatchesLinq(Expression<Func<Row, bool>> predicate)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.AsQueryable().All(predicate);
        var actual = ctx.Query<Row>().All(predicate);
        Assert.Equal(expected, actual);
    }

    // Every non-null row SATISFIES these, so only the NULL row (false in C#) can make All false —
    // isolating the three-valued-logic negation path that the textual NOT(...) bug got wrong.
    [Fact] public void All_greaterThan_on_nullable_counts_null_as_violation()
        => AssertAllMatchesLinq(r => r.Value > 4);

    [Fact] public void All_lessThanOrEqual_on_nullable_counts_null_as_violation()
        => AssertAllMatchesLinq(r => r.Value <= 100);

    [Fact] public void All_greaterThanOrEqual_on_nullable_counts_null_as_violation()
        => AssertAllMatchesLinq(r => r.Value >= 0);

    // Both named rows satisfy, so only the NULL Name row (== is false in C#) can make All false.
    [Fact] public void All_string_equality_disjunction_counts_null_as_violation()
        => AssertAllMatchesLinq(r => r.Name == "a" || r.Name == "b");

    // A non-null row also violates here, so All is false regardless — a control that must stay false.
    [Fact] public void All_string_equality_single_constant_stays_false()
        => AssertAllMatchesLinq(r => r.Name == "a");

    // Positive control: predicate true for every row (Id is non-null and > 0) → All must be true.
    [Fact] public void All_nonNull_column_all_satisfy_returns_true()
        => AssertAllMatchesLinq(r => r.Id > 0);

    // Nullable-value inequality: null != 999 is true in C#; must stay All=true.
    [Fact] public void All_notEqual_constant_on_nullable_matches_linq()
        => AssertAllMatchesLinq(r => r.Value != 999);
}
