using System;
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
/// The simple-query fast path handled First/FirstOrDefault by reading only the source argument and
/// silently DROPPED the predicate overload's filter (Arguments[1]), so First(r => r.Id == N) emitted
/// "SELECT ... FROM T" with no WHERE and returned the first row of the whole table — a HIGH silent
/// wrong result. The fast path must decline the predicate overload so it falls through to the full
/// translator.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class FirstPredicateFastPathTests
{
    [Table("FpRow")]
    private class FpRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "INSERT INTO FpRow VALUES (1,'one'),(2,'two'),(3,'three'),(300,'threehundred');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void First_with_literal_predicate_returns_matching_row()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // First(r => r.Id == 1) coincidentally matched row 1 even when broken; 300 exposes the drop.
        Assert.Equal(1, ctx.Query<FpRow>().First(r => r.Id == 1).Id);
        Assert.Equal(300, ctx.Query<FpRow>().First(r => r.Id == 300).Id);   // BUG: returned 1 (first row)
        Assert.Equal(2, ctx.Query<FpRow>().First(r => r.Id == 2).Id);
    }

    [Fact]
    public void Sequential_different_literals_return_different_rows()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // A prior query with a different literal must not leak into the next (global plan-cache concern).
        Assert.Equal(1, ctx.Query<FpRow>().First(r => r.Id == 1).Id);
        Assert.Equal(3, ctx.Query<FpRow>().First(r => r.Id == 3).Id);       // BUG: returned 1
        Assert.Equal(300, ctx.Query<FpRow>().First(r => r.Id == 300).Id);   // BUG: returned 1
    }

    [Fact]
    public void FirstOrDefault_with_predicate_matches_or_returns_null()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(300, ctx.Query<FpRow>().FirstOrDefault(r => r.Id == 300)!.Id);
        Assert.Null(ctx.Query<FpRow>().FirstOrDefault(r => r.Id == 9999));  // no match -> null, not row 1
    }

    [Fact]
    public void First_with_variable_predicate_returns_matching_row()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        var id = 300;
        Assert.Equal(300, ctx.Query<FpRow>().First(r => r.Id == id).Id);
    }
}
