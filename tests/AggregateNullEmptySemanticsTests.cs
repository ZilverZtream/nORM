using System;
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
/// Aggregate NULL/empty-set semantics must match LINQ-to-Objects (Enumerable): Sum over an
/// all-NULL nullable column is 0 — NOT null — because <see cref="System.Linq.Enumerable.Sum(System.Collections.Generic.IEnumerable{int?})"/>
/// seeds its accumulator at 0 and skips nulls, so it never returns null (this deliberately diverges
/// from EF Core, which returns null here). Sum over an empty non-nullable set is likewise 0.
/// Max/Min/Average over an all-NULL nullable column are null (those skip nulls and have no zero to
/// fall back on), while over an empty non-nullable set they throw. Getting any of these wrong
/// silently returns the wrong number or the wrong exception behavior.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class AggregateNullEmptySemanticsTests
{
    [Table("AggRow")]
    private class AggRow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Value { get; set; }
        public int? NullableValue { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Three rows; NullableValue is NULL for all of them.
            cmd.CommandText = "CREATE TABLE AggRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL, NullableValue INTEGER);" +
                              "INSERT INTO AggRow (Value, NullableValue) VALUES (10, NULL),(20, NULL),(30, NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Sum_of_all_null_nullable_is_zero()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        // Enumerable.Sum(IEnumerable<int?>) returns 0 (not null) over an all-null source; SQL
        // SUM(col) returns NULL there, so nORM maps NULL back to 0 to match the LINQ contract.
        Assert.Equal(0, ctx.Query<AggRow>().Sum(r => r.NullableValue));
    }

    [Fact]
    public void Max_and_min_of_all_null_nullable_is_null()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Null(ctx.Query<AggRow>().Max(r => r.NullableValue));
        Assert.Null(ctx.Query<AggRow>().Min(r => r.NullableValue));
    }

    [Fact]
    public void Average_of_all_null_nullable_is_null()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Null(ctx.Query<AggRow>().Average(r => r.NullableValue));
    }

    [Fact]
    public void Sum_over_empty_nonnullable_is_zero()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Equal(0, ctx.Query<AggRow>().Where(r => r.Id == 999).Sum(r => r.Value));
    }

    [Fact]
    public void Max_over_empty_nonnullable_throws()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Throws<InvalidOperationException>(() => ctx.Query<AggRow>().Where(r => r.Id == 999).Max(r => r.Value));
    }

    [Fact]
    public void Average_over_empty_nonnullable_throws()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        Assert.Throws<InvalidOperationException>(() => ctx.Query<AggRow>().Where(r => r.Id == 999).Average(r => r.Value));
    }
}
