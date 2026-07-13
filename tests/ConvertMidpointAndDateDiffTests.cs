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
/// .NET Convert.ToInt32/ToInt64 over floating-point and decimal sources round half to
/// even (2.5 -> 2, 3.5 -> 4, -2.5 -> -2), while a bare SQL CAST truncates on
/// SQLite/SQL Server and rounds half away from zero on MySQL. DateTime subtraction
/// must be tick-exact: SQLite's julianday difference drifted by microseconds
/// (a 90-second difference came back as 89.9999946s), breaking equality on spans.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConvertMidpointAndDateDiffTests
{
    [Table("ConvDiff_Row")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public double Val { get; set; }
        public decimal Price { get; set; }
        public DateTime A { get; set; }
        public DateTime B { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ConvDiff_Row (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Val REAL NOT NULL,
                    Price TEXT NOT NULL,
                    A TEXT NOT NULL,
                    B TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Row { Val = 2.5, Price = 2.5m, A = new DateTime(2020, 1, 2, 3, 0, 0), B = new DateTime(2020, 1, 1, 0, 0, 0) });
        ctx.Add(new Row { Val = 3.5, Price = 3.5m, A = new DateTime(2020, 6, 1, 0, 0, 30), B = new DateTime(2020, 5, 31, 23, 59, 0) });
        ctx.Add(new Row { Val = -2.5, Price = -2.5m, A = new DateTime(2020, 7, 1, 0, 0, 0, 500), B = new DateTime(2020, 7, 1, 0, 0, 0) });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (cn, ctx);
    }

    [Fact]
    public void Convert_ToInt32_from_double_rounds_half_to_even()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToInt32(r.Val)).ToList();
        Assert.Equal(new[] { 2, 4, -2 }, actual);
    }

    [Fact]
    public void Convert_ToInt64_from_double_rounds_half_to_even()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToInt64(r.Val)).ToList();
        Assert.Equal(new long[] { 2, 4, -2 }, actual);
    }

    [Fact]
    public void Convert_ToInt32_from_decimal_rounds_half_to_even()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => Convert.ToInt32(r.Price)).ToList();
        Assert.Equal(new[] { 2, 4, -2 }, actual);
    }

    [Fact]
    public void Convert_ToInt32_in_predicate_uses_dotnet_rounding()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Row>().Where(r => Convert.ToInt32(r.Val) == 4).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void Explicit_int_cast_truncates_toward_zero()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // C# explicit casts truncate: (int)2.5 -> 2, (int)3.5 -> 3, (int)-2.5 -> -2
        // (unlike Convert.ToInt32's round-half-to-even above).
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (int)r.Val).ToList();
        Assert.Equal(new[] { 2, 3, -2 }, actual);
    }

    [Fact]
    public void Explicit_long_cast_truncates_toward_zero()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (long)r.Val).ToList();
        Assert.Equal(new long[] { 2, 3, -2 }, actual);
    }

    [Fact]
    public void DateTime_subtraction_is_tick_exact()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.A - r.B).ToList();
        Assert.Equal(new[]
        {
            TimeSpan.FromHours(27),
            TimeSpan.FromSeconds(90),
            TimeSpan.FromMilliseconds(500),
        }, actual);
    }

    [Fact]
    public void DateTime_subtraction_equality_predicate_matches()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var span = TimeSpan.FromSeconds(90);
        var ids = ctx.Query<Row>().Where(r => (r.A - r.B) == span).Select(r => r.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }
}
