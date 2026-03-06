using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// QP-1: Verifies that scalar aggregate materializers return the correct CLR type and value
/// instead of always coercing to long.
/// </summary>
public class ScalarAggregateTypeTests
{
    [Table("AggRow")]
    private class AggRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
        public DateTime DateValue { get; set; }
        public int? NullableInt { get; set; }
        public double? NullableDouble { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE AggRow (
                    Id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntValue     INTEGER NOT NULL DEFAULT 0,
                    LongValue    INTEGER NOT NULL DEFAULT 0,
                    DoubleValue  REAL    NOT NULL DEFAULT 0,
                    DecimalValue REAL    NOT NULL DEFAULT 0,
                    DateValue    TEXT    NOT NULL DEFAULT '0001-01-01',
                    NullableInt  INTEGER NULL,
                    NullableDouble REAL NULL
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(SqliteConnection cn, int i, long l, double d, decimal m, DateTime dt, int? ni, double? nd)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO AggRow (IntValue,LongValue,DoubleValue,DecimalValue,DateValue,NullableInt,NullableDouble) VALUES (@i,@l,@d,@m,@dt,@ni,@nd)";
        cmd.Parameters.AddWithValue("@i", i);
        cmd.Parameters.AddWithValue("@l", l);
        cmd.Parameters.AddWithValue("@d", d);
        cmd.Parameters.AddWithValue("@m", (double)m);
        cmd.Parameters.AddWithValue("@dt", dt.ToString("O"));
        cmd.Parameters.AddWithValue("@ni", (object?)ni ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nd", (object?)nd ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ─── Sum ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Sum_Int_Returns_Int()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 10, 0, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 20, 0, 0, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().SumAsync(r => r.IntValue);
        Assert.IsType<int>(result);
        Assert.Equal(30, result);
    }

    [Fact]
    public async Task Sum_Long_Returns_Long()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 0, 100L, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 0, 200L, 0, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().SumAsync(r => r.LongValue);
        Assert.IsType<long>(result);
        Assert.Equal(300L, result);
    }

    [Fact]
    public async Task Sum_Double_Returns_Double()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 0, 0, 1.5, 0, DateTime.MinValue, null, null);
        Insert(cn, 0, 0, 2.5, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().SumAsync(r => r.DoubleValue);
        Assert.IsType<double>(result);
        Assert.Equal(4.0, result, precision: 10);
    }

    [Fact]
    public async Task Sum_Decimal_Returns_Decimal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 0, 0, 0, 1.5m, DateTime.MinValue, null, null);
        Insert(cn, 0, 0, 0, 2.5m, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().SumAsync(r => r.DecimalValue);
        Assert.IsType<decimal>(result);
        Assert.Equal(4.0m, result);
    }

    [Fact]
    public async Task Sum_Int_EmptySet_Returns_Zero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var result = await ctx.Query<AggRow>().SumAsync(r => r.IntValue);
        Assert.IsType<int>(result);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task Sum_NullableInt_EmptySet_Returns_Null()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var result = await ctx.Query<AggRow>().SumAsync(r => r.NullableInt);
        Assert.Null(result);
    }

    // ─── Average ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Average_Double_Returns_Double()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 0, 0, 1.0, 0, DateTime.MinValue, null, null);
        Insert(cn, 0, 0, 3.0, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().AverageAsync(r => r.DoubleValue);
        Assert.IsType<double>(result);
        Assert.Equal(2.0, result, precision: 10);
    }

    [Fact]
    public async Task Average_Decimal_Returns_Decimal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 0, 0, 0, 2.0m, DateTime.MinValue, null, null);
        Insert(cn, 0, 0, 0, 4.0m, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().AverageAsync(r => r.DecimalValue);
        Assert.IsType<decimal>(result);
        Assert.Equal(3.0m, result);
    }

    [Fact]
    public async Task Average_Double_EmptySet_Throws_InvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<AggRow>().AverageAsync(r => r.DoubleValue));
    }

    [Fact]
    public async Task Average_NullableDouble_EmptySet_Returns_Null()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var result = await ctx.Query<AggRow>().AverageAsync(r => r.NullableDouble);
        Assert.Null(result);
    }

    // ─── Min / Max ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Min_Int_Returns_Int()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 5, 0, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 3, 0, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 9, 0, 0, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().MinAsync(r => r.IntValue);
        Assert.IsType<int>(result);
        Assert.Equal(3, result);
    }

    [Fact]
    public async Task Max_Int_Returns_Int()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 5, 0, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 3, 0, 0, 0, DateTime.MinValue, null, null);
        Insert(cn, 9, 0, 0, 0, DateTime.MinValue, null, null);
        var result = await ctx.Query<AggRow>().MaxAsync(r => r.IntValue);
        Assert.IsType<int>(result);
        Assert.Equal(9, result);
    }

    [Fact]
    public async Task Min_Int_EmptySet_Throws_InvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<AggRow>().MinAsync(r => r.IntValue));
    }

    [Fact]
    public async Task Count_EmptySet_Returns_Zero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var result = await ctx.Query<AggRow>().CountAsync();
        Assert.IsType<int>(result);
        Assert.Equal(0, result);
    }
}
