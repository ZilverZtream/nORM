using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: type-conversion / cast translation on SQLite. Each test compares nORM's
/// SQL translation against the SAME lambda evaluated by LINQ-to-Objects (the oracle). A wrong
/// value / wrong row without an exception is a silent-wrong data-loss bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TypeConversionCastSilentWrongHuntTests
{
    private enum Status { Pending = 0, Processing = 1, Shipped = 2, Delivered = 3, Cancelled = 4 }

    private sealed class StatusStringConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    [Table("TccIntEnum")]
    private sealed class IntEnumRow
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }

    [Table("TccStrEnum")]
    private sealed class StrEnumRow
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }

    [Table("TccNum")]
    private sealed class NumRow
    {
        [Key] public int Id { get; set; }
        public double D { get; set; }
        public decimal M { get; set; }
        public long L { get; set; }
        public int I { get; set; }
        public bool B { get; set; }
        public string S { get; set; } = "";
        public int? NI { get; set; }
    }

    private static DbContext NewIntEnumCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TccIntEnum (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static DbContext NewStrEnumCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TccStrEnum (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<StrEnumRow>().Property(o => o.Status).HasConversion(new StatusStringConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static DbContext NewNumCtx(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TccNum (Id INTEGER PRIMARY KEY, D REAL NOT NULL, M TEXT NOT NULL, L INTEGER NOT NULL, I INTEGER NOT NULL, B INTEGER NOT NULL, S TEXT NOT NULL, NI INTEGER NULL);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    // =========================================================================================
    // SILENT-WRONG REPROS (these FAIL): Convert.ToDecimal / decimal.Parse lose precision on SQLite
    // because SqliteProvider.GetRealCastSql ignores asDecimal and always emits CAST AS REAL (double).
    // =========================================================================================

    [Fact]
    public async Task ConvertToDecimal_from_decimal_column_loses_precision_SILENTWRONG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        decimal precise = 12345678901234.56789m; // 19 sig digits, beyond REAL's ~15-16
        ctx.Add(new NumRow { Id = 1, D = 0, M = precise, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        // C#: Convert.ToDecimal(decimalValue) is identity -> exact.
        var r = ctx.Query<NumRow>().Where(x => x.Id == 1).Select(x => Convert.ToDecimal(x.M)).ToList().Single();
        Assert.Equal(Convert.ToDecimal(precise), r);
    }

    [Fact]
    public async Task ConvertToDecimal_from_high_precision_string_loses_precision_SILENTWRONG()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        string text = "12345678901234.56789";
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = 0, I = 0, B = false, S = text });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<NumRow>().Where(x => x.Id == 1).Select(x => Convert.ToDecimal(x.S)).ToList().Single();
        Assert.Equal(Convert.ToDecimal(text, System.Globalization.CultureInfo.InvariantCulture), r);
    }

    // Clean bill: the PREDICATE path compares the high-precision decimal correctly (routes through
    // nORM's exact decimal comparison, not CAST-AS-REAL), so the bug is projection-only.
    [Fact]
    public async Task ConvertToDecimal_predicate_high_precision_keeps_row_CLEAN()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        decimal precise = 12345678901234.56789m;
        ctx.Add(new NumRow { Id = 1, D = 0, M = precise, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var ids = ctx.Query<NumRow>().Where(x => Convert.ToDecimal(x.M) == precise).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    // Control: a PLAIN decimal projection (no Convert) preserves full precision, isolating the bug
    // to the Convert/CAST-AS-REAL path.
    [Fact]
    public async Task Plain_decimal_projection_preserves_precision_CONTROL()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        decimal precise = 12345678901234.56789m;
        ctx.Add(new NumRow { Id = 1, D = 0, M = precise, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<NumRow>().Where(x => x.Id == 1).Select(x => x.M).ToList().Single();
        Assert.Equal(precise, r);
    }

    // =========================================================================================
    // CLEAN BILL (these PASS): verified-correct conversions.
    // =========================================================================================

    [Fact]
    public async Task IntStoredEnum_ordinal_cast_projection_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewIntEnumCtx(cn);
        ctx.Add(new IntEnumRow { Id = 1, Status = Status.Shipped });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<IntEnumRow>().Where(x => x.Id == 1).Select(x => (int)x.Status).ToList().Single();
        Assert.Equal((int)Status.Shipped, r);
    }

    [Fact]
    public async Task StringStoredEnum_ordinal_cast_projection_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewStrEnumCtx(cn);
        ctx.Add(new StrEnumRow { Id = 1, Status = Status.Shipped });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<StrEnumRow>().Where(x => x.Id == 1).Select(x => (int)x.Status).ToList().Single();
        Assert.Equal((int)Status.Shipped, r);
    }

    [Fact]
    public async Task StringStoredEnum_ordinal_cast_predicate_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewStrEnumCtx(cn);
        ctx.Add(new StrEnumRow { Id = 0, Status = Status.Pending });
        ctx.Add(new StrEnumRow { Id = 2, Status = Status.Shipped });
        ctx.Add(new StrEnumRow { Id = 3, Status = Status.Delivered });
        ctx.Add(new StrEnumRow { Id = 4, Status = Status.Cancelled });
        await ctx.SaveChangesAsync();
        var ids = ctx.Query<StrEnumRow>().Where(x => (int)x.Status >= 2).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3, 4 }, ids);
    }

    [Fact]
    public async Task IntStoredEnum_ToString_projection_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewIntEnumCtx(cn);
        ctx.Add(new IntEnumRow { Id = 1, Status = Status.Shipped });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<IntEnumRow>().Where(x => x.Id == 1).Select(x => x.Status.ToString()).ToList().Single();
        Assert.Equal(Status.Shipped.ToString(), r);
    }

    [Fact]
    public async Task ConvertToInt32_double_bankers_rounding_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 2.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        ctx.Add(new NumRow { Id = 2, D = 3.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var rows = ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => Convert.ToInt32(x.D)).ToList();
        Assert.Equal(new[] { Convert.ToInt32(2.5), Convert.ToInt32(3.5) }, rows); // {2, 4}
    }

    [Fact]
    public async Task ConvertToInt32_double_bankers_rounding_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 2.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var ids = ctx.Query<NumRow>().Where(x => Convert.ToInt32(x.D) == 2).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    // Fail-loud inconsistency: top-level Convert.ToInt16/ToByte/ToSByte projection throws under the
    // default Throw policy (the translatability PROBE routes through provider TranslateFunction, which
    // only lists ToInt32/ToInt64), even though the SelectClauseVisitor emitter DOES support them and
    // Convert.ToInt32 in the same shape works. Fail-loud, not silent-wrong. Asserted here as-is.
    [Fact]
    public async Task ConvertToInt16_double_projection_fails_loud_inconsistent_with_ToInt32()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 2.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => Convert.ToInt16(x.D)).ToList());
    }

    [Fact]
    public async Task ConvertToByte_double_projection_fails_loud_inconsistent_with_ToInt32()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 2.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => Convert.ToByte(x.D)).ToList());
    }

    // Probe: under Allow policy the ToByte projection runs client-side and yields the CORRECT
    // banker's-rounded value — confirming the fail-loud path is NOT masking a silent-wrong.
    [Fact]
    public async Task ConvertToByte_double_projection_under_Allow_is_correct_not_silentwrong()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TccNum (Id INTEGER PRIMARY KEY, D REAL NOT NULL, M TEXT NOT NULL, L INTEGER NOT NULL, I INTEGER NOT NULL, B INTEGER NOT NULL, S TEXT NOT NULL, NI INTEGER NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { ClientEvaluationPolicy = ClientEvaluationPolicy.Allow };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new NumRow { Id = 1, D = 2.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        ctx.Add(new NumRow { Id = 2, D = 3.5, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var rows = ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => Convert.ToByte(x.D)).ToList();
        Assert.Equal(new byte[] { Convert.ToByte(2.5), Convert.ToByte(3.5) }, rows); // {2, 4}
    }

    [Fact]
    public async Task IntCast_double_truncates_toward_zero_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 2.9, M = 0m, L = 0, I = 0, B = false, S = "" });
        ctx.Add(new NumRow { Id = 2, D = -2.9, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var rows = ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => (int)x.D).ToList();
        Assert.Equal(new[] { (int)2.9, (int)-2.9 }, rows); // {2, -2}
    }

    [Fact]
    public async Task IntCast_decimal_truncates_toward_zero_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 0, M = 2.9m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var ids = ctx.Query<NumRow>().Where(x => (int)x.M == 2).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task ConvertToInt32_bool_to_one_or_zero_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = 0, I = 0, B = true, S = "" });
        ctx.Add(new NumRow { Id = 2, D = 0, M = 0m, L = 0, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var rows = ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => Convert.ToInt32(x.B)).ToList();
        Assert.Equal(new[] { Convert.ToInt32(true), Convert.ToInt32(false) }, rows); // {1, 0}
    }

    [Fact]
    public async Task Widening_int_to_long_compared_to_long_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = 0, I = 5, B = false, S = "" });
        await ctx.SaveChangesAsync();
        long threshold = 3L;
        var ids = ctx.Query<NumRow>().Where(x => (long)x.I > threshold).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public async Task Nullable_int_projection_null_propagates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = 0, I = 0, B = false, S = "", NI = null });
        ctx.Add(new NumRow { Id = 2, D = 0, M = 0m, L = 0, I = 0, B = false, S = "", NI = 7 });
        await ctx.SaveChangesAsync();
        var rows = ctx.Query<NumRow>().OrderBy(x => x.Id).Select(x => (long?)x.NI).ToList();
        Assert.Equal(new long?[] { null, 7L }, rows);
    }

    [Fact]
    public async Task ConvertToDecimal_from_int_column_is_exact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = 0, I = 42, B = false, S = "" });
        await ctx.SaveChangesAsync();
        var r = ctx.Query<NumRow>().Where(x => x.Id == 1).Select(x => Convert.ToDecimal(x.I)).ToList().Single();
        Assert.Equal(Convert.ToDecimal(42), r);
    }

    // =========================================================================================
    // FAIL-LOUD gaps (these PASS by asserting the current throw): NOT silent-wrong, documented
    // here for completeness / lower priority.
    // =========================================================================================

    // (int)longColumn for a value outside int range throws OverflowException at materialization
    // rather than C#'s unchecked wraparound. Fail-loud (crash), not silent-wrong.
    [Fact]
    public async Task IntCast_long_out_of_range_fails_loud_not_silent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewNumCtx(cn);
        long big = 4294967298L; // 2^32 + 2
        ctx.Add(new NumRow { Id = 1, D = 0, M = 0m, L = big, I = 0, B = false, S = "" });
        await ctx.SaveChangesAsync();
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<NumRow>().Where(x => x.Id == 1).Select(x => (int)x.L).ToList());
    }

    // A string-stored (converter) enum used inside a computed projection (.ToString()) throws a
    // clean NormUnsupportedFeatureException. Fail-loud, not silent-wrong.
    [Fact]
    public async Task StringStoredEnum_ToString_projection_fails_loud_not_silent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = NewStrEnumCtx(cn);
        ctx.Add(new StrEnumRow { Id = 1, Status = Status.Shipped });
        await ctx.SaveChangesAsync();
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<StrEnumRow>().Where(x => x.Id == 1).Select(x => x.Status.ToString()).ToList());
    }
}
