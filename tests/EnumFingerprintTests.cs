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
/// QP/PC-1: Verifies that LINQ plan fingerprinting handles enums backed by types wider than int32
/// without throwing OverflowException.
/// Root cause: AppendStableValue used Convert.ToInt32 for all enums, which overflows for
/// long/ulong-backed enums with values outside Int32 range.
/// </summary>
public class EnumFingerprintTests
{
    // ── Wide-underlying-type enums ────────────────────────────────────────────

    private enum IntEnum { A = 1, B = 2, C = int.MaxValue }
    private enum LongEnum : long { Small = 1L, Huge = 5_000_000_000L, MaxVal = long.MaxValue - 1 }
    private enum ULongEnum : ulong { Small = 1UL, Huge = 10_000_000_000UL }
    private enum ShortEnum : short { A = 1, B = short.MaxValue }
    private enum ByteEnum : byte { A = 0, B = byte.MaxValue }
    private enum SByteEnum : sbyte { A = -1, B = sbyte.MaxValue }
    private enum UShortEnum : ushort { A = 0, B = ushort.MaxValue }
    private enum UIntEnum : uint { A = 0, B = uint.MaxValue }

    // ── Entity: 'Tag' is a long so we can filter with (long)LongEnum.Huge ────

    [Table("EfpItem")]
    private class EfpItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public long Tag { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE EfpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Tag INTEGER NOT NULL DEFAULT 0)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── QP/PC-1 tests — wide enum constants in expression tree ───────────────

    /// <summary>
    /// QP/PC-1: A long-backed enum constant (5 billion) cast to long in a lambda
    /// must fingerprint without OverflowException.
    /// The cast `(long)LongEnum.Huge` injects a ConstantExpression{LongEnum} into the
    /// expression tree; AppendStableValue must use AppendLong, not Convert.ToInt32.
    /// </summary>
    [Fact]
    public void Fingerprint_LongEnumConstant_OutsideInt32Range_DoesNotThrow()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        // QP/PC-1: `(long)LongEnum.Huge` creates Convert(Constant(LongEnum.Huge), long)
        // in the expression tree. AppendStableValue is called with the enum value.
        var ex = Record.Exception(() =>
            ctx.Query<EfpItem>()
               .Where(e => e.Tag == (long)LongEnum.Huge)
               .ToList());

        Assert.Null(ex);
    }

    /// <summary>
    /// QP/PC-1: int.MaxValue enum constant (max value for int) — baseline must work.
    /// </summary>
    [Fact]
    public void Fingerprint_IntEnum_MaxValue_DoesNotThrow()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        var ex = Record.Exception(() =>
            ctx.Query<EfpItem>()
               .Where(e => e.Id == (int)IntEnum.C)
               .ToList());

        Assert.Null(ex);
    }

    /// <summary>
    /// QP/PC-1: uint.MaxValue (4294967295) cast to long — must not throw.
    /// </summary>
    [Fact]
    public void Fingerprint_UIntEnum_MaxValue_DoesNotThrow()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        var ex = Record.Exception(() =>
            ctx.Query<EfpItem>()
               .Where(e => e.Tag == (long)UIntEnum.B)
               .ToList());

        Assert.Null(ex);
    }

    /// <summary>
    /// QP/PC-1: Two queries with different wide enum constants must produce distinct plan
    /// cache entries and return the correct (distinct) rows.
    /// </summary>
    [Fact]
    public async Task Fingerprint_DifferentLongEnumValues_ProduceDifferentPlans()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        await ctx.InsertAsync(new EfpItem { Name = "small", Tag = (long)LongEnum.Small });
        await ctx.InsertAsync(new EfpItem { Name = "huge", Tag = (long)LongEnum.Huge });

        // Each query must find only its own matching row — proves distinct plans.
        var r1 = ctx.Query<EfpItem>().Where(e => e.Tag == (long)LongEnum.Small).ToList();
        var r2 = ctx.Query<EfpItem>().Where(e => e.Tag == (long)LongEnum.Huge).ToList();

        Assert.Single(r1);
        Assert.Single(r2);
        Assert.Equal("small", r1[0].Name);
        Assert.Equal("huge", r2[0].Name);
    }

    /// <summary>
    /// QP/PC-1: short, byte, sbyte, ushort-backed enum constants in lambdas must not throw.
    /// </summary>
    [Theory]
    [InlineData("byte")]
    [InlineData("sbyte")]
    [InlineData("short")]
    [InlineData("ushort")]
    public void Fingerprint_NarrowEnums_DoNotThrow(string enumType)
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        var ex = enumType switch
        {
            "byte"   => Record.Exception(() => ctx.Query<EfpItem>().Where(e => e.Tag == (long)ByteEnum.B).ToList()),
            "sbyte"  => Record.Exception(() => ctx.Query<EfpItem>().Where(e => e.Tag == (long)(sbyte)SByteEnum.B).ToList()),
            "short"  => Record.Exception(() => ctx.Query<EfpItem>().Where(e => e.Tag == (long)ShortEnum.B).ToList()),
            "ushort" => Record.Exception(() => ctx.Query<EfpItem>().Where(e => e.Tag == (long)UShortEnum.B).ToList()),
            _ => throw new ArgumentOutOfRangeException(nameof(enumType))
        };

        Assert.Null(ex);
    }

    /// <summary>
    /// QP/PC-1: A inline int enum constant in a lambda produces a ConstantExpression
    /// that AppendStableValue handles — works before and after the fix.
    /// </summary>
    [Fact]
    public void Fingerprint_InlineIntEnumLiteral_StillWorks()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        var ex = Record.Exception(() =>
            ctx.Query<EfpItem>()
               .Where(e => e.Tag == (long)IntEnum.B)
               .ToList());

        Assert.Null(ex);
    }
}
