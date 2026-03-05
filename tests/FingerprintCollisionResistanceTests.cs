using System;
using System.Linq.Expressions;
using System.Reflection;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// QP-1: Verifies that ExpressionFingerprint uses full-value bytes instead of
/// GetHashCode() for constant values so that distinct constants always produce
/// distinct fingerprints (no 32-bit truncation collisions for strings/longs/doubles).
/// </summary>
public class FingerprintCollisionResistanceTests
{
    // ExpressionFingerprint is internal — access via reflection.
    private static readonly Type _fpType =
        typeof(nORM.Query.NormQueryProvider).Assembly.GetType("nORM.Query.ExpressionFingerprint")!;

    private static readonly MethodInfo _compute =
        _fpType.GetMethod("Compute", BindingFlags.Public | BindingFlags.Static)!;

    private static object Compute(Expression expr) => _compute.Invoke(null, new object[] { expr })!;

    // ─── QP-1: Different string constants → different fingerprints ────────────

    [Fact]
    public void DifferentStringConstants_ProduceDifferentFingerprints()
    {
        // Two ConstantExpressions with different strings must produce distinct fingerprints.
        // GetHashCode() on .NET strings is unstable across runs but at runtime two
        // identical runs that differ only in value should differ.
        var e1 = Expression.Constant("Alice");
        var e2 = Expression.Constant("Bob");

        var fp1 = Compute(e1);
        var fp2 = Compute(e2);

        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void SameStringConstant_ProducesSameFingerprint()
    {
        // Non-regression: same string → same fingerprint.
        var e1 = Expression.Constant("Alice");
        var e2 = Expression.Constant("Alice");

        Assert.Equal(Compute(e1), Compute(e2));
    }

    // ─── QP-1: Longs differing only in high 32 bits → different fingerprints ──

    [Fact]
    public void LongsWithSameGetHashCode_ProduceDifferentFingerprints()
    {
        // Before fix: AppendInt(n.GetHashCode()) on a long truncates to 32 bits.
        // Two longs whose XOR of high/low halves produces the same 32-bit hash
        // would produce the same fingerprint despite being different values.
        // After fix: AppendLong writes all 8 bytes → always distinct.
        //
        // 0x0000_0001_0000_0000L and 0x0000_0000_0000_0001L:
        //   long.GetHashCode() = (int)(v ^ (v >> 32)), which gives:
        //   v1 = 0x0000_0001_0000_0000 → (0 ^ 1) = 1
        //   v2 = 0x0000_0000_0000_0001 → (1 ^ 0) = 1  (same hash!)
        long v1 = 0x0000_0001_0000_0000L;
        long v2 = 0x0000_0000_0000_0001L;

        var e1 = Expression.Constant(v1);
        var e2 = Expression.Constant(v2);

        Assert.NotEqual(Compute(e1), Compute(e2));
    }

    // ─── QP-1: Doubles → different fingerprints ───────────────────────────────

    [Fact]
    public void DoublesWithSameInt32Truncation_ProduceDifferentFingerprints()
    {
        // Before fix: AppendInt(d.GetHashCode()) — double.GetHashCode() maps via
        // BitConverter.DoubleToInt64Bits then XORs the halves, losing information.
        // After fix: AppendLong(BitConverter.DoubleToInt64Bits(d)) uses all 64 bits.
        //
        // 1.0 and 2.0 have different bit patterns and must produce distinct fingerprints.
        var e1 = Expression.Constant(1.0);
        var e2 = Expression.Constant(2.0);

        Assert.NotEqual(Compute(e1), Compute(e2));
    }

    // ─── QP-1: Guid constants → different fingerprints ────────────────────────

    [Fact]
    public void DifferentGuidConstants_ProduceDifferentFingerprints()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();

        var e1 = Expression.Constant(g1);
        var e2 = Expression.Constant(g2);

        Assert.NotEqual(Compute(e1), Compute(e2));
    }

    [Fact]
    public void SameGuidConstant_ProducesSameFingerprint()
    {
        var g = Guid.Parse("12345678-1234-1234-1234-123456789abc");
        var e1 = Expression.Constant(g);
        var e2 = Expression.Constant(g);

        Assert.Equal(Compute(e1), Compute(e2));
    }

    // ─── QP-1: Decimal constants → different fingerprints ─────────────────────

    [Fact]
    public void DifferentDecimalConstants_ProduceDifferentFingerprints()
    {
        var e1 = Expression.Constant(1.23m);
        var e2 = Expression.Constant(4.56m);

        Assert.NotEqual(Compute(e1), Compute(e2));
    }
}
