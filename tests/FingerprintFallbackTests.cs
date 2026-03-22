using System;
using nORM.Query;
using System.Linq.Expressions;
using Xunit;

namespace nORM.Tests;

/// <summary>Verify AppendStableValue fallback produces collision-resistant fingerprints.</summary>
public class FingerprintFallbackTests
{
    // Helper: compute fingerprint for a constant expression containing the given value.
    private static ExpressionFingerprint FpOf(object value)
        => ExpressionFingerprint.Compute(Expression.Constant(value, value.GetType()));

    private enum ColorEnum { Red = 1, Green = 2 }
    private enum SizeEnum { Small = 1, Large = 2 }

    [Fact]
    public void DifferentEnumTypes_SameNumericValue_ProduceDifferentFingerprints()
    {
        // ColorEnum.Red == 1 and SizeEnum.Small == 1, but different types → different fingerprints.
        var fp1 = FpOf(ColorEnum.Red);
        var fp2 = FpOf(SizeEnum.Small);
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void SameEnumType_DifferentValues_ProduceDifferentFingerprints()
    {
        var fp1 = FpOf(ColorEnum.Red);
        var fp2 = FpOf(ColorEnum.Green);
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void SameEnumType_SameValue_ProduceSameFingerprint()
    {
        var fp1 = FpOf(ColorEnum.Red);
        var fp2 = FpOf(ColorEnum.Red);
        Assert.Equal(fp1, fp2);
    }

    private sealed class CustomValue
    {
        private readonly string _s;
        public CustomValue(string s) => _s = s;
        public override string ToString() => _s;
    }

    [Fact]
    public void CustomType_DifferentToString_ProduceDifferentFingerprints()
    {
        // Covers the fallback branch: not a primitive, not an enum.
        var fp1 = FpOf(new CustomValue("foo"));
        var fp2 = FpOf(new CustomValue("bar"));
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void CustomType_SameToString_ProduceSameFingerprint()
    {
        var fp1 = FpOf(new CustomValue("same"));
        var fp2 = FpOf(new CustomValue("same"));
        Assert.Equal(fp1, fp2);
    }

    // ── G1: DateOnly and TimeOnly stable fingerprint ──────────────────────────

    /// <summary>
    /// Two different DateOnly values must produce different fingerprints.
    /// Before the G1 fix: DateOnly fell through to the default branch which called
    /// ToString() — culture-dependent and potentially unstable across environments.
    /// After the fix: DateOnly uses DayNumber (a stable int) as its fingerprint.
    /// </summary>
    [Fact]
    public void DateOnly_DifferentDates_ProduceDifferentFingerprints()
    {
        var fp1 = FpOf(new DateOnly(2024, 7, 15));
        var fp2 = FpOf(new DateOnly(2024, 7, 16));
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void DateOnly_SameDate_ProduceSameFingerprint()
    {
        var fp1 = FpOf(new DateOnly(2024, 7, 15));
        var fp2 = FpOf(new DateOnly(2024, 7, 15));
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void DateOnly_DifferentFromDateTime_ProduceDifferentFingerprints()
    {
        // DateOnly and DateTime with the same "date" must get different fingerprints
        // because they are different types with different semantics.
        var fp1 = FpOf(new DateOnly(2024, 7, 15));
        var fp2 = FpOf(new DateTime(2024, 7, 15));
        Assert.NotEqual(fp1, fp2);
    }

    /// <summary>
    /// Two different TimeOnly values must produce different fingerprints.
    /// After the G1 fix: TimeOnly uses Ticks (a stable long) for its fingerprint.
    /// </summary>
    [Fact]
    public void TimeOnly_DifferentTimes_ProduceDifferentFingerprints()
    {
        var fp1 = FpOf(new TimeOnly(9, 0));
        var fp2 = FpOf(new TimeOnly(9, 1));
        Assert.NotEqual(fp1, fp2);
    }

    [Fact]
    public void TimeOnly_SameTime_ProduceSameFingerprint()
    {
        var fp1 = FpOf(new TimeOnly(14, 30, 45));
        var fp2 = FpOf(new TimeOnly(14, 30, 45));
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void TimeOnly_DifferentFromTimeSpan_ProduceDifferentFingerprints()
    {
        // TimeOnly and TimeSpan with equivalent durations must get different fingerprints.
        var fp1 = FpOf(new TimeOnly(9, 30));
        var fp2 = FpOf(new TimeSpan(9, 30, 0));
        Assert.NotEqual(fp1, fp2);
    }
}
