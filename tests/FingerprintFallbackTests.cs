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
}
