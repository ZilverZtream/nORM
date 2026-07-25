using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Core;
using Xunit;

namespace nORM.Tests.Fuzzing;

/// <summary>
/// Runtime companion to <see cref="UnsupportedReasonContractTests"/>. The static Roslyn scan proves every
/// throw-site in src/nORM CARRIES a reason code; this proves a REAL rejection SURFACES that stable code at
/// runtime — so the fuzz differentials record the catalog code into the manifest, not a fragile message token
/// or the exception type name (which was the same string for every rejection). If someone reverts a
/// differential to <c>nufe.Message</c>-derived codes, or a throw-site loses its code, these guard it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class RuntimeReasonCodeTests
{
    private static ISet<string> CatalogCodeValues()
        => typeof(NormUnsupportedReason)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

    [Fact]
    public void A_real_unsupported_query_surfaces_its_stable_registered_reason_code()
    {
        using var ctx = QueryIrDifferential.CreateSeededContext(Array.Empty<IrRow>());

        // IgnoreCase string.Replace has no portable case-insensitive substring rewrite on SQLite, so nORM
        // fail-louds rather than silently emitting a case-sensitive REPLACE — a deterministic rejection.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<IrRow>()
               .Where(r => r.Name.Replace("a", "b", StringComparison.OrdinalIgnoreCase) == "x")
               .ToList());

        Assert.Equal(NormUnsupportedReason.StringReplaceIgnoreCaseNotMobile, ex.ReasonCode);
        Assert.Contains(ex.ReasonCode!, CatalogCodeValues());
    }

    [Fact]
    public void A_rejection_reason_code_is_a_kebab_catalog_value_never_a_type_name_or_message_token()
    {
        // The differentials assign `ReasonCode = nufe.ReasonCode`; this pins the SHAPE of that value so a
        // regression back to `nufe.GetType().Name` (always the type name) or a `nufe.Message` token is caught.
        using var ctx = QueryIrDifferential.CreateSeededContext(Array.Empty<IrRow>());
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<IrRow>()
               .Select(r => r.Name.Replace("a", "b", StringComparison.OrdinalIgnoreCase))
               .ToList());

        // The reason code is a stable kebab-case catalog value, never the exception type name or a message token.
        Assert.NotNull(ex.ReasonCode);
        Assert.Matches("^[a-z0-9]+(-[a-z0-9]+)*$", ex.ReasonCode);
        Assert.NotEqual(nameof(NormUnsupportedFeatureException), ex.ReasonCode);
        Assert.Contains(ex.ReasonCode!, CatalogCodeValues());
    }
}
