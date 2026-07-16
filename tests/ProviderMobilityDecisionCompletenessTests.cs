using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the strict-mode admit/deny contract against the FULL ProviderMobilityFeature enum:
/// every member has exactly one classification decision, the strict-runtime ADMIT set is the
/// exact reviewed list (a new enum member or a flipped decision must break this test so the
/// change is deliberate), and the decisions obey the coherence invariants the certification
/// layer relies on (denied implies Error severity, Unsupported is never admitted, every
/// decision carries a reason and a suggested fix).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProviderMobilityDecisionCompletenessTests
{
    private static IReadOnlyList<ProviderMobilityFeature> AllFeatures =>
        Enum.GetValues<ProviderMobilityFeature>();

    [Fact]
    public void Every_feature_has_exactly_one_decision()
    {
        foreach (var feature in AllFeatures)
        {
            var decision = ProviderMobilityTranslator.Decide(feature);
            Assert.Equal(feature, decision.Feature);
        }
    }

    [Fact]
    public void The_strict_runtime_admit_set_is_the_reviewed_list()
    {
        var admitted = AllFeatures
            .Where(f => ProviderMobilityTranslator.Decide(f).StrictRuntimeAllowed)
            .OrderBy(f => f.ToString(), StringComparer.Ordinal)
            .ToArray();

        var reviewed = new[]
        {
            ProviderMobilityFeature.ConstrainedLinqShape,
            ProviderMobilityFeature.ExplicitClientProjectionTail,
            ProviderMobilityFeature.GeneratedLinqQuery,
            ProviderMobilityFeature.GeneratedTenantBoundary,
            ProviderMobilityFeature.GeneratedWrite,
            ProviderMobilityFeature.NormManagedTemporal,
            ProviderMobilityFeature.ProviderBootstrap,
            ProviderMobilityFeature.SchemaDefaultNeedsReview,
            ProviderMobilityFeature.SchemaMissingPrimaryKey,
            ProviderMobilityFeature.WrappedSavepoint,
        };

        Assert.Equal(reviewed, admitted);
    }

    [Fact]
    public void Denied_features_carry_error_severity_and_admitted_features_do_not()
    {
        foreach (var feature in AllFeatures)
        {
            var decision = ProviderMobilityTranslator.Decide(feature);
            if (decision.StrictRuntimeAllowed)
                Assert.True(decision.CertificationSeverity is "Info" or "Warning",
                    $"{feature}: admitted but severity is '{decision.CertificationSeverity}'");
            else
                Assert.True(decision.CertificationSeverity == "Error",
                    $"{feature}: denied but severity is '{decision.CertificationSeverity}'");
        }
    }

    [Fact]
    public void Unsupported_features_are_never_admitted_and_portable_features_always_are()
    {
        foreach (var feature in AllFeatures)
        {
            var decision = ProviderMobilityTranslator.Decide(feature);
            if (decision.Support == ProviderMobilitySupport.Unsupported)
                Assert.False(decision.StrictRuntimeAllowed, $"{feature}: Unsupported yet admitted");
            if (decision.Support is ProviderMobilitySupport.Portable or ProviderMobilitySupport.Emulated)
                Assert.True(decision.StrictRuntimeAllowed, $"{feature}: {decision.Support} yet denied");
        }
    }

    [Fact]
    public void Every_decision_explains_itself()
    {
        foreach (var feature in AllFeatures)
        {
            var decision = ProviderMobilityTranslator.Decide(feature);
            Assert.False(string.IsNullOrWhiteSpace(decision.Reason), $"{feature}: empty reason");
            Assert.False(string.IsNullOrWhiteSpace(decision.SuggestedFix), $"{feature}: empty suggested fix");
        }
    }
}
