using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
{
    private static IReadOnlyList<ProviderMobilityRecommendation> BuildRecommendations(
        IReadOnlyList<ProviderMobilityFinding> findings,
        IReadOnlyList<ProviderMobilityProviderTargetReport> providerTargets)
    {
        var targetRecommendations = providerTargets
            .SelectMany(static target => target.Decisions)
            .Where(static decision => !decision.CertificationSeverity.Equals("Info", StringComparison.OrdinalIgnoreCase))
            .Select(static decision => new ProviderMobilityFinding(
                "provider-target",
                0,
                "provider-target-" + decision.Feature,
                decision.CertificationSeverity,
                decision.Reason,
                decision.SuggestedFix));

        return findings
            .Concat(targetRecommendations)
            .GroupBy(static finding => finding.Kind)
            .Select(static group =>
            {
                var first = group.First();
                var count = group.Count();
                var hasError = group.Any(static finding => finding.Severity.Equals("Error", StringComparison.OrdinalIgnoreCase));
                var priority = hasError && count >= 3
                    ? "P0"
                    : hasError ? "P1" : "P2";
                var suggestedFix = string.Join(" Also: ", group
                    .Select(static finding => finding.SuggestedFix)
                    .Distinct(StringComparer.Ordinal));
                return new ProviderMobilityRecommendation(first.Kind, count, priority, suggestedFix);
            })
            .OrderBy(static recommendation => recommendation.Priority)
            .ThenByDescending(static recommendation => recommendation.Count)
            .ThenBy(static recommendation => recommendation.Kind, StringComparer.Ordinal)
            .ToArray();
    }

    private static IReadOnlyList<ProviderMobilityFinding> BuildUnclassifiedFindingGuards(IReadOnlyList<ProviderMobilityFinding> findings)
        => findings
            .Where(static finding => !ProviderMobilityTranslator.TryDecideFindingKind(finding.Kind, out _))
            .Select(static finding =>
            {
                var decision = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.CertificationUnclassifiedFinding);
                return new ProviderMobilityFinding(
                    finding.Path,
                    finding.Line,
                    "certification-unclassified-finding",
                    decision.CertificationSeverity,
                    $"{decision.Reason} Unclassified kind: '{finding.Kind}'.",
                    decision.SuggestedFix);
            })
            .ToArray();
}
