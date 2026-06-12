using System;
using System.Collections.Generic;
using System.IO;
using nORM.Configuration;

namespace nORM.Cli;

public static partial class ProviderMobilitySourceScanner
{
    private static void AddFinding(
        string root,
        string file,
        int line,
        (string Pattern, string Kind, string SuggestedFix) rule,
        List<ProviderMobilityFinding> findings)
    {
        var reason = $"Provider-bound usage '{rule.Pattern}' is outside strict provider mobility certification.";
        var suggestedFix = rule.SuggestedFix;
        var severity = SeverityFor(rule.Kind);
        if (ProviderMobilityTranslator.TryDecideFindingKind(rule.Kind, out var decision))
        {
            reason = decision.Reason;
            suggestedFix = rule.SuggestedFix.Equals(decision.SuggestedFix, StringComparison.Ordinal)
                ? decision.SuggestedFix
                : decision.SuggestedFix + " Pattern-specific remediation: " + rule.SuggestedFix;
            severity = decision.CertificationSeverity;
        }

        findings.Add(new ProviderMobilityFinding(
            Path.GetRelativePath(root, file),
            line,
            rule.Kind,
            severity,
            reason,
            suggestedFix));
    }

    private static string SeverityFor(string kind)
        => kind is "provider-specific-package" or "provider-bootstrap-connection" or "client-projection-tail"
            ? "Warning"
            : "Error";
}
