using System.Text;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
{
    private static void AppendProviderTargets(StringBuilder builder, ProviderMobilityCertificationReport report)
    {
        if (report.ProviderTargets.Count == 0)
            return;

        builder.AppendLine("<h2>Provider Targets</h2><table><thead><tr><th>Provider</th><th>Feature</th><th>Support</th><th>Severity</th><th>Reason</th><th>Suggested fix</th></tr></thead><tbody>");
        foreach (var target in report.ProviderTargets)
        {
            foreach (var decision in target.Decisions)
            {
                builder.Append("<tr><td>").Append(Html(target.Provider)).Append("</td><td>").Append(Html(decision.Feature.ToString())).Append("</td><td>")
                    .Append(Html(decision.Support.ToString())).Append("</td><td>").Append(Html(decision.CertificationSeverity)).Append("</td><td>")
                    .Append(Html(decision.Reason)).Append("</td><td>").Append(Html(decision.SuggestedFix)).AppendLine("</td></tr>");
            }
        }
        builder.AppendLine("</tbody></table>");
    }

    private static void AppendRecommendations(StringBuilder builder, ProviderMobilityCertificationReport report)
    {
        if (report.Recommendations.Count == 0)
            return;

        builder.AppendLine("<h2>Recommended Fix Order</h2><table><thead><tr><th>Priority</th><th>Finding</th><th>Count</th><th>Suggested fix</th></tr></thead><tbody>");
        foreach (var recommendation in report.Recommendations)
        {
            builder.Append("<tr><td>").Append(Html(recommendation.Priority)).Append("</td><td>").Append(Html(recommendation.Kind)).Append("</td><td>").Append(recommendation.Count).Append("</td><td>").Append(Html(recommendation.SuggestedFix)).AppendLine("</td></tr>");
        }
        builder.AppendLine("</tbody></table>");
    }

    private static void AppendFindings(StringBuilder builder, ProviderMobilityCertificationReport report)
    {
        builder.AppendLine("<h2>Findings</h2>");
        if (report.Findings.Count == 0)
        {
            builder.AppendLine("<div class=\"empty\">No provider-bound source findings were detected.</div>");
            return;
        }

        builder.AppendLine("<table><thead><tr><th>Severity</th><th>Kind</th><th>Location</th><th>Reason</th><th>Suggested fix</th></tr></thead><tbody>");
        foreach (var finding in report.Findings)
        {
            builder.Append("<tr><td>").Append(Html(finding.Severity)).Append("</td><td>").Append(Html(finding.Kind)).Append("</td><td><code>")
                .Append(Html(finding.Path)).Append(':').Append(finding.Line).Append("</code></td><td>")
                .Append(Html(finding.Reason)).Append("</td><td>").Append(Html(finding.SuggestedFix)).AppendLine("</td></tr>");
        }
        builder.AppendLine("</tbody></table>");
    }
}
