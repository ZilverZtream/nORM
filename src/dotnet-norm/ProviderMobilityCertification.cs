using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using nORM.Configuration;
using nORM.Migration;
using nORM.Providers;

namespace nORM.Cli;

public sealed record ProviderMobilityCertificationReport(
    string Contract,
    string Tool,
    string Profile,
    DateTime GeneratedUtc,
    string ScanPath,
    string Status,
    int ErrorCount,
    int WarningCount,
    int ScannedFiles,
    int SchemaTables,
    IReadOnlyList<ProviderMobilityProviderTargetReport> ProviderTargets,
    IReadOnlyList<ProviderMobilityFinding> Findings,
    IReadOnlyList<ProviderMobilityRecommendation> Recommendations);

public sealed record ProviderMobilityProviderTargetReport(
    string Provider,
    string? MinimumServerVersion,
    IReadOnlyList<ProviderMobilityProviderDecision> Decisions);

public sealed record ProviderMobilityFinding(
    string Path,
    int Line,
    string Kind,
    string Severity,
    string Reason,
    string SuggestedFix);

public sealed record ProviderMobilityRecommendation(
    string Kind,
    int Count,
    string Priority,
    string SuggestedFix);

public sealed class ProviderMobilityCertificationOptions
{
    public required string ScanPath { get; init; }
    public string Profile { get; init; } = "all-four";
    public string? JsonReportPath { get; init; }
    public string? HtmlReportPath { get; init; }
    public SchemaSnapshot? SchemaSnapshot { get; init; }
    public string? SchemaSnapshotPath { get; init; }
    public IReadOnlyList<string>? TargetProviders { get; init; }
    public IReadOnlyDictionary<string, Version?>? TargetProviderVersions { get; init; }
    public IReadOnlyList<ProviderMobilityFinding>? TargetProviderFindings { get; init; }
}

public static class ProviderMobilityCertificationRunner
{
    public static ProviderMobilityCertificationReport Run(ProviderMobilityCertificationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ScanPath))
            throw new ArgumentException("A scan path is required.", nameof(options));

        var scanResult = ProviderMobilitySourceScanner.Scan(options.ScanPath);
        var (loadedSnapshot, snapshotLoadFindings) = ReadSchemaSnapshot(options.SchemaSnapshotPath);
        var schemaSnapshot = options.SchemaSnapshot ?? loadedSnapshot;
        var inspectedSchemaFindings = schemaSnapshot is null
            ? Array.Empty<ProviderMobilityFinding>()
            : ProviderMobilitySchemaInspector.Inspect(schemaSnapshot);
        var schemaFindings = snapshotLoadFindings.Concat(inspectedSchemaFindings).ToArray();
        var providerTargets = BuildProviderTargets(
            options.TargetProviders ?? ProviderTargetsForProfile(options.Profile),
            options.TargetProviderVersions,
            out var providerTargetFindings);
        var findings = scanResult.Findings
            .Concat(schemaFindings)
            .Concat(options.TargetProviderFindings ?? Array.Empty<ProviderMobilityFinding>())
            .Concat(providerTargetFindings)
            .ToArray();
        findings = findings
            .Concat(BuildUnclassifiedFindingGuards(findings))
            .ToArray();
        var providerTargetWarnings = providerTargets.Sum(static target => target.Decisions.Count(static decision =>
            decision.CertificationSeverity.Equals("Warning", StringComparison.OrdinalIgnoreCase)));
        var errors = findings.Count(static f => f.Severity.Equals("Error", StringComparison.OrdinalIgnoreCase));
        var warnings = findings.Count(static f => f.Severity.Equals("Warning", StringComparison.OrdinalIgnoreCase)) + providerTargetWarnings;
        var report = new ProviderMobilityCertificationReport(
            "nORM-provider-mobility-v1",
            "dotnet-norm",
            options.Profile,
            DateTime.UtcNow,
            scanResult.RootPath,
            errors == 0 ? "PASS" : "FAIL",
            errors,
            warnings,
            scanResult.ScannedFiles,
            schemaSnapshot?.Tables.Count ?? 0,
            providerTargets,
            findings,
            BuildRecommendations(findings, providerTargets));

        if (!string.IsNullOrWhiteSpace(options.JsonReportPath))
            WriteJson(options.JsonReportPath!, report);
        if (!string.IsNullOrWhiteSpace(options.HtmlReportPath))
            WriteHtml(options.HtmlReportPath!, report);

        return report;
    }

    private static (SchemaSnapshot? Snapshot, IReadOnlyList<ProviderMobilityFinding> Findings) ReadSchemaSnapshot(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return (null, Array.Empty<ProviderMobilityFinding>());

        var fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath))
        {
            return (null, new[]
            {
                new ProviderMobilityFinding(
                    fullPath,
                    0,
                    "schema-snapshot-missing",
                    "Error",
                    "The requested nORM schema snapshot path does not exist.",
                    "Pass a valid schema.snapshot.json path or use --assembly so the CLI can build the snapshot from the design-time DbContext.")
            });
        }

        try
        {
            return (JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(fullPath))
                ?? new SchemaSnapshot(), Array.Empty<ProviderMobilityFinding>());
        }
        catch (JsonException ex)
        {
            return (null, new[]
            {
                new ProviderMobilityFinding(
                    fullPath,
                    0,
                    "schema-snapshot-invalid-json",
                    "Error",
                    "The requested nORM schema snapshot is not valid JSON: " + ex.Message,
                    "Regenerate the snapshot through nORM migrations or pass the application assembly with --assembly.")
            });
        }
    }

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

    private static IReadOnlyList<ProviderMobilityProviderTargetReport> BuildProviderTargets(
        IReadOnlyList<string> providerNames,
        IReadOnlyDictionary<string, Version?>? actualVersions,
        out IReadOnlyList<ProviderMobilityFinding> findings)
    {
        var targets = new List<ProviderMobilityProviderTargetReport>();
        var targetFindings = new List<ProviderMobilityFinding>();
        var normalizedActualVersions = actualVersions?
            .GroupBy(static pair => NormalizeProviderName(pair.Key), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(static group => group.Key, static group => group.First().Value, StringComparer.OrdinalIgnoreCase);
        foreach (var providerName in providerNames
            .Select(static providerName => NormalizeProviderName(providerName))
            .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var provider = CreateProviderDescriptor(providerName);
            if (provider == null)
            {
                targetFindings.Add(new ProviderMobilityFinding(
                    "provider-target",
                    0,
                    "provider-target-unknown",
                    "Error",
                    $"Provider target '{providerName}' is not recognized by nORM provider mobility certification.",
                    "Use one of sqlite, sqlserver, postgres, or mysql, or add an explicit provider target descriptor before certification."));
                continue;
            }

            var capabilities = provider.Capabilities;
            Version? actualVersion = null;
            var hasActualVersionEvidence = normalizedActualVersions != null &&
                normalizedActualVersions.TryGetValue(providerName, out actualVersion);
            targets.Add(new ProviderMobilityProviderTargetReport(
                capabilities.ProviderName,
                capabilities.MinimumServerVersion?.ToString(),
                ProviderMobilityTranslator.DecideProviderImplementationProfile(
                    provider,
                    hasActualVersionEvidence ? actualVersion : null,
                    requireActualServerVersion: hasActualVersionEvidence)));
            foreach (var decision in targets[^1].Decisions)
            {
                if (!decision.CertificationSeverity.Equals("Error", StringComparison.OrdinalIgnoreCase))
                    continue;

                targetFindings.Add(new ProviderMobilityFinding(
                    "provider-target",
                    0,
                    "provider-target-capability",
                    "Error",
                    decision.Reason,
                    decision.SuggestedFix));
            }
        }

        findings = targetFindings;
        return targets;
    }

    private static IReadOnlyList<string> ProviderTargetsForProfile(string profile)
    {
        if (string.IsNullOrWhiteSpace(profile) || profile.Equals("all-four", StringComparison.OrdinalIgnoreCase))
            return new[] { "sqlite", "sqlserver", "postgres", "mysql" };

        return profile
            .Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static p => p.ToLowerInvariant())
            .ToArray();
    }

    private static DatabaseProvider? CreateProviderDescriptor(string providerName)
        => providerName.ToLowerInvariant() switch
        {
            "sqlite" => new SqliteProvider(),
            "sqlserver" or "mssql" => new SqlServerProvider(),
            "postgres" or "postgresql" => new PostgresProvider(),
            "mysql" or "mariadb" => new MySqlProvider(),
            _ => null
        };

    private static string NormalizeProviderName(string providerName)
        => ProviderNameNormalizer.Normalize(providerName);

    private static void WriteJson(string reportPath, ProviderMobilityCertificationReport report)
    {
        var fullPath = Path.GetFullPath(reportPath);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var options = new JsonSerializerOptions { WriteIndented = true };
        options.Converters.Add(new JsonStringEnumConverter());
        File.WriteAllText(fullPath, JsonSerializer.Serialize(report, options));
    }

    private static void WriteHtml(string reportPath, ProviderMobilityCertificationReport report)
    {
        var fullPath = Path.GetFullPath(reportPath);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var statusClass = report.Status == "PASS" ? "pass" : "fail";
        var builder = new StringBuilder();
        builder.AppendLine("<!doctype html>");
        builder.AppendLine("<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
        builder.AppendLine("<title>nORM Provider Mobility Certification</title>");
        builder.AppendLine("<style>");
        builder.AppendLine("body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f7f8fa;color:#1f2933}main{max-width:1120px;margin:0 auto;padding:32px}h1{font-size:28px;margin:0 0 6px}.meta{color:#52606d}.summary{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:24px 0}.tile{background:#fff;border:1px solid #dde3ea;border-radius:8px;padding:16px}.tile b{display:block;font-size:24px}.pass{color:#087f5b}.fail{color:#c92a2a}table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #dde3ea;border-radius:8px;overflow:hidden}th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e6ebf1;vertical-align:top}th{background:#eef2f6;font-size:13px}code{background:#eef2f6;padding:2px 4px;border-radius:4px}.empty{background:#fff;border:1px solid #dde3ea;border-radius:8px;padding:20px}@media(max-width:760px){main{padding:20px}.summary{grid-template-columns:repeat(2,minmax(0,1fr))}}");
        builder.AppendLine("</style></head><body><main>");
        builder.AppendLine("<h1>nORM Provider Mobility Certification</h1>");
        builder.Append("<div class=\"meta\">").Append(Html(report.Contract)).Append(" | profile ").Append(Html(report.Profile)).Append(" | ").Append(Html(report.GeneratedUtc.ToString("O"))).AppendLine("</div>");
        builder.Append("<section class=\"summary\"><div class=\"tile\"><span>Status</span><b class=\"").Append(statusClass).Append("\">").Append(Html(report.Status)).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Errors</span><b>").Append(report.ErrorCount).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Warnings</span><b>").Append(report.WarningCount).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Files</span><b>").Append(report.ScannedFiles).Append("</b></div>");
        builder.Append("<div class=\"tile\"><span>Schema tables</span><b>").Append(report.SchemaTables).Append("</b></div></section>");
        builder.Append("<p class=\"meta\">Scan path: <code>").Append(Html(report.ScanPath)).AppendLine("</code></p>");

        if (report.ProviderTargets.Count > 0)
        {
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

        if (report.Recommendations.Count > 0)
        {
            builder.AppendLine("<h2>Recommended Fix Order</h2><table><thead><tr><th>Priority</th><th>Finding</th><th>Count</th><th>Suggested fix</th></tr></thead><tbody>");
            foreach (var recommendation in report.Recommendations)
            {
                builder.Append("<tr><td>").Append(Html(recommendation.Priority)).Append("</td><td>").Append(Html(recommendation.Kind)).Append("</td><td>").Append(recommendation.Count).Append("</td><td>").Append(Html(recommendation.SuggestedFix)).AppendLine("</td></tr>");
            }
            builder.AppendLine("</tbody></table>");
        }

        builder.AppendLine("<h2>Findings</h2>");
        if (report.Findings.Count == 0)
        {
            builder.AppendLine("<div class=\"empty\">No provider-bound source findings were detected.</div>");
        }
        else
        {
            builder.AppendLine("<table><thead><tr><th>Severity</th><th>Kind</th><th>Location</th><th>Reason</th><th>Suggested fix</th></tr></thead><tbody>");
            foreach (var finding in report.Findings)
            {
                builder.Append("<tr><td>").Append(Html(finding.Severity)).Append("</td><td>").Append(Html(finding.Kind)).Append("</td><td><code>")
                    .Append(Html(finding.Path)).Append(':').Append(finding.Line).Append("</code></td><td>")
                    .Append(Html(finding.Reason)).Append("</td><td>").Append(Html(finding.SuggestedFix)).AppendLine("</td></tr>");
            }
            builder.AppendLine("</tbody></table>");
        }

        builder.AppendLine("</main></body></html>");
        File.WriteAllText(fullPath, builder.ToString());
    }

    private static string Html(string value) => WebUtility.HtmlEncode(value);
}
