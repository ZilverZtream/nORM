using System;
using System.Linq;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
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
}
