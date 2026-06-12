using System;
using System.Collections.Generic;
using nORM.Configuration;
using nORM.Migration;

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
