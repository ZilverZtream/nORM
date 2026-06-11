using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml;
using System.Xml.Linq;
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

public sealed record ProviderMobilityScanResult(string RootPath, int ScannedFiles, IReadOnlyList<ProviderMobilityFinding> Findings);

public static class ProviderMobilitySourceScanner
{
    private static readonly (string Pattern, string Kind, string SuggestedFix)[] CSharpRules =
    {
        ("FromSqlRawAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("FromSqlInterpolatedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("FromSqlRaw(", "raw-sql", "Replace EF Core raw SQL queries with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound migration finding."),
        ("FromSqlInterpolated(", "raw-sql", "Replace EF Core raw SQL queries with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound migration finding."),
        ("ExecuteSqlRaw(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSqlInterpolated(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSql(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("ExecuteSqlAsync(", "raw-sql", "Replace EF Core raw SQL commands with generated nORM write/query APIs when possible, or keep as explicit provider-bound migration work."),
        ("migrationBuilder.Sql(", "raw-sql", "Inventory hand-authored EF migration SQL and replace portable schema changes with nORM migrations; keep remaining SQL as explicit per-provider migration work."),
        ("using Dapper", "raw-sql", "Inventory Dapper SQL and rewrite provider-mobile paths to generated nORM LINQ/write APIs; keep remaining SQL as provider-bound migration work."),
        ("SqlMapper.", "raw-sql", "Inventory Dapper SQL and rewrite provider-mobile paths to generated nORM LINQ/write APIs; keep remaining SQL as provider-bound migration work."),
        ("QueryUnchangedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("QueryUnchangedInterpolatedAsync", "raw-sql", "Replace with generated nORM LINQ/query APIs when the shape is known; otherwise keep as an explicit provider-bound path outside strict certification."),
        ("ExecuteStoredProcedure", "stored-procedure", "Move data access semantics to generated nORM LINQ/write APIs where possible. If the procedure contains business logic, flag it for human rewrite or provider-specific deployment."),
        ("SqlFunction", "custom-sql-function", "Replace custom SQL fragments with built-in nORM/provider translations or keep them outside strict certification with per-provider evidence."),
        ("Regex.IsMatch(", "constrained-linq-shape", "Regex.IsMatch has an all-four simple subset (literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w); Regex.Replace has an all-four literal-pattern/literal-replacement subset. Keep regex LINQ inside the documented subset or add provider-owned live evidence."),
        ("Regex.Replace(", "constrained-linq-shape", "Regex.IsMatch has an all-four simple subset (literal text, ^/$ anchors, simple ASCII bracket classes, \\d, \\w); Regex.Replace has an all-four literal-pattern/literal-replacement subset. Keep regex LINQ inside the documented subset or add provider-owned live evidence."),
        (".TakeWhile(", "constrained-linq-shape", "TakeWhile is provider-mobile only with an explicit OrderBy/ThenBy sequence and the one-argument or index-aware predicate overload. Unordered and post-Take/Skip forms fail closed."),
        (".SkipWhile(", "constrained-linq-shape", "SkipWhile is provider-mobile only with an explicit OrderBy/ThenBy sequence and the one-argument or index-aware predicate overload. Unordered and post-Take/Skip forms fail closed."),
        (".SequenceEqual(", "constrained-linq-shape", "SequenceEqual is provider-mobile when queryable sources are explicitly ordered and the default equality comparer is used; local mapped-entity second sequences are parameterized as ordered derived tables. Unordered queryable sources and comparer overloads fail closed."),
        ("CompileTimeQuery", "compile-time-raw-sql", "Replace raw SQL [CompileTimeQuery] methods with Norm.CompileQuery LINQ where possible. Keep provider-specific SQL outside strict provider mobility certification."),
        ("CreateCompiledQueryCommandAsync", "compile-time-raw-sql", "Use Norm.CompileQuery LINQ for provider-mobile compiled queries. Direct command construction is a provider-bound escape hatch."),
        ("ctx.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("context.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("db.Connection", "direct-connection", "Use generated nORM APIs. Direct DbContext.Connection access is caller-owned provider language and cannot be certified as provider-mobile."),
        ("ctx.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("context.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("db.Provider", "direct-provider-access", "Use generated nORM APIs. Direct DatabaseProvider access is provider-specific and cannot be certified as provider-mobile."),
        ("ctx.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("context.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("db.Query(\"", "dynamic-table-query", "Use typed Query<T>() with mapped entities so nORM owns provider translation and schema semantics."),
        ("CurrentTransaction", "direct-transaction-access", "Use nORM's DbContextTransaction wrapper for generated nORM operations. Direct DbTransaction access is provider-specific ADO.NET surface."),
        (".Transaction", "direct-transaction-access", "Use nORM's transaction wrapper without exposing the raw DbTransaction handle in strict provider-mobile code."),
        ("DbTransaction", "direct-transaction-access", "Use nORM's transaction wrapper for generated nORM operations. Raw DbTransaction usage is provider-specific ADO.NET surface."),
        ("DbConnection", "provider-bootstrap-connection", "Keep provider selection and connection construction in composition-root infrastructure; strict-certified data access should use generated nORM APIs."),
        ("DbCommand", "direct-command-access", "Use generated nORM query/write APIs. Direct DbCommand usage is caller-authored provider language."),
        ("CreateCommand(", "direct-command-access", "Use generated nORM query/write APIs. Direct command construction is caller-authored provider language."),
        ("SqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("NpgsqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("MySqlCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("SqliteCommand", "direct-command-access", "Replace provider-specific command objects with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("SqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("NpgsqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("MySqlDataAdapter", "direct-command-access", "Replace provider-specific data adapters with generated nORM query/write APIs or keep the SQL as explicit provider-bound migration work."),
        ("IDbCommandInterceptor", "command-interceptor", "Keep command interception outside strict certification because it can inspect, rewrite, or suppress generated provider commands."),
        ("CommandInterceptors", "command-interceptor", "Keep command interception outside strict certification because it can inspect, rewrite, or suppress generated provider commands."),
        ("SqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("NpgsqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("MySqlConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("SqliteConnection", "provider-bootstrap-connection", "Keep provider selection in configuration/factory code. Generated repositories should receive a configured nORM DbContext, not concrete provider connections."),
        ("UseSqlServer(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseNpgsql(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseMySql(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("UseSqlite(", "provider-bootstrap-connection", "EF Core provider selection belongs in migration/bootstrap inventory. nORM-certified data access should use provider-neutral nORM configuration and generated APIs."),
        ("Microsoft.Data.SqlClient", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("Microsoft.Data.Sqlite", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("Npgsql", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("MySqlConnector", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("MySql.Data", "provider-specific-package", "Keep provider packages and concrete provider bootstrapping in configuration/infrastructure outside strict-certified data access."),
        ("EnableNativeTenantSessionContext", "provider-native-tenant-security", "Use generated-path tenant enforcement for provider mobility; keep native RLS/session context as explicit defense-in-depth deployment work."),
        ("TemporalStorageMode.ProviderNative", "provider-native-temporal", "Use nORM-managed temporal history for provider mobility; keep provider-native temporal mode as SQL Server-specific deployment work."),
        ("ProviderNative", "provider-native-temporal", "Use nORM-managed temporal history for provider mobility; keep provider-native temporal mode as provider-specific deployment work."),
        ("ClientEvaluationPolicy.Allow", "client-evaluation", "Translate the expression to supported nORM LINQ or switch to ClientEvaluationPolicy.Warn for explicit top-level projection tails after server filtering/paging."),
        ("ClientEvaluationPolicy.Warn", "client-projection-tail", "Keep this as an explicit, reviewed projection-tail choice: server filters, ordering and paging must run before the client projection."),
        ("TypeName =", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("TypeName=", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("HasColumnType", "provider-specific-column-type", "Use CLR type mapping and nORM migrations for provider-mobile schema. Provider-specific column type strings must stay outside strict certification."),
        ("HasDefaultValueSql", "schema-provider-specific-default", "Replace provider SQL defaults with application-stamped values, simple literals, or explicit provider-specific migrations outside strict certification."),
        ("HasComputedColumnSql", "schema-provider-specific-default", "Computed column SQL is provider language. Replace with generated/application logic or keep as provider-specific migration work."),
        ("HasIdentityOptions", "schema-provider-specific-default", "Identity seed/increment metadata is provider DDL. Keep it as reviewed provider-specific migration work outside strict certification."),
        ("UseCollation(", "schema-provider-specific-default", "Inventory provider-specific collation choices and replace them with provider-neutral comparison semantics or reviewed per-provider migration work."),
        ("HasCollation(", "schema-provider-specific-default", "Inventory provider-specific collation choices and replace them with provider-neutral comparison semantics or reviewed per-provider migration work."),
        ("Annotation(\"SqlServer:", "schema-provider-specific-default", "Inventory SQL Server-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("Annotation(\"Npgsql:", "schema-provider-specific-default", "Inventory PostgreSQL-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("Annotation(\"MySql:", "schema-provider-specific-default", "Inventory MySQL-specific EF migration annotations and replace them with provider-neutral nORM schema metadata where possible."),
        ("SqlServerValueGenerationStrategy", "schema-provider-specific-default", "Inventory SQL Server-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible."),
        ("NpgsqlValueGenerationStrategy", "schema-provider-specific-default", "Inventory PostgreSQL-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible."),
        ("MySqlValueGenerationStrategy", "schema-provider-specific-default", "Inventory MySQL-specific EF value generation annotations and replace them with provider-neutral nORM identity/schema metadata where possible.")
    };

    private static readonly (string Pattern, string Kind, string SuggestedFix)[] SqlRules =
    {
        ("CREATE PROCEDURE", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("CREATE PROC", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("ALTER PROCEDURE", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("ALTER PROC", "stored-procedure-definition", "Move portable data-access behavior to generated nORM APIs. If the procedure contains business logic, rewrite it explicitly for the target provider or keep it as a provider-specific deployment asset."),
        ("WITH (NOLOCK)", "sql-server-specific-sql", "Replace SQL Server locking hints with nORM/provider-neutral query semantics or document a provider-specific concurrency design."),
        ("GETDATE()", "sql-server-specific-sql", "Use nORM-generated temporal/date expressions or provider-neutral application timestamps where equivalent semantics are required."),
        ("DATEADD(", "sql-server-specific-sql", "Use supported nORM DateTime/TimeSpan LINQ translation where possible; otherwise rewrite per provider with explicit evidence."),
        ("FOR JSON", "sql-server-specific-sql", "Use application-side JSON projection or provider-specific reviewed SQL outside strict provider mobility certification.")
    };

    private static readonly (string Pattern, string Kind, string SuggestedFix)[] ProjectRules =
    {
        ("Dapper", "raw-sql", "Inventory Dapper package usage and rewrite provider-mobile data access to generated nORM LINQ/write APIs; keep remaining Dapper SQL outside strict certification."),
        ("Microsoft.EntityFrameworkCore.SqlServer", "provider-specific-package", "Inventory EF Core SQL Server provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Microsoft.EntityFrameworkCore.Sqlite", "provider-specific-package", "Inventory EF Core SQLite provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Npgsql.EntityFrameworkCore.PostgreSQL", "provider-specific-package", "Inventory EF Core PostgreSQL provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Pomelo.EntityFrameworkCore.MySql", "provider-specific-package", "Inventory EF Core MySQL/MariaDB provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("MySql.EntityFrameworkCore", "provider-specific-package", "Inventory EF Core MySQL provider usage during migration and isolate provider bootstrapping from strict-certified nORM data access."),
        ("Microsoft.Data.SqlClient", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("System.Data.SqlClient", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("Microsoft.Data.Sqlite", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("Npgsql", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("MySqlConnector", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions."),
        ("MySql.Data", "provider-specific-package", "Keep concrete provider packages in composition-root infrastructure; strict-certified repositories/services should depend on nORM abstractions.")
    };

    public static ProviderMobilityScanResult Scan(string rootPath)
    {
        var fullRoot = Path.GetFullPath(rootPath);
        if (!Directory.Exists(fullRoot) && !File.Exists(fullRoot))
        {
            return new ProviderMobilityScanResult(fullRoot, 0, new[]
            {
                new ProviderMobilityFinding(
                    fullRoot,
                    0,
                    "scan-path-missing",
                    "Error",
                    "The requested portability scan path does not exist.",
                    "Pass a valid application source directory or file.")
            });
        }

        var files = File.Exists(fullRoot)
            ? new[] { fullRoot }
            : Directory.EnumerateFiles(fullRoot, "*.*", SearchOption.AllDirectories)
                .Where(path => !IsGeneratedOrBuildOutput(fullRoot, path))
                .Where(static path =>
                    path.EndsWith(".cs", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
                    path.EndsWith(".targets", StringComparison.OrdinalIgnoreCase))
                .ToArray();

        var findings = new List<ProviderMobilityFinding>();
        foreach (var file in files)
            ScanFile(fullRoot, file, findings);

        return new ProviderMobilityScanResult(fullRoot, files.Length, findings);
    }

    private static void ScanFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        if (IsProjectFile(file) && TryScanProjectFile(root, file, findings))
            return;

        var rules = RulesForFile(file);
        var lines = File.ReadAllLines(file);
        var inBlockComment = false;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = StripBlockComments(lines[i], ref inBlockComment);
            var trimmed = line.TrimStart();
            if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("--", StringComparison.Ordinal))
                continue;

            foreach (var rule in rules)
            {
                if (!line.Contains(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (rule.Pattern == "ProviderNative" && line.Contains("TemporalStorageMode.ProviderNative", StringComparison.Ordinal))
                    continue;

                AddFinding(root, file, i + 1, rule, findings);
            }
        }
    }

    private static bool TryScanProjectFile(string root, string file, List<ProviderMobilityFinding> findings)
    {
        try
        {
            var document = XDocument.Load(file, LoadOptions.SetLineInfo);
            foreach (var package in document.Descendants().Where(static element =>
                         element.Name.LocalName is "PackageReference" or "PackageVersion"))
            {
                var packageId = (string?)package.Attribute("Include") ?? (string?)package.Attribute("Update");
                if (string.IsNullOrWhiteSpace(packageId))
                    continue;

                foreach (var rule in ProjectRules)
                {
                    if (!packageId.Equals(rule.Pattern, StringComparison.OrdinalIgnoreCase))
                        continue;

                    AddFinding(
                        root,
                        file,
                        package is IXmlLineInfo lineInfo && lineInfo.HasLineInfo() ? lineInfo.LineNumber : 0,
                        rule,
                        findings);
                }
            }

            return true;
        }
        catch (XmlException)
        {
            return false;
        }
    }

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

    private static (string Pattern, string Kind, string SuggestedFix)[] RulesForFile(string file)
    {
        if (file.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            return SqlRules;
        if (IsProjectFile(file))
            return ProjectRules;

        return CSharpRules;
    }

    private static bool IsProjectFile(string file)
        => file.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".props", StringComparison.OrdinalIgnoreCase) ||
           file.EndsWith(".targets", StringComparison.OrdinalIgnoreCase);

    private static string StripBlockComments(string line, ref bool inBlockComment)
    {
        var current = line;
        while (true)
        {
            if (inBlockComment)
            {
                var end = current.IndexOf("*/", StringComparison.Ordinal);
                if (end < 0)
                    return string.Empty;

                current = current[(end + 2)..];
                inBlockComment = false;
            }

            var start = current.IndexOf("/*", StringComparison.Ordinal);
            if (start < 0)
                return current;

            var close = current.IndexOf("*/", start + 2, StringComparison.Ordinal);
            if (close < 0)
            {
                inBlockComment = true;
                return current[..start];
            }

            current = current.Remove(start, close - start + 2);
        }
    }

    private static string SeverityFor(string kind)
        => kind is "provider-specific-package" or "provider-bootstrap-connection" or "client-projection-tail"
            ? "Warning"
            : "Error";

    private static bool IsGeneratedOrBuildOutput(string root, string path)
    {
        var normalized = Path.GetRelativePath(root, path).Replace('\\', '/');
        return normalized.StartsWith("bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/bin/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/obj/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/artifacts/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.git/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith(".tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/.tmp/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/node_modules/", StringComparison.OrdinalIgnoreCase) ||
               normalized.StartsWith("packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.Contains("/packages/", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".g.cs", StringComparison.OrdinalIgnoreCase) ||
               normalized.EndsWith(".designer.cs", StringComparison.OrdinalIgnoreCase);
    }
}

public static class ProviderMobilitySchemaInspector
{
    private static readonly HashSet<string> PortableClrTypes = new(StringComparer.Ordinal)
    {
        typeof(bool).FullName!,
        typeof(byte).FullName!,
        typeof(sbyte).FullName!,
        typeof(short).FullName!,
        typeof(ushort).FullName!,
        typeof(int).FullName!,
        typeof(uint).FullName!,
        typeof(long).FullName!,
        typeof(ulong).FullName!,
        typeof(float).FullName!,
        typeof(double).FullName!,
        typeof(decimal).FullName!,
        typeof(string).FullName!,
        typeof(Guid).FullName!,
        typeof(DateTime).FullName!,
        typeof(DateTimeOffset).FullName!,
        typeof(DateOnly).FullName!,
        typeof(TimeOnly).FullName!,
        typeof(TimeSpan).FullName!,
        typeof(char).FullName!,
        typeof(byte[]).FullName!
    };

    private static readonly string[] ProviderSpecificDefaultTokens =
    {
        "GETDATE(",
        "GETUTCDATE(",
        "SYSDATETIME(",
        "NEWID(",
        "NEWSEQUENTIALID(",
        "DATEADD(",
        "FOR JSON",
        "UUID(",
        "UUID_TO_BIN(",
        "GEN_RANDOM_UUID(",
        "NOW(",
        "SYSDATE(",
        "CLOCK_TIMESTAMP(",
        "LAST_INSERT_ID(",
        "NEXTVAL",
        "timezone(",
        "::",
        "N'"
    };

    private static readonly string[] DefaultReviewTokens =
    {
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "CURRENT_USER"
    };

    private static readonly IMigrationSqlGenerator[] Generators =
    {
        new SqliteMigrationSqlGenerator(),
        new SqlServerMigrationSqlGenerator(),
        new PostgresMigrationSqlGenerator(),
        new MySqlMigrationSqlGenerator()
    };

    public static IReadOnlyList<ProviderMobilityFinding> Inspect(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var findings = new List<ProviderMobilityFinding>();
        if (snapshot.Tables is null)
        {
            findings.Add(Finding("schema", 0, "schema-invalid", "Error",
                "SchemaSnapshot.Tables is null.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return findings;
        }

        var tableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var table in snapshot.Tables)
        {
            if (string.IsNullOrWhiteSpace(table.Name))
            {
                findings.Add(Finding("schema", 0, "schema-invalid-identifier", "Error",
                    "A schema table has no name.",
                    "Give every mapped entity a non-empty table name."));
                continue;
            }

            if (!tableNames.Add(table.Name))
            {
                findings.Add(Finding($"schema:{table.Name}", 0, "schema-duplicate-table", "Error",
                    $"The schema contains duplicate table name '{table.Name}'.",
                    "Use distinct table names before certifying provider mobility."));
            }

            InspectColumns(table, findings);
            InspectForeignKeys(table, findings);
        }

        ValidateProviderGeneration(snapshot, findings);
        return findings;
    }

    private static void InspectColumns(TableSchema table, List<ProviderMobilityFinding> findings)
    {
        if (table.Columns is null)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid", "Error",
                $"Table '{table.Name}' has null Columns.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return;
        }

        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var pkCount = 0;
        foreach (var column in table.Columns)
        {
            var location = $"schema:{table.Name}.{column.Name}";
            if (string.IsNullOrWhiteSpace(column.Name))
            {
                findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid-identifier", "Error",
                    $"Table '{table.Name}' has a column with no name.",
                    "Give every mapped property a non-empty column name."));
                continue;
            }

            if (!columnNames.Add(column.Name))
            {
                findings.Add(Finding(location, 0, "schema-duplicate-column", "Error",
                    $"Table '{table.Name}' contains duplicate column '{column.Name}'.",
                    "Use distinct column names before certifying provider mobility."));
            }

            if (string.IsNullOrWhiteSpace(column.ClrType))
            {
                findings.Add(Finding(location, 0, "schema-missing-clr-type", "Error",
                    $"Column '{table.Name}.{column.Name}' has no CLR type metadata.",
                    "Regenerate the schema snapshot from nORM mapping metadata so migration type mapping is deterministic."));
            }
            else if (!IsPortableClrType(column.ClrType, out var unresolvedCustomType))
            {
                findings.Add(Finding(location, 0, "schema-unsupported-clr-type", unresolvedCustomType ? "Warning" : "Error",
                    $"Column '{table.Name}.{column.Name}' uses CLR type '{column.ClrType}', which is not in the provider-mobile scalar type set.",
                    unresolvedCustomType
                        ? "Run certification with the application assembly loaded so enum types can be resolved, or convert the property to a supported scalar provider type."
                        : "Use a supported scalar CLR type or a value converter whose provider type is in nORM's provider-mobile type map."));
            }

            if (column.IsPrimaryKey)
                pkCount++;

            if (column.IsIdentity && !IsPortableIdentityType(column.ClrType))
            {
                findings.Add(Finding(location, 0, "schema-nonportable-identity", "Error",
                    $"Column '{table.Name}.{column.Name}' is marked identity but uses '{column.ClrType}'.",
                    "Use an integral identity column type for provider-mobile generated migrations."));
            }

            InspectDefaultValue(table, column, findings);
        }

        if (pkCount == 0)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-missing-primary-key", "Warning",
                $"Table '{table.Name}' has no primary key in the migration snapshot.",
                "Add a primary key for full generated write, include, tenant, and temporal behavior. Keyless/read-only shapes should be explicitly documented."));
        }
    }

    private static void InspectDefaultValue(TableSchema table, ColumnSchema column, List<ProviderMobilityFinding> findings)
    {
        if (string.IsNullOrWhiteSpace(column.DefaultValue))
            return;

        var defaultValue = column.DefaultValue!;
        foreach (var token in ProviderSpecificDefaultTokens)
        {
            if (!defaultValue.Contains(token, StringComparison.OrdinalIgnoreCase))
                continue;

            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-provider-specific-default", "Error",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' contains provider-specific token '{token}'.",
                "Use an application-stamped value, a simple literal default, or an explicit provider-specific migration outside strict certification."));
            return;
        }

        foreach (var token in DefaultReviewTokens)
        {
            if (!defaultValue.Equals(token, StringComparison.OrdinalIgnoreCase))
                continue;

            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-default-needs-review", "Warning",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' uses database-evaluated token '{token}'.",
                "Verify timezone, precision, and user semantics on every target provider or replace it with an application-stamped value before strict certification."));
            return;
        }

        if (defaultValue.Contains('(') && !defaultValue.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            findings.Add(Finding($"schema:{table.Name}.{column.Name}", 0, "schema-default-needs-review", "Warning",
                $"Column '{table.Name}.{column.Name}' default value '{defaultValue}' looks function-based.",
                "Verify this default on every target provider or replace it with an application-stamped value before strict certification."));
        }
    }

    private static void InspectForeignKeys(TableSchema table, List<ProviderMobilityFinding> findings)
    {
        if (table.ForeignKeys is null)
        {
            findings.Add(Finding($"schema:{table.Name}", 0, "schema-invalid", "Error",
                $"Table '{table.Name}' has null ForeignKeys.",
                "Regenerate the snapshot through nORM migrations or a design-time DbContext."));
            return;
        }

        var fkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fk in table.ForeignKeys)
        {
            var location = $"schema:{table.Name}.{fk.ConstraintName}";
            if (string.IsNullOrWhiteSpace(fk.ConstraintName) || !fkNames.Add(fk.ConstraintName))
            {
                findings.Add(Finding(location, 0, "schema-invalid-foreign-key", "Error",
                    $"Table '{table.Name}' has an empty or duplicate foreign key constraint name.",
                    "Give each foreign key a stable, unique constraint name."));
            }

            if (fk.DependentColumns is null || fk.DependentColumns.Length == 0 ||
                fk.PrincipalColumns is null || fk.PrincipalColumns.Length == 0 ||
                string.IsNullOrWhiteSpace(fk.PrincipalTable))
            {
                findings.Add(Finding(location, 0, "schema-invalid-foreign-key", "Error",
                    $"Foreign key '{fk.ConstraintName}' on '{table.Name}' is missing dependent columns, principal columns, or principal table.",
                    "Regenerate the schema snapshot from fluent relation metadata."));
            }
        }
    }

    private static void ValidateProviderGeneration(SchemaSnapshot snapshot, List<ProviderMobilityFinding> findings)
    {
        if (findings.Any(static finding => finding.Severity.Equals("Error", StringComparison.OrdinalIgnoreCase)))
            return;

        var diff = new SchemaDiff();
        foreach (var table in snapshot.Tables)
            diff.AddedTables.Add(table);

        foreach (var generator in Generators)
        {
            try
            {
                _ = generator.GenerateSql(diff);
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or NotSupportedException)
            {
                findings.Add(Finding("schema", 0, "schema-provider-generation-failed", "Error",
                    $"{generator.GetType().Name} could not generate provider DDL from the snapshot: {ex.Message}",
                    "Fix schema metadata so all target provider migration generators can emit DDL deterministically."));
            }
        }
    }

    private static bool IsPortableClrType(string clrType, out bool unresolvedCustomType)
    {
        unresolvedCustomType = false;
        if (PortableClrTypes.Contains(clrType))
            return true;

        var resolved = ResolveType(clrType);
        if (resolved?.IsEnum == true)
            return true;

        unresolvedCustomType = !clrType.StartsWith("System.", StringComparison.Ordinal);
        return false;
    }

    private static bool IsPortableIdentityType(string clrType)
        => clrType == typeof(byte).FullName ||
           clrType == typeof(short).FullName ||
           clrType == typeof(int).FullName ||
           clrType == typeof(long).FullName;

    private static Type? ResolveType(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        var type = Type.GetType(typeName);
        if (type != null)
            return type;

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = assembly.GetType(typeName);
            if (type != null)
                return type;
        }

        return null;
    }

    private static ProviderMobilityFinding Finding(
        string path,
        int line,
        string kind,
        string severity,
        string reason,
        string suggestedFix)
        => new(path, line, kind, severity, reason, suggestedFix);
}
