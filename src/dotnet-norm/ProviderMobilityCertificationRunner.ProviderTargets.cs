using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Cli;

public static partial class ProviderMobilityCertificationRunner
{
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
}
