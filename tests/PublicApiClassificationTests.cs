using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Enforces the v1 public API support-tier map from docs/namespace-policy.md.
/// The docs are the release promise; this test keeps the shipped baseline from
/// gaining unclassified or stale namespace entries.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public sealed class PublicApiClassificationTests
{
    [Fact]
    public void Every_public_api_entry_in_shipped_baseline_has_a_documented_support_tier()
    {
        var typeNamespaces = ReadShippedTypeNamespaces();
        var policy = ReadNamespacePolicy();
        var typeNames = typeNamespaces.Keys
            .OrderByDescending(static type => type.Length)
            .ToArray();

        var unclassified = new List<string>();
        foreach (var line in ReadPublicApiLines())
        {
            var typeName = line.StartsWith("T:", StringComparison.Ordinal)
                ? line[2..]
                : FindOwningType(line[2..], typeNames);
            if (typeName is null)
            {
                unclassified.Add(line + " -> owning type not found in shipped baseline");
                continue;
            }

            var ns = typeNamespaces[typeName];
            if (!policy.TryGetValue(ns, out var tier) || string.IsNullOrWhiteSpace(tier))
                unclassified.Add(line + " -> namespace '" + ns + "' has no documented support tier");
        }

        Assert.True(unclassified.Count == 0,
            "Public API entries without documented support tiers:\n" +
            string.Join("\n", unclassified));
    }

    [Fact]
    public void Namespace_policy_matches_shipped_public_namespaces()
    {
        var shipped = ReadShippedTypeNamespaces()
            .Values
            .ToHashSet(StringComparer.Ordinal);
        var documented = ReadNamespacePolicy()
            .Keys
            .ToHashSet(StringComparer.Ordinal);

        var missing = shipped.Except(documented).OrderBy(static ns => ns, StringComparer.Ordinal).ToArray();
        var stale = documented.Except(shipped).OrderBy(static ns => ns, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0,
            "Namespaces in PublicApi.Shipped.txt are missing from docs/namespace-policy.md:\n" +
            string.Join("\n", missing));
        Assert.True(stale.Length == 0,
            "Namespaces in docs/namespace-policy.md have no shipped public types:\n" +
            string.Join("\n", stale));
    }

    [Fact]
    public void Namespace_policy_records_required_v1_support_tiers()
    {
        var policy = ReadNamespacePolicy();

        Assert.Contains(policy, entry => entry.Value.Contains("Stable user API", StringComparison.Ordinal));
        Assert.Contains(policy, entry => entry.Value.Contains("Stable provider API", StringComparison.Ordinal));
        Assert.Contains(policy, entry => entry.Value.Contains("Stable tooling", StringComparison.Ordinal));
        Assert.Equal("Stable user API", policy["nORM.Execution"]);
        Assert.Contains("Deprecated namespace", policy["nORM.Internal"], StringComparison.Ordinal);
        Assert.Contains("relocation", policy["nORM.Internal"], StringComparison.OrdinalIgnoreCase);
    }

    private static string GetRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static string GetBaselinePath()
    {
        var path = Path.Combine(GetRepoRoot(), "tests", "PublicApi.Shipped.txt");
        Assert.True(File.Exists(path), "PublicApi.Shipped.txt not found at " + path + ".");
        return path;
    }

    private static string GetNamespacePolicyPath()
    {
        var path = Path.Combine(GetRepoRoot(), "docs", "namespace-policy.md");
        Assert.True(File.Exists(path), "docs/namespace-policy.md not found at " + path + ".");
        return path;
    }

    private static IReadOnlyList<string> ReadPublicApiLines()
        => File.ReadAllLines(GetBaselinePath())
            .Where(static line => line.Length > 2 && !line.StartsWith("#", StringComparison.Ordinal))
            .ToArray();

    private static IReadOnlyDictionary<string, string> ReadShippedTypeNamespaces()
        => ReadPublicApiLines()
            .Where(static line => line.StartsWith("T:", StringComparison.Ordinal))
            .Select(static line => line[2..])
            .ToDictionary(static type => type, ExtractNamespace, StringComparer.Ordinal);

    private static IReadOnlyDictionary<string, string> ReadNamespacePolicy()
    {
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        var inApprovedTable = false;
        var rowPattern = new Regex(@"^\|\s*`(?<namespace>[^`]+)`\s*\|\s*(?<tier>[^|]+?)\s*\|", RegexOptions.CultureInvariant);

        foreach (var line in File.ReadLines(GetNamespacePolicyPath()))
        {
            if (line.StartsWith("## Approved Namespaces", StringComparison.Ordinal))
            {
                inApprovedTable = true;
                continue;
            }

            if (inApprovedTable && line.StartsWith("## ", StringComparison.Ordinal))
                break;
            if (!inApprovedTable)
                continue;

            var match = rowPattern.Match(line);
            if (!match.Success)
                continue;

            var ns = match.Groups["namespace"].Value.Trim();
            var tier = Regex.Replace(match.Groups["tier"].Value, @"[*_]", string.Empty, RegexOptions.CultureInvariant).Trim();
            rows[ns] = tier;
        }

        Assert.NotEmpty(rows);
        return rows;
    }

    private static string? FindOwningType(string memberLineBody, IReadOnlyList<string> typeNames)
        => typeNames.FirstOrDefault(type => memberLineBody.StartsWith(type + ".", StringComparison.Ordinal));

    private static string ExtractNamespace(string typeName)
    {
        var bare = typeName.Split('{')[0].Split('[')[0];
        var segments = bare.Split('.');
        if (segments.Length < 2)
            return bare;

        if (segments[0] == "Microsoft" && segments.Length >= 3)
            return string.Join(".", segments[0], segments[1], segments[2]);

        return string.Join(".", segments[0], segments[1]);
    }
}
