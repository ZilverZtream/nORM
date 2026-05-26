using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Enforces the v1 public-API supportability classification.
///
/// Each public namespace is assigned exactly one of four stability labels:
///   StableUser      — application code depends on these types; breaking changes
///                     require a major version bump.
///   StableProvider  — provider and mapping implementors depend on these; subject
///                     to the same breaking-change policy as StableUser.
///   StableTooling   — CLI, migrations, scaffolding, source-generation; stable for
///                     the v1 tooling contract, but not the runtime embedding API.
///   Unsupported     — types that are public for technical reasons (source-gen
///                     codegen targets, internal utilities) but carry no stability
///                     guarantee for v1.  Consumers may not depend on them.
///
/// Adding a new public namespace without updating this test is a build-time
/// signal to confirm the intended stability category before shipping.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public sealed class PublicApiClassificationTests
{
    // ── Classification registry ───────────────────────────────────────────────

    private enum StabilityCategory { StableUser, StableProvider, StableTooling, Unsupported }

    private static readonly Dictionary<string, StabilityCategory> NamespaceClassification =
        new(StringComparer.Ordinal)
        {
            // Extension methods for ILogger — stable user surface.
            ["Microsoft.Extensions.Logging"] = StabilityCategory.StableUser,

            // Primary user-facing surface: DbContext, query operators, LINQ extensions,
            // change tracking, exceptions, JSON helpers, caching extensions.
            ["nORM.Configuration"]  = StabilityCategory.StableUser,
            ["nORM.Core"]           = StabilityCategory.StableUser,
            ["nORM.Enterprise"]     = StabilityCategory.StableUser,

            // Provider and mapping contracts: DatabaseProvider base class, Column,
            // TableMapping, value converters, attributes.
            ["nORM.Mapping"]        = StabilityCategory.StableProvider,
            ["nORM.Providers"]      = StabilityCategory.StableProvider,

            // Navigation lazy-loading helpers — stable user surface for eager/lazy nav.
            ["nORM.Navigation"]     = StabilityCategory.StableUser,

            // Query builder utilities and SQL functions — stable user surface.
            ["nORM.Query"]          = StabilityCategory.StableUser,

            // Tooling: migration runners, SQL generators, schema diff, design-time
            // factory interface, scaffolder.
            ["nORM.Migration"]      = StabilityCategory.StableTooling,
            ["nORM.Scaffolding"]    = StabilityCategory.StableTooling,

            // Source generation: compile-time query attributes and materializer store.
            // Stable for the source-gen tooling contract.
            ["nORM.SourceGeneration"] = StabilityCategory.StableTooling,

            // AdaptiveTimeoutManager: public for advanced operator configuration but
            // not committed to v1 stability — behaviour and shape may change.
            ["nORM.Execution"]      = StabilityCategory.Unsupported,

            // Implementation utilities (ConcurrentLruCache, ParameterOptimizer).
            // Public for use by provider packages; not a stable end-user API.
            ["nORM.Internal"]       = StabilityCategory.Unsupported,
        };

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Every_public_type_in_shipped_api_has_a_stability_classification()
    {
        var baselinePath = GetBaselinePath();
        var typeLines = File.ReadAllLines(baselinePath)
            .Where(static l => l.StartsWith("T:", StringComparison.Ordinal))
            .ToList();

        var unclassified = new List<string>();
        foreach (var line in typeLines)
        {
            var ns = ExtractNamespace(line[2..]); // strip "T:"
            if (!NamespaceClassification.ContainsKey(ns))
                unclassified.Add($"{line}  →  namespace '{ns}' not in classification registry");
        }

        Assert.True(unclassified.Count == 0,
            "Unclassified public types found. Add their namespace to NamespaceClassification:\n" +
            string.Join("\n", unclassified));
    }

    [Fact]
    public void No_uncovered_namespaces_exist_in_shipped_api()
    {
        var baselinePath = GetBaselinePath();
        var actualNamespaces = File.ReadAllLines(baselinePath)
            .Where(static l => l.StartsWith("T:", StringComparison.Ordinal))
            .Select(static l => ExtractNamespace(l[2..]))
            .ToHashSet(StringComparer.Ordinal);

        var coveredNamespaces = NamespaceClassification.Keys.ToHashSet(StringComparer.Ordinal);

        var missing = actualNamespaces.Except(coveredNamespaces).OrderBy(static x => x).ToList();
        Assert.True(missing.Count == 0,
            "Namespaces in PublicApi.Shipped.txt have no classification entry:\n" +
            string.Join("\n", missing));
    }

    [Fact]
    public void StableUser_and_StableProvider_categories_are_non_empty()
    {
        var stableUser = NamespaceClassification.Values.Count(static v => v == StabilityCategory.StableUser);
        var stableProvider = NamespaceClassification.Values.Count(static v => v == StabilityCategory.StableProvider);

        Assert.True(stableUser >= 4,
            $"Expected at least 4 StableUser namespace groups, got {stableUser}.");
        Assert.True(stableProvider >= 2,
            $"Expected at least 2 StableProvider namespace groups, got {stableProvider}.");
    }

    [Fact]
    public void Unsupported_namespaces_are_explicitly_declared_not_discovered_by_accident()
    {
        // Every Unsupported entry must exist in the shipped API — prevents zombie
        // classifications for removed namespaces.
        var baselinePath = GetBaselinePath();
        var actualNamespaces = File.ReadAllLines(baselinePath)
            .Where(static l => l.StartsWith("T:", StringComparison.Ordinal))
            .Select(static l => ExtractNamespace(l[2..]))
            .ToHashSet(StringComparer.Ordinal);

        var zombies = NamespaceClassification
            .Where(static kv => kv.Value == StabilityCategory.Unsupported)
            .Select(static kv => kv.Key)
            .Where(ns => !actualNamespaces.Contains(ns))
            .ToList();

        Assert.True(zombies.Count == 0,
            "Unsupported namespaces in classification have no types in the shipped API (stale entries):\n" +
            string.Join("\n", zombies));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string GetBaselinePath()
    {
        var path = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "PublicApi.Shipped.txt"));
        Assert.True(File.Exists(path),
            $"PublicApi.Shipped.txt not found at {path}.");
        return path;
    }

    private static string ExtractNamespace(string typeName)
    {
        // Strip generic arity suffix from the type name before parsing namespace.
        // e.g.  "nORM.Core.INormQueryable{`0}"  →  "nORM.Core"
        //       "Microsoft.Extensions.Logging.DbContextLoggingExtensions"  →  "Microsoft.Extensions.Logging"
        var bare = typeName.Split('{')[0].Split('[')[0];
        var segments = bare.Split('.');
        if (segments.Length < 2) return bare;

        // Special-case Microsoft.Extensions.Logging — three-segment namespace.
        if (segments[0] == "Microsoft" && segments.Length >= 3)
            return string.Join(".", segments[0], segments[1], segments[2]);

        // nORM.SubNamespace — two-segment.
        return string.Join(".", segments[0], segments[1]);
    }
}
