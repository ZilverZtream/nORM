using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Every exported nORM type must live in one of the approved namespaces listed in
/// <c>docs/namespace-policy.md</c>. <c>nORM.Internal</c> remains on the list for backwards
/// compatibility with the existing shipped surface but is closed for additions; new types
/// must land in a supported namespace.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NamespacePolicyContractTests
{
    private static readonly HashSet<string> ApprovedNamespaces = new(StringComparer.Ordinal)
    {
        "nORM.Configuration",
        "nORM.Core",
        "nORM.Enterprise",
        "nORM.Execution",
        "nORM.Mapping",
        "nORM.Migration",
        "nORM.Navigation",
        "nORM.Providers",
        "nORM.Query",
        "nORM.Scaffolding",
        "nORM.SourceGeneration",
        "nORM.Internal", // deprecated; existing entries grandfathered, additions blocked below
        // Extension types intentionally registered under the namespace of the BCL type they
        // extend (convention for Microsoft.Extensions.* integration).
        "Microsoft.Extensions",
    };

    // Types currently exported under nORM.Internal at the v1.0 baseline. Additions to this
    // namespace are NOT permitted; new public types must land in a supported namespace.
    private static readonly HashSet<string> GrandfatheredInternalTypes = new(StringComparer.Ordinal)
    {
        // ConcurrentLruCache was internalized during API-freeze prep — no longer public.
        "nORM.Internal.ParameterOptimizer",
    };

    [Fact]
    public void Every_public_type_lives_in_an_approved_namespace()
    {
        var assembly = typeof(DbContext).Assembly;
        var offending = new List<string>();

        foreach (var type in assembly.GetExportedTypes())
        {
            if (type.Namespace is null) continue;
            // Walk up to the top-level namespace (nORM.X) for comparison.
            var parts = type.Namespace.Split('.');
            var topLevel = parts.Length >= 2
                ? $"{parts[0]}.{parts[1]}"
                : type.Namespace;
            if (!ApprovedNamespaces.Contains(topLevel))
            {
                offending.Add($"{type.FullName} (namespace {type.Namespace} is not in the approved list)");
            }
        }

        Assert.True(offending.Count == 0,
            "Public types in unapproved namespaces. Add the namespace to docs/namespace-policy.md " +
            "and NamespacePolicyContractTests.ApprovedNamespaces, or move the types:\n" +
            string.Join("\n", offending));
    }

    [Fact]
    public void Internal_namespace_remains_closed_for_additions()
    {
        var assembly = typeof(DbContext).Assembly;
        var current = assembly.GetExportedTypes()
            .Where(t => t.Namespace == "nORM.Internal")
            .Select(t => t.FullName ?? t.Name)
            .ToHashSet(StringComparer.Ordinal);

        var newAdditions = current.Where(t => !GrandfatheredInternalTypes.Contains(t)).ToList();

        Assert.True(newAdditions.Count == 0,
            "New public types found under nORM.Internal:\n" +
            string.Join("\n", newAdditions) +
            "\nMove them to a supported namespace (see docs/namespace-policy.md) before merging.");

        var missing = GrandfatheredInternalTypes.Where(t => !current.Contains(t)).ToList();
        Assert.True(missing.Count == 0,
            "Grandfathered nORM.Internal entries no longer exist (good - removed/relocated). " +
            "Update NamespacePolicyContractTests.GrandfatheredInternalTypes:\n" +
            string.Join("\n", missing));
    }
}
