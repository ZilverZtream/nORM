using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class TenantTemporalDocumentationContractTests
{
    [Fact]
    public void Tenant_boundary_doc_names_generated_and_privileged_paths()
    {
        var doc = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "docs", "tenant-boundary.md"));

        Assert.Contains("Tenant boundary, not tenant convenience.", doc, StringComparison.Ordinal);
        Assert.Contains("Generated `Query<T>()` paths", doc, StringComparison.Ordinal);
        Assert.Contains("Raw SQL", doc, StringComparison.Ordinal);
        Assert.Contains("Stored procedure", doc, StringComparison.Ordinal);
        Assert.Contains("GetTenantBoundaryDiagnostics", doc, StringComparison.Ordinal);
        Assert.Contains("TenantTemporalProviderSwapTests", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Tenant_deployment_docs_bound_topology_and_rls_claims()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var topology = File.ReadAllText(Path.Combine(root, "docs", "tenant-deployment-patterns.md"));
        var rls = File.ReadAllText(Path.Combine(root, "docs", "tenant-database-native-rls.md"));

        Assert.Contains("docs/tenant-deployment-patterns.md", readme, StringComparison.Ordinal);
        Assert.Contains("docs/tenant-database-native-rls.md", readme, StringComparison.Ordinal);
        Assert.Contains("Shared Table", topology, StringComparison.Ordinal);
        Assert.Contains("Database Per Tenant", topology, StringComparison.Ordinal);
        Assert.Contains("Connection Per Tenant", topology, StringComparison.Ordinal);
        Assert.Contains("privileged paths", topology, StringComparison.Ordinal);
        Assert.Contains("v1 does not", rls, StringComparison.Ordinal);
        Assert.Contains("does not make native RLS policy execution automatic", rls, StringComparison.Ordinal);
        Assert.Contains("EnableNativeTenantSessionContext", rls, StringComparison.Ordinal);
        Assert.Contains("GenerateNativeTenantPolicySql", rls, StringComparison.Ordinal);
        Assert.Contains("ApplyNativeTenantPolicyAsync", rls, StringComparison.Ordinal);
        Assert.Contains("DropNativeTenantPolicyAsync", rls, StringComparison.Ordinal);
        Assert.Contains("SQL Server", rls, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", rls, StringComparison.Ordinal);
        Assert.Contains("defense in depth", rls, StringComparison.Ordinal);
    }

    [Fact]
    public void Temporal_doc_bounds_provider_neutral_history_and_tracking_behavior()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var doc = File.ReadAllText(Path.Combine(root, "docs", "temporal-versioning.md"));
        var precision = File.ReadAllText(Path.Combine(root, "docs", "temporal-precision.md"));

        Assert.Contains("docs/temporal-precision.md", readme, StringComparison.Ordinal);
        Assert.Contains("nORM-managed temporal history", doc, StringComparison.Ordinal);
        Assert.Contains("not provider-native temporal", doc, StringComparison.Ordinal);
        Assert.Contains("no-tracking snapshots", doc, StringComparison.Ordinal);
        Assert.Contains("RestoreTemporalVersionAsync", doc, StringComparison.Ordinal);
        Assert.Contains("PruneTemporalHistoryAsync", doc, StringComparison.Ordinal);
        Assert.Contains("GetTemporalDiffAsync", doc, StringComparison.Ordinal);
        Assert.Contains("explicit transactions", doc, StringComparison.Ordinal);
        Assert.Contains("TemporalStorageMode.ProviderNative", doc, StringComparison.Ordinal);
        Assert.Contains("ApplyProviderNativeTemporalBootstrapAsync", doc, StringComparison.Ordinal);
        Assert.Contains("FOR SYSTEM_TIME AS OF", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-neutral history, diff, restore, and", doc, StringComparison.Ordinal);
        Assert.Contains("TenantTemporalProviderSwapTests", doc, StringComparison.Ordinal);
        Assert.Contains("ValidFrom <= timestamp < ValidTo", precision, StringComparison.Ordinal);
        Assert.Contains("SQLite", precision, StringComparison.Ordinal);
        Assert.Contains("provider trigger clock", precision, StringComparison.Ordinal);
        Assert.DoesNotContain("system-versioned temporal tables for v1 temporal support", doc, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Product_positioning_doc_avoids_unbounded_replacement_claims()
    {
        var doc = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "docs", "product-positioning.md"));

        Assert.Contains("evidence-gated", doc, StringComparison.Ordinal);
        Assert.Contains("provider-mobile", doc, StringComparison.Ordinal);
        Assert.Contains("provider-swap certification", doc, StringComparison.Ordinal);
        Assert.Contains("It is not a full EF Core replacement.", doc, StringComparison.Ordinal);
        Assert.DoesNotContain("drop-in EF Core replacement", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("It is not always faster than raw ADO.NET.", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Provider_mobility_contract_defines_translate_emulate_or_fail_rule()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var doc = File.ReadAllText(Path.Combine(root, "docs", "provider-mobility-contract.md"));
        var sampleReadme = File.ReadAllText(Path.Combine(root, "samples", "nORM.Sample.Store", "README.md"));

        Assert.Contains("docs/provider-mobility-contract.md", readme, StringComparison.Ordinal);
        Assert.Contains("translate correctly", doc, StringComparison.Ordinal);
        Assert.Contains("emulate equivalent behavior", doc, StringComparison.Ordinal);
        Assert.Contains("fail", doc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Portable", doc, StringComparison.Ordinal);
        Assert.Contains("Emulated", doc, StringComparison.Ordinal);
        Assert.Contains("Provider-bound", doc, StringComparison.Ordinal);
        Assert.Contains("Unsupported", doc, StringComparison.Ordinal);
        Assert.Contains("certify-provider-swap", doc, StringComparison.Ordinal);
        Assert.Contains("UseStrictProviderMobility", doc, StringComparison.Ordinal);
        Assert.Contains("strict-provider-mobility-mode", doc, StringComparison.Ordinal);
        Assert.Contains("migration findings", doc, StringComparison.Ordinal);
        Assert.Contains("--scan-path", doc, StringComparison.Ordinal);
        Assert.Contains("machine-readable JSON report", sampleReadme, StringComparison.Ordinal);
        Assert.Contains("UseStrictProviderMobility", sampleReadme, StringComparison.Ordinal);
        Assert.Contains("--scan-path", sampleReadme, StringComparison.Ordinal);
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }
}
