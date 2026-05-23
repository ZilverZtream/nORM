using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the operational docs that downstream operators rely on: <c>docs/cache-policy.md</c>
/// (shared cache bounds and release-gate evidence), <c>docs/transactions.md</c> (savepoint
/// support per provider and the commit-cancellation rule), and the existence of
/// <c>nORM.Core.ConnectionManager</c> in the public API snapshot for HA topology consumers.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OperationalContractDocTests
{
    private static string ReadDoc(string fileName)
    {
        var asmDir = Path.GetDirectoryName(typeof(OperationalContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", fileName);
        Assert.True(File.Exists(path), $"docs/{fileName} not found at {path}");
        return File.ReadAllText(path);
    }

    // Cache bounds (docs/cache-policy.md)

    [Theory]
    [InlineData("Shared Runtime Caches")]
    [InlineData("Per-Context Caches")]
    [InlineData("Compiled Query Cache")]
    [InlineData("User Result Cache")]
    [InlineData("Release Gate Evidence")]
    public void Cache_policy_doc_covers_section(string heading)
    {
        var doc = ReadDoc("cache-policy.md");
        Assert.Contains(heading, doc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Query plan cache")]
    [InlineData("Dynamic table type cache")]
    [InlineData("Compiled materializer store")]
    [InlineData("Materializer factory caches")]
    public void Cache_policy_doc_names_bounded_shared_caches(string label)
    {
        var doc = ReadDoc("cache-policy.md");
        Assert.Contains(label, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Cache_policy_doc_names_release_gate_tests()
    {
        var doc = ReadDoc("cache-policy.md");
        Assert.Contains("CacheMemoryBoundReleaseGateTests", doc, StringComparison.Ordinal);
        Assert.Contains("ConcurrentLruCacheStressTests", doc, StringComparison.Ordinal);
    }

    // Savepoint uniformity + transaction ownership (docs/transactions.md)

    [Theory]
    [InlineData("SQLite", "Yes")]
    [InlineData("SQL Server", "Yes")]
    [InlineData("PostgreSQL", "Yes")]
    [InlineData("MySQL", "Yes")]
    public void Transactions_doc_marks_provider_savepoint_support(string provider, string expected)
    {
        var doc = ReadDoc("transactions.md");
        // The table line "| <Provider> | Yes (...)" must be present.
        var probe = $"| {provider} | {expected}";
        Assert.Contains(probe, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Transactions_doc_documents_commit_cancellation_rule()
    {
        var doc = ReadDoc("transactions.md");
        // Commit cancellation rule: nORM uses CancellationToken.None once completion begins.
        // The doc wraps the type name in backticks, so check the surrounding clause.
        Assert.Contains("once completion begins", doc, StringComparison.Ordinal);
        Assert.Contains("CancellationToken.None", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Transactions_doc_covers_ambient_scope_policy_options()
    {
        var doc = ReadDoc("transactions.md");
        Assert.Contains("FailFast", doc, StringComparison.Ordinal);
        Assert.Contains("BestEffort", doc, StringComparison.Ordinal);
        Assert.Contains("Ignore", doc, StringComparison.Ordinal);
    }

    // ConnectionManager presence in the public API snapshot - no dedicated doc, the public
    // surface itself is the contract HA consumers depend on.

    [Fact]
    public void ConnectionManager_is_in_the_public_api_snapshot()
    {
        var asmDir = Path.GetDirectoryName(typeof(OperationalContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var snapshot = File.ReadAllText(Path.Combine(repoRoot, "tests", "PublicApi.Shipped.txt"));
        Assert.Contains("ConnectionManager", snapshot, StringComparison.Ordinal);
    }
}
