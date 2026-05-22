using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

public class DocumentationContractTests
{
    [Fact]
    public void Readme_links_to_precise_linq_support_matrix()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var matrix = File.ReadAllText(Path.Combine(root, "docs", "linq-support.md"));

        Assert.Contains("docs/linq-support.md", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Complete LINQ Support", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("full LINQ", readme, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("| Feature | Status | Notes |", matrix, StringComparison.Ordinal);
        Assert.Contains("ExecuteUpdateAsync", matrix, StringComparison.Ordinal);
        Assert.Contains("AsAsyncEnumerable", matrix, StringComparison.Ordinal);
        Assert.Contains("Unsupported", matrix, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_aot_and_trimming_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "aot-trimming.md"));

        Assert.Contains("docs/aot-trimming.md", readme, StringComparison.Ordinal);
        Assert.Contains("NativeAOT", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresDynamicCode", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresUnreferencedCode", policy, StringComparison.Ordinal);
        Assert.Contains("not supported", policy, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_links_to_cache_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "cache-policy.md"));

        Assert.Contains("docs/cache-policy.md", readme, StringComparison.Ordinal);
        Assert.Contains("Compiled materializer store", policy, StringComparison.Ordinal);
        Assert.Contains("Evictions", policy, StringComparison.Ordinal);
        Assert.Contains("Per-Context Caches", policy, StringComparison.Ordinal);
        Assert.Contains("process-wide", policy, StringComparison.OrdinalIgnoreCase);
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
