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
