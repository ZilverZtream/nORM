using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class RepositoryHygieneTests
{
    private static readonly string RepoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
    private const int MaxProductionScaffoldingFileLines = 250;

    [Fact]
    public void Test_project_does_not_suppress_async_warning_as_release_exception()
    {
        var project = File.ReadAllText(Path.Combine(RepoRoot, "tests", "nORM.Tests.csproj"));

        Assert.DoesNotContain("CS1998", project, StringComparison.Ordinal);
        Assert.DoesNotContain("WarningsNotAsErrors", project, StringComparison.Ordinal);
    }

    [Fact]
    public void Generated_test_artifacts_are_ignored_and_not_tracked()
    {
        var gitignore = File.ReadAllText(Path.Combine(RepoRoot, ".gitignore"));
        var hygiene = File.ReadAllText(Path.Combine(RepoRoot, "docs", "repository-hygiene.md"));
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));

        Assert.Contains("tests/TestResults/", gitignore, StringComparison.Ordinal);
        Assert.Contains(".tmp/", gitignore, StringComparison.Ordinal);
        Assert.Contains("*.trx", gitignore, StringComparison.Ordinal);
        Assert.Contains("*.coverage", gitignore, StringComparison.Ordinal);
        Assert.Contains(".tmp/", hygiene, StringComparison.Ordinal);
        Assert.Contains("tests/TestResults/", hygiene, StringComparison.Ordinal);
        Assert.Contains("test-suite-ownership.md", hygiene, StringComparison.Ordinal);
        Assert.Contains("Do not add new catch-all `CoverageBoost` files.", ownership, StringComparison.Ordinal);

        var trackedArtifacts = GetTrackedFiles()
            .Where(path => path.StartsWith("tests/TestResults/", StringComparison.OrdinalIgnoreCase) ||
                           path.EndsWith(".trx", StringComparison.OrdinalIgnoreCase) ||
                           path.EndsWith(".coverage", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.Empty(trackedArtifacts);
    }

    [Fact]
    public void Production_scaffolding_files_stay_split_by_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Production scaffolding files stay below 250 lines", ownership, StringComparison.Ordinal);

        var scaffoldingDirectory = Path.Combine(RepoRoot, "src", "nORM", "Scaffolding");
        var oversizedFiles = Directory.EnumerateFiles(scaffoldingDirectory, "*.cs", SearchOption.AllDirectories)
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProductionScaffoldingFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split production scaffolding code before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    private static string[] GetTrackedFiles()
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "git",
            WorkingDirectory = RepoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        process.StartInfo.ArgumentList.Add("ls-files");
        process.Start();
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0, error);
        return output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
    }
}
