using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the `dotnet norm` command surface as a reviewed list and cross-checks that every
/// user-facing invocation is documented: a new verb, a renamed verb, or an undocumented
/// command must break this test so the surface changes deliberately before the API freeze.
/// The CLI sources are scanned as text (the same pattern the scaffolding doc-contract
/// tests use), so the pin needs no reference to the tool assembly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CliCommandSurfaceContractTests
{
    private static readonly string RepoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));

    private static string ReadAllCliCommandSource()
        => string.Concat(Directory.GetFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "Program*.cs")
            .OrderBy(p => p, StringComparer.Ordinal)
            .Select(File.ReadAllText));

    [Fact]
    public void The_command_surface_is_the_reviewed_verb_list()
    {
        var source = ReadAllCliCommandSource();
        var verbs = Regex.Matches(source, "new (?:Root)?Command\\(\"(?<name>[a-z]+)\"")
            .Select(m => m.Groups["name"].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToArray();

        var reviewed = new[] { "add", "certify", "database", "dbcontext", "drop", "migrations", "portability", "scaffold", "update" };
        Assert.Equal(reviewed, verbs);
    }

    [Theory]
    [InlineData("docs/scaffolding.md", "norm scaffold")]
    [InlineData("docs/scaffolding.md", "norm dbcontext scaffold")]
    [InlineData("docs/design-time-migrations.md", "norm migrations add")]
    [InlineData("docs/design-time-migrations.md", "norm database update")]
    [InlineData("docs/design-time-migrations.md", "norm database drop")]
    [InlineData("docs/provider-mobility-contract.md", "norm portability certify")]
    public void Every_command_invocation_is_documented(string docPath, string invocation)
    {
        var doc = File.ReadAllText(Path.Combine(RepoRoot, docPath.Replace('/', Path.DirectorySeparatorChar)));
        Assert.Contains(invocation, doc, StringComparison.Ordinal);
    }
}
