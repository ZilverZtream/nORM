using System;
using System.IO;
using System.Xml.Linq;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>NormVersion</c> in <c>Directory.Build.props</c> is the single source of truth for every
/// published artifact. <c>CHANGELOG.md</c> must reference that version (either as a released
/// heading or via an <c>Unreleased</c> section for in-flight RCs) so generated release notes
/// always match the produced packages. The release gate's <c>Assert-CurrentPackageOutput</c>
/// rejects stale <c>.nupkg</c> outputs that do not match this value byte-for-byte.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ChangelogVersionContractTests
{
    private static string RepoRoot()
    {
        var asmDir = Path.GetDirectoryName(typeof(ChangelogVersionContractTests).Assembly.Location)!;
        return Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
    }

    private static string ReadNormVersion()
    {
        var propsPath = Path.Combine(RepoRoot(), "Directory.Build.props");
        Assert.True(File.Exists(propsPath), $"Directory.Build.props not found at {propsPath}");
        var doc = XDocument.Load(propsPath);
        var element = doc.Root?.Element("PropertyGroup")?.Element("NormVersion");
        Assert.NotNull(element);
        var value = element!.Value.Trim();
        Assert.False(string.IsNullOrEmpty(value), "NormVersion is empty in Directory.Build.props");
        return value;
    }

    [Fact]
    public void NormVersion_is_well_formed_semver()
    {
        var version = ReadNormVersion();
        // Major.Minor.Patch optionally followed by -prerelease.identifier
        var pattern = new System.Text.RegularExpressions.Regex(
            @"^\d+\.\d+\.\d+(-[A-Za-z0-9.-]+)?$");
        Assert.True(pattern.IsMatch(version), $"NormVersion '{version}' does not match Major.Minor.Patch[-prerelease]");
    }

    [Fact]
    public void Changelog_references_current_NormVersion_or_has_Unreleased_section()
    {
        var version = ReadNormVersion();
        var changelogPath = Path.Combine(RepoRoot(), "CHANGELOG.md");
        Assert.True(File.Exists(changelogPath), $"CHANGELOG.md not found at {changelogPath}");
        var content = File.ReadAllText(changelogPath);

        var hasUnreleased = content.IndexOf("## Unreleased", StringComparison.OrdinalIgnoreCase) >= 0;
        var hasVersioned = content.Contains(version, StringComparison.Ordinal);

        Assert.True(
            hasUnreleased || hasVersioned,
            $"CHANGELOG.md must either contain an '## Unreleased' section for in-flight work " +
            $"or reference NormVersion '{version}' as a released heading.");
    }

    [Fact]
    public void Release_checklist_documents_version_transitions()
    {
        var path = Path.Combine(RepoRoot(), "docs", "release-checklist.md");
        Assert.True(File.Exists(path));
        var content = File.ReadAllText(path);
        Assert.Contains("Version Transitions", content, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NormVersion", content, StringComparison.Ordinal);
        Assert.Contains("Never reuse a `NormVersion` value", content, StringComparison.Ordinal);
    }
}
