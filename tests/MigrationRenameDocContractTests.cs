using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins README migration guidance against the actual `[RenameColumn]` API. The README used to
/// say property-to-column renames were undetected; the attribute now makes them detectable, and
/// docs must not regress back to the old claim or stop documenting the supported workflow.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class MigrationRenameDocContractTests
{
    private static string ReadReadme()
    {
        var asmDir = Path.GetDirectoryName(typeof(MigrationRenameDocContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var readme = Path.Combine(repoRoot, "README.md");
        Assert.True(File.Exists(readme), $"README.md not found at {readme}");
        return File.ReadAllText(readme);
    }

    [Fact]
    public void Readme_documents_rename_column_attribute_as_supported_workflow()
    {
        var content = ReadReadme();
        Assert.Contains("[RenameColumn(\"TotalCost\")]", content, StringComparison.Ordinal);
        Assert.Contains("using nORM.Mapping;", content, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_does_not_claim_property_renames_are_undetected()
    {
        var content = ReadReadme();
        // Pre-v1 README said "Only property-to-column renames are undetected". The attribute now
        // exists; if this exact claim returns, the docs have regressed.
        Assert.DoesNotContain("property-to-column renames are undetected", content, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_preserves_unannotated_rename_warning()
    {
        var content = ReadReadme();
        // Unannotated renames still produce DROP + ADD; the README must keep warning about this
        // even after promoting [RenameColumn] as the supported workflow.
        Assert.Contains("Unannotated renames", content, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("--force", content, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_preserves_table_rename_manual_warning()
    {
        var content = ReadReadme();
        Assert.Contains("Table renames are not yet auto-detected", content, StringComparison.OrdinalIgnoreCase);
    }
}
