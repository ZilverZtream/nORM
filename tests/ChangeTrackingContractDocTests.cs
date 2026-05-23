using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the supportable v1 surface for <c>ChangeTracker</c> as documented in
/// <c>docs/change-tracking.md</c>. Downstream applications depend on each section (tracking
/// modes, entity states, attach/detach, PK mutation rules, owned types, etc.); fail loudly if
/// the doc drops one.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ChangeTrackingContractDocTests
{
    private static string ReadDoc()
    {
        var asmDir = Path.GetDirectoryName(typeof(ChangeTrackingContractDocTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", "change-tracking.md");
        Assert.True(File.Exists(path), $"docs/change-tracking.md not found at {path}");
        return File.ReadAllText(path);
    }

    [Theory]
    [InlineData("Tracking Modes")]
    [InlineData("Entity States")]
    [InlineData("Attach / Detach")]
    [InlineData("Primary Key Mutation")]
    [InlineData("Immutable Entities and Constructor-Bound Properties")]
    [InlineData("Shadow Properties")]
    [InlineData("Owned Types")]
    [InlineData("Relationship Fixup")]
    [InlineData("Notification Tracking")]
    [InlineData("Concurrency Tokens")]
    [InlineData("Cancellation")]
    public void Doc_covers_required_section(string heading)
    {
        var doc = ReadDoc();
        Assert.Contains(heading, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_specifies_default_tracking_mode_is_Tracked()
    {
        var doc = ReadDoc();
        Assert.Contains("Tracked", doc, StringComparison.Ordinal);
        Assert.Contains("default", doc, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Doc_marks_PK_mutation_as_rejected()
    {
        var doc = ReadDoc();
        Assert.Contains("Mutating the PK", doc, StringComparison.Ordinal);
        Assert.Contains("NormUsageException", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Doc_states_AsNoTracking_is_invisible_to_SaveChanges()
    {
        var doc = ReadDoc();
        Assert.Contains("AsNoTracking", doc, StringComparison.Ordinal);
        Assert.Contains("invisible to", doc, StringComparison.OrdinalIgnoreCase);
    }
}
