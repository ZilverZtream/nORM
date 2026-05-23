using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the documented provider package architecture (<c>docs/provider-packages.md</c>)
/// against the package-consumer test that inspects the produced <c>.nupkg</c>. Both say: one
/// runtime package, SQL Server + SQLite drivers bundled, PostgreSQL + MySQL drivers loaded by
/// reflection so consumers opt in. Drift between the doc and the nuspec is a support hazard.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProviderPackageArchitectureContractTests
{
    private static string ReadProviderPackagesDoc()
    {
        var asmDir = Path.GetDirectoryName(typeof(ProviderPackageArchitectureContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "docs", "provider-packages.md");
        Assert.True(File.Exists(path), $"docs/provider-packages.md not found at {path}");
        return File.ReadAllText(path);
    }

    [Fact]
    public void Documented_architecture_lists_all_four_providers()
    {
        var doc = ReadProviderPackagesDoc();
        Assert.Contains("SQL Server", doc, StringComparison.Ordinal);
        Assert.Contains("SQLite", doc, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", doc, StringComparison.Ordinal);
        Assert.Contains("MySQL", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Documented_architecture_marks_bundled_drivers()
    {
        var doc = ReadProviderPackagesDoc();
        Assert.Contains("Microsoft.Data.SqlClient", doc, StringComparison.Ordinal);
        Assert.Contains("Microsoft.Data.Sqlite", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Documented_architecture_marks_reflection_loaded_drivers()
    {
        var doc = ReadProviderPackagesDoc();
        // PostgreSQL and MySQL must remain opt-in / reflection-loaded; the docs and the
        // PackageConsumerIntegrationTests both enforce that the runtime nuspec does not
        // transitively pull these in.
        Assert.Contains("Npgsql", doc, StringComparison.Ordinal);
        Assert.True(
            doc.Contains("MySqlConnector", StringComparison.Ordinal) ||
            doc.Contains("MySql.Data", StringComparison.Ordinal),
            "Provider packages doc must name at least one MySQL driver as the consumer install target.");
    }

    [Fact]
    public void Documented_architecture_states_monolithic_package_choice()
    {
        var doc = ReadProviderPackagesDoc();
        // The v1 commitment is one runtime package, not split provider packages.
        Assert.Contains("one runtime package", doc, StringComparison.OrdinalIgnoreCase);
    }
}
