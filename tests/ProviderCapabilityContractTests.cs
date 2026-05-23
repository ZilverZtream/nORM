using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Provider <see cref="ProviderCapabilities.MinimumServerVersion"/> values must match the
/// public table in <c>docs/provider-capabilities.md</c>. The table is the promise made to
/// users; the runtime is the enforcement (a connection to an older server fails startup
/// validation). They must not drift apart.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ProviderCapabilityContractTests
{
    private static string DocPath()
    {
        var asmDir = Path.GetDirectoryName(typeof(ProviderCapabilityContractTests).Assembly.Location)!;
        // tests/bin/Release/net8.0 -> repo root
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var docPath = Path.Combine(repoRoot, "docs", "provider-capabilities.md");
        Assert.True(File.Exists(docPath), $"Documentation file not found at {docPath}");
        return docPath;
    }

    [Theory]
    [InlineData("SQL Server", typeof(SqlServerProvider), 13, 0)]
    [InlineData("PostgreSQL", typeof(PostgresProvider), 12, 0)]
    [InlineData("MySQL", typeof(MySqlProvider), 8, 0)]
    [InlineData("SQLite", typeof(SqliteProvider), 3, 25)]
    public void Provider_MinimumServerVersion_matches_v1_floor(string label, Type providerType, int major, int minor)
    {
        var provider = (DatabaseProvider)Activator.CreateInstance(providerType, nonPublic: true)!;
        var capabilities = provider.Capabilities;
        Assert.Equal(label, capabilities.ProviderName);
        Assert.NotNull(capabilities.MinimumServerVersion);
        Assert.Equal(major, capabilities.MinimumServerVersion!.Major);
        Assert.Equal(minor, capabilities.MinimumServerVersion.Minor);
    }

    [Theory]
    [InlineData("SQL Server", 13, 0)]
    [InlineData("PostgreSQL", 12, 0)]
    [InlineData("MySQL", 8, 0)]
    [InlineData("SQLite", 3, 25)]
    public void Documented_minimum_version_matches_provider_capabilities(string label, int major, int minor)
    {
        var docContent = File.ReadAllText(DocPath());
        // Match the table row for this provider; tolerate label variations like "Yes/No"
        // and ensure the literal "<major>.<minor>" appears in the Minimum Version column.
        var pattern = $@"\|\s*{Regex.Escape(label)}\s*\|\s*([^\|]+?)\s*\|";
        var match = Regex.Match(docContent, pattern);
        Assert.True(match.Success, $"No provider table row found for '{label}' in docs/provider-capabilities.md");
        var minVersionCell = match.Groups[1].Value;
        Assert.True(
            minVersionCell.Contains($"{major}.{minor}", StringComparison.Ordinal),
            $"Expected '{major}.{minor}' in Minimum Version cell for {label}, got: {minVersionCell}");
    }
}
