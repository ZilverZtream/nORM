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

    private static string RepoRoot()
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(DocPath())!, ".."));

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

    [Theory]
    [InlineData(typeof(SqlServerProvider), true, true, true, true, true, true, new[] { "Microsoft.Data.SqlClient" })]
    [InlineData(typeof(PostgresProvider), true, true, false, true, true, true, new[] { "Npgsql" })]
    [InlineData(typeof(MySqlProvider), true, true, false, true, true, false, new[] { "MySqlConnector", "MySql.Data" })]
    [InlineData(typeof(SqliteProvider), true, true, false, false, true, false, new[] { "Microsoft.Data.Sqlite" })]
    public void Provider_capability_flags_match_documented_matrix(
        Type providerType, bool json, bool temporal, bool nativeTemporalDdl,
        bool nativeBulk, bool savepoints, bool nativeTenantSession, string[] drivers)
    {
        var provider = (DatabaseProvider)Activator.CreateInstance(providerType, nonPublic: true)!;
        var capabilities = provider.Capabilities;
        Assert.Equal(json, capabilities.SupportsJson);
        Assert.Equal(temporal, capabilities.SupportsTemporalVersioning);
        Assert.Equal(nativeTemporalDdl, provider.SupportsProviderNativeTemporalTables);
        Assert.Equal(nativeBulk, capabilities.SupportsNativeBulkInsert);
        Assert.Equal(savepoints, capabilities.SupportsSavepoints);
        Assert.Equal(nativeTenantSession, provider.SupportsNativeTenantSessionContext);
        foreach (var driver in drivers)
            Assert.Contains(driver, capabilities.Notes, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SQL Server", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Microsoft.Data.SqlClient")]
    [InlineData("PostgreSQL", "Yes", "Yes", "No", "Yes", "Yes", "Yes", "Npgsql")]
    [InlineData("MySQL", "Yes", "Yes", "No", "Yes", "Yes", "No", "MySqlConnector")]
    [InlineData("SQLite", "JSON1-dependent", "Yes", "No", "No", "Yes", "No", "Microsoft.Data.Sqlite")]
    public void Documented_capability_cells_match_provider_table(
        string label, string json, string temporal, string nativeTemporalDdl,
        string nativeBulk, string savepoints, string nativeTenantSession, string driver)
    {
        var row = File.ReadAllLines(DocPath())
            .FirstOrDefault(l => l.StartsWith($"| {label} |", StringComparison.Ordinal));
        Assert.False(row is null, $"No provider table row found for '{label}' in docs/provider-capabilities.md");
        var cells = row!.Trim().Trim('|').Split('|').Select(c => c.Trim()).ToArray();
        // Provider | Minimum Version | Notes | JSON | Temporal | Native Temporal DDL |
        // Native Bulk Insert | Savepoints | Native Tenant Session | Driver
        Assert.Equal(10, cells.Length);
        Assert.Equal(json, cells[3]);
        Assert.Equal(temporal, cells[4]);
        Assert.Equal(nativeTemporalDdl, cells[5]);
        Assert.Equal(nativeBulk, cells[6]);
        Assert.Equal(savepoints, cells[7]);
        Assert.Equal(nativeTenantSession, cells[8]);
        Assert.Contains(driver, cells[9], StringComparison.Ordinal);
    }

    [Fact]
    public void Provider_floor_feature_evidence_is_documented_for_release_review()
    {
        var docContent = File.ReadAllText(DocPath());
        var releaseGates = File.ReadAllText(Path.Combine(Path.GetDirectoryName(DocPath())!, "release-gates.md"));

        Assert.Contains("## Floor Feature Evidence", docContent, StringComparison.Ordinal);
        Assert.Contains("dotnet-norm portability certify", docContent, StringComparison.Ordinal);
        Assert.Contains("actual server versions", docContent, StringComparison.Ordinal);
        Assert.Contains("Descriptor-only reports", docContent, StringComparison.Ordinal);
        Assert.Contains("not be treated as old-version proof", docContent, StringComparison.Ordinal);
        Assert.Contains("provider-target-capabilities.json", docContent, StringComparison.Ordinal);

        foreach (var required in new[]
        {
            "SQL Server 2016",
            "PostgreSQL 12",
            "MySQL 8.0",
            "SQLite 3.25",
            "JSON_VALUE",
            "jsonb",
            "JSON_EXTRACT",
            "json_extract",
            "ROW_NUMBER",
            "OUTPUT",
            "RETURNING",
            "LAST_INSERT_ID",
            "AUTOINCREMENT",
            "sp_rename",
            "ALTER TABLE RENAME COLUMN",
            "SAVE TRANSACTION",
            "SAVEPOINT",
            "IF NOT EXISTS",
            "ON CONFLICT DO NOTHING",
            "INSERT IGNORE",
            "INSERT OR IGNORE",
            "nORM-managed temporal history/triggers",
            "native bulk insert",
            "native tenant session context"
        })
        {
            Assert.Contains(required, docContent, StringComparison.Ordinal);
        }

        Assert.Contains("declared provider floor-feature ledger", releaseGates, StringComparison.Ordinal);
        Assert.Contains("not actual version proof", releaseGates, StringComparison.Ordinal);
        Assert.Contains("actual server-version decisions", releaseGates, StringComparison.Ordinal);
    }

    [Fact]
    public void Rc_artifact_manifest_records_declared_floor_features_without_fake_actual_versions()
    {
        var root = RepoRoot();
        var manifestScript = File.ReadAllText(Path.Combine(root, "eng", "rc-artifact-manifest.ps1"));

        Assert.Contains("New-ProviderFloorEvidence", manifestScript, StringComparison.Ordinal);
        Assert.Contains("RequiredFloorFeatures", manifestScript, StringComparison.Ordinal);
        Assert.Contains("ActualServerVersion = $null", manifestScript, StringComparison.Ordinal);
        Assert.Contains("use dotnet-norm portability certify target reports", manifestScript, StringComparison.Ordinal);

        foreach (var required in new[]
        {
            "MinimumVersion '13.0'",
            "MinimumVersion '12.0'",
            "MinimumVersion '8.0'",
            "MinimumVersion '3.25'",
            "SQL Server 2016",
            "PostgreSQL 12",
            "MySQL 8.0",
            "SQLite 3.25",
            "JSON_VALUE JSON translation",
            "jsonb JSON translation",
            "JSON_EXTRACT JSON translation",
            "JSON1 json_extract JSON translation",
            "ROW_NUMBER/window translation",
            "OUTPUT generated-value retrieval",
            "RETURNING generated-value retrieval",
            "LAST_INSERT_ID generated-value retrieval",
            "AUTOINCREMENT generated-value retrieval",
            "sp_rename column rename",
            "ALTER TABLE RENAME COLUMN",
            "SAVE TRANSACTION savepoints",
            "SAVEPOINT savepoints",
            "IF NOT EXISTS idempotent join-table insert",
            "ON CONFLICT DO NOTHING idempotent join-table insert",
            "INSERT IGNORE idempotent join-table insert",
            "INSERT OR IGNORE idempotent join-table insert",
            "nORM-managed temporal history/triggers",
            "native bulk insert",
            "native tenant session context"
        })
        {
            Assert.Contains(required, manifestScript, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Portability_certify_target_probe_exercises_representative_floor_features()
    {
        var program = File.ReadAllText(Path.Combine(RepoRoot(), "src", "dotnet-norm", "Program.Portability.Probes.cs"));

        foreach (var required in new[]
        {
            "JSON translation",
            "window function",
            "generated-value retrieval",
            "rename column DDL",
            "savepoint",
            "idempotent insert/ignore",
            "ROW_NUMBER() OVER",
            "JSON_VALUE",
            "json_extract",
            "JSON_EXTRACT",
            "jsonb",
            "OUTPUT INSERTED",
            "RETURNING",
            "LAST_INSERT_ID",
            "last_insert_rowid",
            "RENAME COLUMN",
            "SAVE TRANSACTION",
            "SAVEPOINT",
            "IF NOT EXISTS",
            "ON CONFLICT DO NOTHING",
            "INSERT IGNORE",
            "INSERT OR IGNORE",
            "provider-target-capability"
        })
        {
            Assert.Contains(required, program, StringComparison.Ordinal);
        }
    }
}
