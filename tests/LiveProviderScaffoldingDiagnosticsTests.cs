#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider diagnostics scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_owned_and_keyless_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupWarningDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_warnings_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldWarningContext",
                    new ScaffoldOptions { Tables = new[] { WarningTable, KeylessTable }, OverwriteFiles = false });

                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("MissingPrimaryKey", warnings, StringComparison.Ordinal);
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldWarningContext.cs"));
                var defaultPromotedToModel = contextCode.Contains("HasDefaultValueSql", StringComparison.Ordinal);
                if (!defaultPromotedToModel)
                {
                    Assert.Contains("Provider-Owned Schema Features", warnings, StringComparison.Ordinal);
                    Assert.Contains("Default", warnings, StringComparison.Ordinal);
                }

                var providerOwned = warningJson.RootElement
                    .GetProperty("providerOwnedSchemaFeatures")
                    .EnumerateArray()
                    .ToArray();

                if (defaultPromotedToModel)
                {
                    Assert.Contains(".Property(e => e.Status).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "Default" &&
                        LastTableNameEquals(item.GetProperty("table").GetString(), WarningTable) &&
                        item.GetProperty("name").GetString() == "Status");
                }
                else
                {
                    Assert.Contains(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "Default" &&
                        LastTableNameEquals(item.GetProperty("table").GetString(), WarningTable) &&
                        item.GetProperty("name").GetString() == "Status" &&
                        item.GetProperty("suggestedAction").GetString()!.Contains("default", StringComparison.OrdinalIgnoreCase));
                }

                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), KeylessTable) &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownWarningDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_suppresses_keyless_dependent_fk_navigation_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupKeylessDependentRelationshipAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_keyless_dependent_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldKeylessDependentContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { KeylessDependentParentTable, KeylessDependentTable },
                        OverwriteFiles = false
                    });

                var dependentCode = await File.ReadAllTextAsync(Path.Combine(dir, KeylessDependentTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, KeylessDependentParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldKeylessDependentContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", dependentCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
                Assert.DoesNotContain(KeylessDependentTable + "s", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), KeylessDependentTable) &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownKeylessDependentRelationshipAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_promotes_check_and_computed_metadata_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupFeatureOwnedMetadataAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_feature_owned_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFeatureOwnedContext",
                    new ScaffoldOptions { Tables = new[] { FeatureOwnedTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, FeatureOwnedTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFeatureOwnedContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("NameLength { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(".HasCheckConstraint(", contextCode, StringComparison.Ordinal);
                Assert.Contains(FeatureOwnedCheckName, contextCode, StringComparison.Ordinal);
                Assert.Contains("HasComputedColumnSql(", contextCode, StringComparison.Ordinal);
                Assert.Contains("HasCollation(", contextCode, StringComparison.Ordinal);

                if (File.Exists(warningJsonPath))
                {
                    using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                    var providerOwned = warningJson.RootElement
                        .GetProperty("providerOwnedSchemaFeatures")
                        .EnumerateArray()
                        .ToArray();

                    Assert.DoesNotContain(providerOwned, item =>
                        LastTableNameEquals(item.GetProperty("table").GetString(), FeatureOwnedTable) &&
                        item.GetProperty("kind").GetString() is "CheckConstraint" or "Computed" or "Collation");
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownFeatureOwnedMetadataAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_trigger_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupTriggerDiagnosticsAsync(connection, provider, kind);
            }
            catch (DbException ex)
            {
                await TeardownTriggerDiagnosticsAsync(connection, provider, kind);
                if (Skip.If(true, $"{kind} trigger diagnostics setup is not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_trigger_diag_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldTriggerDiagnosticsContext",
                    new ScaffoldOptions { Tables = new[] { TriggerDiagnosticsTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, TriggerDiagnosticsTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                Assert.Contains("Touched { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.True(File.Exists(warningJsonPath), "Trigger diagnostics must write the scaffold warning JSON report.");

                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                var providerOwned = warningJson.RootElement
                    .GetProperty("providerOwnedSchemaFeatures")
                    .EnumerateArray()
                    .ToArray();

                var triggerDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("code").GetString() == "SCF110" &&
                    item.GetProperty("category").GetString() == "database-object" &&
                    item.GetProperty("kind").GetString() == "Trigger" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), TriggerDiagnosticsTable) &&
                    item.GetProperty("name").GetString() == TriggerDiagnosticsTrigger &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("trigger", StringComparison.OrdinalIgnoreCase));
                var triggerMetadata = triggerDiagnostic.GetProperty("metadata");
                Assert.Equal("Trigger", triggerMetadata.GetProperty("providerObjectKind").GetString());
                Assert.True(LastTableNameEquals(triggerMetadata.GetProperty("table").GetString(), TriggerDiagnosticsTable));
                Assert.Equal(TriggerDiagnosticsTrigger, triggerMetadata.GetProperty("triggerName").GetString());
                Assert.True(triggerMetadata.GetProperty("providerOwnedDdl").GetBoolean());
                Assert.False(triggerMetadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
                Assert.True(triggerMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(triggerMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-owned-trigger", triggerMetadata.GetProperty("reason").GetString());
                switch (kind)
                {
                    case ProviderKind.SqlServer:
                        Assert.Equal("AFTER", triggerMetadata.GetProperty("timing").GetString());
                        Assert.False(triggerMetadata.GetProperty("isDisabled").GetBoolean());
                        Assert.False(triggerMetadata.GetProperty("isInsteadOf").GetBoolean());
                        break;
                    case ProviderKind.Postgres:
                    case ProviderKind.MySql:
                        Assert.Equal("BEFORE", triggerMetadata.GetProperty("timing").GetString());
                        Assert.Equal("INSERT", triggerMetadata.GetProperty("event").GetString());
                        Assert.Equal("ROW", triggerMetadata.GetProperty("orientation").GetString());
                        break;
                    case ProviderKind.Sqlite:
                        Assert.True(triggerMetadata.GetProperty("definitionAvailable").GetBoolean());
                        Assert.Contains("CREATE TRIGGER", triggerMetadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
                        break;
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownTriggerDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

}
