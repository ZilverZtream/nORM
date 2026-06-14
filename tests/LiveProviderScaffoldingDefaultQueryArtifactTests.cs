#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_default_discovery_emits_table_and_view_query_artifacts_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_runtime_default_view_" + Guid.NewGuid().ToString("N")[..8].ToLowerInvariant()
            : null;
        var originalDatabase = connection.Database;
        await using (connection)
        {
            await SetupDefaultQueryArtifactDiscoveryAsync(connection, provider, kind, scratchDatabase);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_default_query_artifact_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldDefaultQueryArtifactContext",
                    DefaultQueryArtifactScaffoldOptions(kind));

                var baseCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, DefaultQueryArtifactBaseTable));
                var viewCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, DefaultQueryArtifactView));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldDefaultQueryArtifactContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.DoesNotContain("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
                Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{DefaultQueryArtifactBaseTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.Contains($"IQueryable<{DefaultQueryArtifactView}>", contextCode, StringComparison.OrdinalIgnoreCase);
                AssertDefaultQueryArtifactSchema(kind, baseCode, viewCode);
                AssertDefaultQueryArtifactCommentDocumentation(kind, viewCode);

                Assert.True(File.Exists(warningJsonPath));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), DefaultQueryArtifactView));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownDefaultQueryArtifactDiscoveryAsync(connection, provider, kind, scratchDatabase, originalDatabase);
            }
        }
    }

    private static ScaffoldOptions DefaultQueryArtifactScaffoldOptions(ProviderKind kind)
        => kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? new ScaffoldOptions { Schemas = new[] { DefaultQueryArtifactSchemaName }, OverwriteFiles = false }
            : new ScaffoldOptions { OverwriteFiles = false };

    private static void AssertDefaultQueryArtifactSchema(ProviderKind kind, string baseCode, string viewCode)
    {
        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            Assert.Contains($"Schema = \"{DefaultQueryArtifactSchemaName}\"", baseCode, StringComparison.Ordinal);
            Assert.Contains($"Schema = \"{DefaultQueryArtifactSchemaName}\"", viewCode, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("Schema =", baseCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Schema =", viewCode, StringComparison.Ordinal);
        }
    }

    private static void AssertDefaultQueryArtifactCommentDocumentation(ProviderKind kind, string viewCode)
    {
        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            Assert.Contains("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// Name &lt;view&gt; &amp; details", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// <remarks>Maps to column Name</remarks>", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Name <view> & details", viewCode, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("/// Maps to column Name", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
        }
    }
}
