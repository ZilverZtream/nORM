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
    // Live provider referential-action scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupReferentialActionAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_referential_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldReferentialContext",
                    new ScaffoldOptions { Tables = new[] { ReferentialParentTable, ReferentialChildTable }, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldReferentialContext.cs"));

                Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.SetNull", "ReferentialAction.Cascade", ReferentialFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownReferentialActionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_restrict_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupRestrictReferentialActionAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_restrict_referential_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRestrictReferentialContext",
                    new ScaffoldOptions { Tables = new[] { ReferentialRestrictParentTable, ReferentialRestrictChildTable }, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRestrictReferentialContext.cs"));

                Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.Restrict", "ReferentialAction.Cascade", ReferentialRestrictFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownRestrictReferentialActionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_set_default_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSetDefaultReferentialActionAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_default_referential_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldDefaultReferentialContext",
                    new ScaffoldOptions { Tables = new[] { ReferentialDefaultParentTable, ReferentialDefaultChildTable }, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldDefaultReferentialContext.cs"));

                Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.SetDefault", "ReferentialAction.SetDefault", ReferentialDefaultFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSetDefaultReferentialActionAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_postgres_deferrable_fk_semantics_as_relationship_diagnostic()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider Postgres not configured")) return;

        var (connection, provider) = live!.Value;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var parentTable = "ScaffoldLiveDeferrableParent" + suffix;
        var childTable = "ScaffoldLiveDeferrableChild" + suffix;
        var fkName = "FK_ScaffoldLiveDeferrable_" + suffix;
        await using (connection)
        {
            var parent = provider.Escape(parentTable);
            var child = provider.Escape(childTable);
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, childTable, child));
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, parentTable, parent));
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_deferrable_fk_" + suffix);
            try
            {
                await ExecuteAsync(connection,
                    $"CREATE TABLE {parent} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY)");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {child} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("ParentId")} integer NOT NULL, " +
                    $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({provider.Escape("ParentId")}) REFERENCES {parent} ({provider.Escape("Id")}) DEFERRABLE INITIALLY DEFERRED)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresDeferrableFkContext",
                    new ScaffoldOptions { Tables = new[] { parentTable, childTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, childTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresDeferrableFkContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("public int ParentId { get; set; }", childCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"public {parentTable}", childCode, StringComparison.Ordinal);
                Assert.DoesNotContain(".HasForeignKey(", contextCode, StringComparison.Ordinal);
                var diagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ReferentialAction" &&
                    item.GetProperty("name").GetString() == fkName);
                Assert.Equal("SCF106", diagnostic.GetProperty("code").GetString());
                Assert.Contains("DEFERRABLE", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
                var metadata = diagnostic.GetProperty("metadata");
                Assert.True(metadata.GetProperty("navigationSuppressed").GetBoolean());
                Assert.False(metadata.GetProperty("generatedNavigationSupported").GetBoolean());
                Assert.Equal("referential-action-not-scaffoldable", metadata.GetProperty("reason").GetString());
                Assert.Contains("DEFERRABLE", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, childTable, child));
                await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, parentTable, parent));
            }
        }
    }
}
