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
    public async Task ScaffoldAsync_suppresses_synthetic_check_constraint_names_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSyntheticCheckConstraintAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_synthetic_check_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSyntheticCheckContext",
                    new ScaffoldOptions { Tables = new[] { SyntheticCheckTable }, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSyntheticCheckContext.cs"));

                Assert.Contains($".Entity<{SyntheticCheckTable}>().HasCheckConstraint(", contextCode, StringComparison.Ordinal);
                Assert.Contains($".Entity<{SyntheticCheckTable}>().HasCheckConstraint(\"CK_{SyntheticCheckTable}_", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("CK__", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain(SyntheticCheckTable + "_check", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(SyntheticCheckTable + "_chk_", contextCode, StringComparison.OrdinalIgnoreCase);

                AssertNoProviderOwnedFeatureDiagnostics(dir, SyntheticCheckTable);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSyntheticCheckConstraintAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_suppresses_synthetic_unique_constraint_index_names_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSyntheticUniqueConstraintIndexAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_synthetic_unique_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSyntheticUniqueContext",
                    new ScaffoldOptions { Tables = new[] { SyntheticUniqueTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, SyntheticUniqueTable));

                Assert.Contains("IsUnique = true", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"UX_{SyntheticUniqueTable}_Code\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("UQ__", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain(SyntheticUniqueTable + "_Code_key", entityCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("[Index(\"Code\"", entityCode, StringComparison.OrdinalIgnoreCase);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSyntheticUniqueConstraintIndexAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_named_unique_constraint_index_names_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupNamedUniqueConstraintIndexAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_named_unique_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldNamedUniqueContext",
                    new ScaffoldOptions { Tables = new[] { NamedUniqueTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, NamedUniqueTable));

                Assert.Contains($"[Index(\"{NamedUniqueCodeConstraint}\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{NamedUniqueCompositeConstraint}\", IsUnique = true, Order = 0)]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{NamedUniqueCompositeConstraint}\", IsUnique = true, Order = 1)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain($"UX_{NamedUniqueTable}_Code", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"UX_{NamedUniqueTable}_TenantId_ExternalNo", entityCode, StringComparison.Ordinal);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownNamedUniqueConstraintIndexAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_suppresses_synthetic_fk_constraint_names_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSyntheticForeignKeyRelationshipAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_synthetic_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSyntheticFkContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SyntheticFkParentTable, SyntheticFkChildTable },
                        OverwriteFiles = false
                    });

                var childCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, SyntheticFkChildTable));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSyntheticFkContext.cs"));

                Assert.Contains($"public {SyntheticFkParentTable} {SyntheticFkParentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
                Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, cascadeDelete: false);", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain(".HasForeignKey(d => d.ParentId, p => p.Id, \"", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("sqlite_fk_", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("FK__", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("_fkey", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("_ibfk_", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSyntheticForeignKeyRelationshipAsync(connection, provider, kind);
            }
        }
    }

    private static void AssertNoProviderOwnedFeatureDiagnostics(string outputDirectory, string tableName)
    {
        var warningJsonPath = Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json");
        if (!File.Exists(warningJsonPath))
            return;

        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement
            .GetProperty("providerOwnedSchemaFeatures")
            .EnumerateArray()
            .ToArray();

        Assert.DoesNotContain(providerOwned, item => LastTableNameEquals(item.GetProperty("table").GetString(), tableName));
    }
}
