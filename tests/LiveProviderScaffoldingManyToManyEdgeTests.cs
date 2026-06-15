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
    // Live provider many-to-many scaffold edge parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_rejects_filtered_unique_surrogate_join_table_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupFilteredUniqueSurrogateJoinAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_filtered_unique_join_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFilteredUniqueJoinContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { FilteredStudentTable, FilteredCourseTable, FilteredStudentCourseTable },
                        OverwriteFiles = false
                    });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFilteredUniqueJoinContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();

                Assert.True(File.Exists(Path.Combine(dir, FilteredStudentCourseTable + ".cs")));
                Assert.DoesNotContain($".UsingTable(\"{FilteredStudentCourseTable}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(joinTables, item =>
                    LastTableNameEquals(item.GetProperty("table").GetString(), FilteredStudentCourseTable) &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason =>
                        reason.GetString() is "missing-exact-unique-index" or "primary-key-not-exact-bridge-columns"));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownFilteredUniqueSurrogateJoinAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_schema_qualified_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_runtime_m2m_catalog_" + Guid.NewGuid().ToString("N")[..8].ToLowerInvariant()
            : null;
        var originalDatabase = connection.Database;
        await using (connection)
        {
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_schema_catalog_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                if (scratchDatabase is null)
                {
                    await SetupSchemaQualifiedManyToManyAsync(connection, provider, kind);
                }
                else
                {
                    await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                    await ExecuteAsync(connection, $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                    await SetupSurrogateManyToManyAsync(connection, provider, kind);
                }

                var filters = scratchDatabase is null
                    ? new[]
                    {
                        SchemaName + "." + SchemaAuthorTable,
                        SchemaName + "." + SchemaBookTable,
                        SchemaName + "." + SchemaAuthorBookTable
                    }
                    : new[]
                    {
                        scratchDatabase + "." + SurrogateAuthorTable,
                        scratchDatabase + "." + SurrogateBookTable,
                        scratchDatabase + "." + SurrogateAuthorBookTable
                    };
                var authorTable = scratchDatabase is null ? SchemaAuthorTable : SurrogateAuthorTable;
                var authorBookTable = scratchDatabase is null ? SchemaAuthorBookTable : SurrogateAuthorBookTable;
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSchemaManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = filters,
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, authorTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSchemaManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, authorBookTable + ".cs")));
                if (scratchDatabase is null)
                {
                    Assert.Contains($"[Table(\"{SchemaAuthorTable}\", Schema = \"{SchemaName}\")]", authorCode, StringComparison.Ordinal);
                    Assert.Contains($".UsingTable(\"{SchemaAuthorBookTable}\", \"AuthorId\", \"BookId\", schema: \"{SchemaName}\");", contextCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.DoesNotContain("Schema =", authorCode, StringComparison.Ordinal);
                    Assert.Contains($".UsingTable(\"{SurrogateAuthorBookTable}\", \"AuthorId\", \"BookId\");", contextCode, StringComparison.Ordinal);
                }

                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);

                if (scratchDatabase is null)
                {
                    await TeardownSchemaQualifiedManyToManyAsync(connection, provider, kind);
                }
                else
                {
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(originalDatabase))
                            connection.ChangeDatabase(originalDatabase);
                        await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                    }
                    catch
                    {
                        // Best-effort cleanup; test body reports operational failures.
                    }
                }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_rejects_nullable_fk_bridge_join_table_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupNullableBridgeManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_nullable_bridge_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldNullableBridgeContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { NullableBridgeStudentTable, NullableBridgeCourseTable, NullableBridgeStudentCourseTable },
                        OverwriteFiles = false
                    });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldNullableBridgeContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();

                Assert.True(File.Exists(Path.Combine(dir, NullableBridgeStudentCourseTable + ".cs")));
                Assert.DoesNotContain($".UsingTable(\"{NullableBridgeStudentCourseTable}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(joinTables, item =>
                    LastTableNameEquals(item.GetProperty("table").GetString(), NullableBridgeStudentCourseTable) &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "nullable-foreign-key") &&
                    item.GetProperty("metadata").GetProperty("nullableForeignKeyColumns").EnumerateArray().Any(column => column.GetString() == "StudentId"));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownNullableBridgeManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_rejects_provider_owned_bridge_join_table_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupProviderOwnedBridgeManyToManyAsync(connection, provider, kind);
            }
            catch (DbException ex)
            {
                await TeardownProviderOwnedBridgeManyToManyAsync(connection, provider, kind);
                if (Skip.If(true, $"{kind} provider-owned bridge setup is not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_owned_bridge_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderOwnedBridgeContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { ProviderOwnedBridgeAuthorTable, ProviderOwnedBridgeBookTable, ProviderOwnedBridgeAuthorBookTable },
                        OverwriteFiles = false
                    });

                var joinCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderOwnedBridgeAuthorBookTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderOwnedBridgeContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", joinCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".UsingTable(\"{ProviderOwnedBridgeAuthorBookTable}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "Trigger" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), ProviderOwnedBridgeAuthorBookTable) &&
                    item.GetProperty("name").GetString() == ProviderOwnedBridgeTrigger);
                Assert.Contains(joinTables, item =>
                    LastTableNameEquals(item.GetProperty("table").GetString(), ProviderOwnedBridgeAuthorBookTable) &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "provider-owned-write-blocking-schema") &&
                    item.GetProperty("metadata").GetProperty("providerOwnedWriteBlockingSchema").GetBoolean());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderOwnedBridgeManyToManyAsync(connection, provider, kind);
            }
        }
    }

}
