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
    // Live provider index scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificIndexesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderIndexContext.cs"));
                var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
                var warnings = File.Exists(warningPath) ? await File.ReadAllTextAsync(warningPath) : string.Empty;
                using var warningJson = File.Exists(warningJsonPath)
                    ? JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath))
                    : JsonDocument.Parse("{\"providerOwnedSchemaFeatures\":[]}");
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains($"[Index(\"{ProviderPartialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{ProviderDescendingIndex}\", IsDescending = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("PartialIndex", warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderPartialIndex, warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderDescendingIndex, warnings, StringComparison.Ordinal);

                if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
                {
                    Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderPartialExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.Contains("filterSql:", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionDescendingIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("name").GetString() == ProviderExpressionDescendingIndex);
                }

                if (kind is ProviderKind.Postgres)
                {
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionLiteralDescIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "DescendingIndex" &&
                        item.GetProperty("name").GetString() == ProviderExpressionLiteralDescIndex);
                }

                if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                {
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("IncludedColumnIndex", warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderIncludedIndex, warnings, StringComparison.Ordinal);
                }

                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PartialIndex" &&
                    item.GetProperty("name").GetString() == ProviderPartialIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderIncludedIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "DescendingIndex" &&
                    item.GetProperty("name").GetString() == ProviderDescendingIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_prefix_index_without_emitting_normal_index()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlPrefixIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_prefix_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlPrefixIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderPrefixIndex, entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{ProviderFullPrefixIndex}\")]", entityCode, StringComparison.Ordinal);
                var prefix = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PrefixIndex" &&
                    item.GetProperty("name").GetString() == ProviderPrefixIndex);
                Assert.Equal("SCF117", prefix.GetProperty("code").GetString());
                Assert.Equal("index", prefix.GetProperty("category").GetString());
                var prefixMetadata = prefix.GetProperty("metadata");
                var prefixColumn = Assert.Single(prefixMetadata.GetProperty("prefixColumns").EnumerateArray());
                Assert.Equal("Name", prefixColumn.GetProperty("name").GetString());
                Assert.Equal(8, prefixColumn.GetProperty("prefixLength").GetInt32());
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PrefixIndex" &&
                    item.GetProperty("name").GetString() == ProviderFullPrefixIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_expression_index_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupMySqlExpressionIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
                if (Skip.If(true, $"MySQL expression indexes are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_expression_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlExpressionIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldMySqlExpressionIndexContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains("LOWER", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.Contains(provider.Escape("Score"), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported MySQL expression indexes should scaffold as provider-bound expression-index metadata.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_reports_provider_specific_index_access_methods_as_provider_owned(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupProviderSpecificAccessMethodIndexAsync(connection, provider, kind);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
                if (Skip.If(true, $"{kind} provider-specific index access method is not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_specific_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderSpecificIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderSpecificIndexContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderSpecificIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderSpecificIndex}\"", contextCode, StringComparison.Ordinal);
                var providerSpecific = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
                Assert.Equal("index", providerSpecific.GetProperty("category").GetString());
                var providerSpecificMetadata = providerSpecific.GetProperty("metadata");
                if (kind is ProviderKind.SqlServer)
                {
                    Assert.Equal("SQL Server", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Contains("COLUMNSTORE", providerSpecificMetadata.GetProperty("indexType").GetString(), StringComparison.OrdinalIgnoreCase);
                }
                else if (kind is ProviderKind.Postgres)
                {
                    Assert.Equal("PostgreSQL", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Equal("hash", providerSpecificMetadata.GetProperty("accessMethod").GetString());
                    Assert.Contains("USING hash", providerSpecificMetadata.GetProperty("indexSql").GetString(), StringComparison.OrdinalIgnoreCase);
                }
                else if (kind is ProviderKind.MySql)
                {
                    Assert.Equal("MySQL", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Equal("FULLTEXT", providerSpecificMetadata.GetProperty("indexType").GetString());
                }

                if (kind is ProviderKind.Postgres)
                {
                    var expression = Assert.Single(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                        item.GetProperty("name").GetString() == ProviderSpecificIndex);
                    Assert.Equal("SCF112", expression.GetProperty("code").GetString());
                    Assert.Equal("index", expression.GetProperty("category").GetString());
                }
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_postgres_expression_index_with_include_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresExpressionIncludedIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_expression_include_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresExpressionIncludeContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresExpressionIncludeContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderExpressionIncludedIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderExpressionIncludedIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                    item.GetProperty("name").GetString() == ProviderExpressionIncludedIndex);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderExpressionIncludedIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_null_sort_order_index_metadata()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresProviderSpecificBtreeOptionIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_null_sort_order_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresNullSortOrderContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Index(\"{ProviderSpecificIndex}\", NullSortOrder = IndexNullSortOrder.First)]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported PostgreSQL NULLS FIRST/LAST column indexes should not produce provider-owned warnings.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_postgres_expression_btree_key_options_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupPostgresExpressionBtreeOptionIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
                if (Skip.If(true, $"PostgreSQL expression B-tree key options are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_expression_btree_options_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresExpressionBtreeOptionsContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresExpressionBtreeOptionsContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderSpecificIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderSpecificIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                var providerSpecific = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
                Assert.Equal("index", providerSpecific.GetProperty("category").GetString());
                Assert.Contains("provider-specific key options", providerSpecific.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
                var metadata = providerSpecific.GetProperty("metadata");
                Assert.Equal("PostgreSQL", metadata.GetProperty("provider").GetString());
                Assert.Equal("btree", metadata.GetProperty("accessMethod").GetString());
                Assert.True(metadata.GetProperty("hasNonDefaultOperatorClass").GetBoolean());
                Assert.False(metadata.GetProperty("hasNullsNotDistinct").GetBoolean());
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_nulls_not_distinct_unique_index_metadata()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupPostgresNullsNotDistinctUniqueIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
                if (Skip.If(true, $"PostgreSQL NULLS NOT DISTINCT indexes are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_nulls_not_distinct_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresNullsNotDistinctContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Index(\"{ProviderSpecificIndex}\", IsUnique = true, NullsNotDistinct = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("ProviderSpecificIndex", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported PostgreSQL NULLS NOT DISTINCT indexes should not produce provider-owned warnings.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

}
