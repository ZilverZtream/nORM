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
    public async Task ScaffoldAsync_preserves_schema_qualified_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSchemaQualifiedManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_schema_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSchemaManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[]
                        {
                            SchemaName + "." + SchemaAuthorTable,
                            SchemaName + "." + SchemaBookTable,
                            SchemaName + "." + SchemaAuthorBookTable
                        },
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, SchemaAuthorTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSchemaManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, SchemaAuthorBookTable + ".cs")));
                Assert.Contains($"[Table(\"{SchemaAuthorTable}\", Schema = \"{SchemaName}\")]", authorCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{SchemaAuthorBookTable}\", \"AuthorId\", \"BookId\", schema: \"{SchemaName}\");", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSchemaQualifiedManyToManyAsync(connection, provider, kind);
            }
        }
    }
}
