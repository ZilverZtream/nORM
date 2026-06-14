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
    // Live provider basic many-to-many scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_surrogate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSurrogateManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_surrogate_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSurrogateManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SurrogateAuthorTable, SurrogateBookTable, SurrogateAuthorBookTable },
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, SurrogateAuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, SurrogateBookTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSurrogateManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, SurrogateAuthorBookTable + ".cs")));
                Assert.Contains($"public List<{SurrogateBookTable}> {SurrogateBookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{SurrogateAuthorTable}> {SurrogateAuthorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{SurrogateAuthorBookTable}\", \"AuthorId\", \"BookId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSurrogateManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_many_to_many_with_database_generated_bridge_column_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupGeneratedBridgeManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_generated_bridge_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldGeneratedBridgeManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { GeneratedBridgeStudentTable, GeneratedBridgeCourseTable, GeneratedBridgeStudentCourseTable },
                        OverwriteFiles = false
                    });

                var studentCode = await File.ReadAllTextAsync(Path.Combine(dir, GeneratedBridgeStudentTable + ".cs"));
                var courseCode = await File.ReadAllTextAsync(Path.Combine(dir, GeneratedBridgeCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldGeneratedBridgeManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, GeneratedBridgeStudentCourseTable + ".cs")));
                Assert.Contains($"public List<{GeneratedBridgeCourseTable}> {GeneratedBridgeCourseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{GeneratedBridgeStudentTable}> {GeneratedBridgeStudentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{GeneratedBridgeStudentCourseTable}\", \"StudentId\", \"CourseId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownGeneratedBridgeManyToManyAsync(connection, provider, kind);
            }
        }
    }
}
