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
    // Live provider composite many-to-many scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_same_composite_fk_model_shape_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeContext",
                    new ScaffoldOptions { Tables = new[] { CompositeParentTable, CompositeChildTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeChildTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeContext.cs"));

                Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositeParentTable} {CompositeParentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeChildTable}> {CompositeChildTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{CompositeParentTable}>().HasKey(e => new {{ e.TenantId, e.OrderNo }});", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.OrderNo }", "p => new { p.TenantId, p.OrderNo }", CompositeFkName), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_composite_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { CompositeStudentTable, CompositeCourseTable, CompositeStudentCourseTable },
                        OverwriteFiles = false
                    });

                var studentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeStudentTable + ".cs"));
                var courseCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, CompositeStudentCourseTable + ".cs")));
                Assert.Contains($"public List<{CompositeCourseTable}> {CompositeCourseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeStudentTable}> {CompositeStudentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{CompositeStudentCourseTable}\", new[] {{ \"StudentTenantId\", \"StudentId\" }}, new[] {{ \"CourseTenantId\", \"CourseId\" }});", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_keeps_composite_payload_join_as_explicit_entity_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositePayloadJoinAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_payload_join_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositePayloadJoinContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { CompositePayloadStudentTable, CompositePayloadCourseTable, CompositePayloadStudentCourseTable },
                        OverwriteFiles = false
                    });

                var joinCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositePayloadStudentCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositePayloadJoinContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var joinTables = warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray().ToArray();

                Assert.Contains("EnrollmentCode", joinCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositePayloadStudentTable} {CompositePayloadStudentTable} {{ get; set; }} = default!;", joinCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositePayloadCourseTable} {CompositePayloadCourseTable} {{ get; set; }} = default!;", joinCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".UsingTable(\"{CompositePayloadStudentCourseTable}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.StudentTenantId, d.StudentId }", "p => new { p.TenantId, p.StudentId }", CompositePayloadStudentCourseStudentFkName), contextCode, StringComparison.Ordinal);
                Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.CourseTenantId, d.CourseId }", "p => new { p.TenantId, p.CourseId }", CompositePayloadStudentCourseCourseFkName), contextCode, StringComparison.Ordinal);
                Assert.Contains(joinTables, item =>
                    LastTableNameEquals(item.GetProperty("table").GetString(), CompositePayloadStudentCourseTable) &&
                    item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "payload-columns") &&
                    !item.GetProperty("reasons").EnumerateArray().Any(reason => reason.GetString() == "composite-foreign-key"));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositePayloadJoinAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_composite_surrogate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeSurrogateManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_surrogate_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeSurrogateManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { CompositeSurrogateStudentTable, CompositeSurrogateCourseTable, CompositeSurrogateStudentCourseTable },
                        OverwriteFiles = false
                    });

                var studentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeSurrogateStudentTable + ".cs"));
                var courseCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeSurrogateCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeSurrogateManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, CompositeSurrogateStudentCourseTable + ".cs")));
                Assert.Contains($"public List<{CompositeSurrogateCourseTable}> {CompositeSurrogateCourseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeSurrogateStudentTable}> {CompositeSurrogateStudentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{CompositeSurrogateStudentCourseTable}\", new[] {{ \"StudentTenantId\", \"StudentId\" }}, new[] {{ \"CourseTenantId\", \"CourseId\" }});", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeSurrogateManyToManyAsync(connection, provider, kind);
            }
        }
    }
}
