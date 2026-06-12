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
    // Live provider many-to-many scaffold parity tests.

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

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_shared_tenant_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSharedTenantManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_shared_tenant_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSharedTenantManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SharedTenantStudentTable, SharedTenantCourseTable, SharedTenantStudentCourseTable },
                        OverwriteFiles = false
                    });

                var studentCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedTenantStudentTable + ".cs"));
                var courseCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedTenantCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSharedTenantManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, SharedTenantStudentCourseTable + ".cs")));
                Assert.Contains($"public List<{SharedTenantCourseTable}> {SharedTenantCourseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{SharedTenantStudentTable}> {SharedTenantStudentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{SharedTenantStudentCourseTable}\", new[] {{ \"TenantId\", \"StudentId\" }}, new[] {{ \"TenantId\", \"CourseId\" }});", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSharedTenantManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_shared_tenant_alternate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSharedAlternateKeyManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_shared_alternate_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSharedAlternateManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SharedAlternateAuthorTable, SharedAlternateBookTable, SharedAlternateAuthorBookTable },
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedAlternateAuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, SharedAlternateBookTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSharedAlternateManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, SharedAlternateAuthorBookTable + ".cs")));
                Assert.Contains($"public List<{SharedAlternateBookTable}> {SharedAlternateBookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{SharedAlternateAuthorTable}> {SharedAlternateAuthorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{SharedAlternateAuthorBookTable}\", new[] {{ \"TenantId\", \"AuthorCode\" }}, new[] {{ \"TenantId\", \"BookIsbn\" }}, p => new {{ p.TenantId, p.Code }}, p => new {{ p.TenantId, p.Isbn }});", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSharedAlternateKeyManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_alternate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupAlternateKeyManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_alternate_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldAlternateManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { AlternateAuthorTable, AlternateBookTable, AlternateAuthorBookTable },
                        OverwriteFiles = false
                    });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, AlternateAuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, AlternateBookTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldAlternateManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, AlternateAuthorBookTable + ".cs")));
                Assert.Contains($"public List<{AlternateBookTable}> {AlternateBookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{AlternateAuthorTable}> {AlternateAuthorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{AlternateAuthorBookTable}\", \"AuthorCode\", \"BookIsbn\", p => p.Code, p => p.Isbn);", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownAlternateKeyManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_self_referencing_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSelfReferencingManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_self_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSelfManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SelfPersonTable, SelfPersonRelationshipTable },
                        OverwriteFiles = false
                    });

                Assert.False(File.Exists(Path.Combine(dir, SelfPersonRelationshipTable + ".cs")));
                var personCode = await File.ReadAllTextAsync(Path.Combine(dir, SelfPersonTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSelfManyToManyContext.cs"));

                Assert.Contains("ByMenteeId", personCode, StringComparison.Ordinal);
                Assert.Contains("ByMentorId", personCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{SelfPersonRelationshipTable}\", \"MenteeId\", \"MentorId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSelfReferencingManyToManyAsync(connection, provider, kind);
            }
        }
    }

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
