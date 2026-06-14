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
    // Live provider alternate-key many-to-many scaffold parity tests.

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
}
