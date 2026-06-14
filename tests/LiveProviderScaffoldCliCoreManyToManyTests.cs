#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI core many-to-many scaffold shape tests.

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_composite_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliCompositeStudent" + suffix;
        var courseTable = "CliCompositeCourse" + suffix;
        var studentCourseTable = "CliCompositeStudentCourse" + suffix;
        var studentFkName = "FK_CliCompositeStudentCourse_Student_" + suffix;
        var courseFkName = "FK_CliCompositeStudentCourse_Course_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeManyToMany(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeManyToManyCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, studentCourseTable + ".cs")), "Pure composite primary-key bridge table should scaffold as skip navigations, not as a payload entity.");
            var studentCode = File.ReadAllText(Path.Combine(output, studentTable + ".cs"));
            var courseCode = File.ReadAllText(Path.Combine(output, courseTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeManyToManyCtx.cs"));

            Assert.Contains($"public List<{courseTable}> {courseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{studentTable}> {studentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{studentCourseTable}\", new[] {{ \"CourseTenantId\", \"CourseId\" }}, new[] {{ \"StudentTenantId\", \"StudentId\" }});", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupCompositeManyToMany(cleanup, provider, studentTable, courseTable, studentCourseTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_many_to_many_referential_actions_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliActionAuthor" + suffix;
        var bookTable = "CliActionBook" + suffix;
        var authorBookTable = "CliActionAuthorBook" + suffix;
        var authorFkName = "FK_CliActionAuthorBook_Author_" + suffix;
        var bookFkName = "FK_CliActionAuthorBook_Book_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_m2m_actions_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupManyToManyReferentialActions(connection, provider, kind, authorTable, bookTable, authorBookTable, authorFkName, bookFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveManyToManyActionCtx " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(authorBookTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = StripDefaultSchemaArguments(File.ReadAllText(Path.Combine(output, "CliLiveManyToManyActionCtx.cs")));
            Assert.False(File.Exists(Path.Combine(output, authorBookTable + ".cs")), "Pure bridge table should scaffold as a many-to-many mapping, not as a payload entity.");
            Assert.Contains($".UsingTable(\"{authorBookTable}\", new[] {{ \"AuthorId\" }}, new[] {{ \"BookId\" }}, ReferentialAction.Cascade, ReferentialAction.Cascade, ReferentialAction.NoAction, ReferentialAction.NoAction);", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupManyToManyReferentialActions(cleanup, provider, authorTable, bookTable, authorBookTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_shared_tenant_alternate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliSharedAlternateAuthor" + suffix;
        var bookTable = "CliSharedAlternateBook" + suffix;
        var authorBookTable = "CliSharedAlternateAuthorBook" + suffix;
        var authorFkName = "FK_CliSharedAlternateAuthorBook_Author_" + suffix;
        var bookFkName = "FK_CliSharedAlternateAuthorBook_Book_" + suffix;
        var bookIndexName = "IX_CliSharedAlternateAuthorBook_Book_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_shared_alternate_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSharedTenantAlternateKeyManyToMany(connection, provider, kind, authorTable, bookTable, authorBookTable, authorFkName, bookFkName, bookIndexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSharedAlternateManyToManyCtx " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(authorBookTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, authorBookTable + ".cs")), "Shared-tenant alternate-key bridge table should scaffold as skip navigations, not as a payload entity.");
            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSharedAlternateManyToManyCtx.cs"));

            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{authorTable}> {authorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
            var expectedUsingTable = kind == ProviderKind.Sqlite
                ? $".UsingTable(\"{authorBookTable}\", new[] {{ \"TenantId\", \"BookIsbn\" }}, new[] {{ \"TenantId\", \"AuthorCode\" }}, p => new {{ p.TenantId, p.Isbn }}, p => new {{ p.TenantId, p.Code }});"
                : $".UsingTable(\"{authorBookTable}\", new[] {{ \"TenantId\", \"AuthorCode\" }}, new[] {{ \"TenantId\", \"BookIsbn\" }}, p => new {{ p.TenantId, p.Code }}, p => new {{ p.TenantId, p.Isbn }});";
            Assert.Contains(expectedUsingTable, StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupSharedTenantAlternateKeyManyToMany(cleanup, provider, authorTable, bookTable, authorBookTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

}
