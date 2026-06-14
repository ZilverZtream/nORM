#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI generated/composite many-to-many scaffold tests.

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_database_generated_bridge_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliGeneratedStudent" + suffix;
        var courseTable = "CliGeneratedCourse" + suffix;
        var studentCourseTable = "CliGeneratedStudentCourse" + suffix;
        var studentFkName = "FK_CliGeneratedJoin_Student_" + suffix;
        var courseFkName = "FK_CliGeneratedJoin_Course_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_generated_bridge_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupDatabaseGeneratedBridgeManyToMany(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveGeneratedBridgeManyToManyCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, studentCourseTable + ".cs")), "Database-generated bridge column should not force a payload join entity.");
            var studentCode = File.ReadAllText(Path.Combine(output, studentTable + ".cs"));
            var courseCode = File.ReadAllText(Path.Combine(output, courseTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveGeneratedBridgeManyToManyCtx.cs"));

            Assert.Contains($"public List<{courseTable}> {courseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{studentTable}> {studentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{studentCourseTable}\", \"StudentId\", \"CourseId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
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
                CleanupDatabaseGeneratedBridgeManyToMany(cleanup, provider, studentTable, courseTable, studentCourseTable);
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
    public void Dotnet_norm_scaffold_builds_composite_surrogate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliCompositeSurrogateStudent" + suffix;
        var courseTable = "CliCompositeSurrogateCourse" + suffix;
        var studentCourseTable = "CliCompositeSurrogateStudentCourse" + suffix;
        var studentFkName = "FK_CliCompositeSurrogate_Student_" + suffix;
        var courseFkName = "FK_CliCompositeSurrogate_Course_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_surrogate_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeSurrogateKeyManyToMany(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeSurrogateManyToManyCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, studentCourseTable + ".cs")), "Composite generated-surrogate pure join table should scaffold as skip navigations, not as a payload entity.");
            var studentCode = File.ReadAllText(Path.Combine(output, studentTable + ".cs"));
            var courseCode = File.ReadAllText(Path.Combine(output, courseTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeSurrogateManyToManyCtx.cs"));

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
                CleanupCompositeSurrogateKeyManyToMany(cleanup, provider, studentTable, courseTable, studentCourseTable);
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
    public void Dotnet_norm_scaffold_keeps_composite_payload_join_as_explicit_entity_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliCompositePayloadStudent" + suffix;
        var courseTable = "CliCompositePayloadCourse" + suffix;
        var studentCourseTable = "CliCompositePayloadStudentCourse" + suffix;
        var studentFkName = "FK_CliCompositePayload_Student_" + suffix;
        var courseFkName = "FK_CliCompositePayload_Course_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_payload_join_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositePayloadJoin(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositePayloadJoinCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var joinCode = File.ReadAllText(Path.Combine(output, studentCourseTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositePayloadJoinCtx.cs"));

            Assert.Contains("EnrollmentCode", joinCode, StringComparison.Ordinal);
            Assert.Contains($"public {studentTable} {studentTable} {{ get; set; }} = default!;", joinCode, StringComparison.Ordinal);
            Assert.Contains($"public {courseTable} {courseTable} {{ get; set; }} = default!;", joinCode, StringComparison.Ordinal);
            Assert.DoesNotContain($".UsingTable(\"{studentCourseTable}\"", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.StudentTenantId, d.StudentId }", "p => new { p.TenantId, p.StudentId }", studentFkName), contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.CourseTenantId, d.CourseId }", "p => new { p.TenantId, p.CourseId }", courseFkName), contextCode, StringComparison.Ordinal);
            AssertPossibleJoinPayloadDiagnosticWithoutCompositeForeignKey(Path.Combine(output, "nORM.ScaffoldWarnings.json"), studentCourseTable);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupCompositePayloadJoin(cleanup, provider, studentTable, courseTable, studentCourseTable);
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
