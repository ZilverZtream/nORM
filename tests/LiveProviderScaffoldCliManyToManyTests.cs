#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_alternate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliLiveAltAuthor" + suffix;
        var bookTable = "CliLiveAltBook" + suffix;
        var authorBookTable = "CliLiveAltAuthorBook" + suffix;
        var authorFkName = "FK_CliLiveAltAuthorBook_Author_" + suffix;
        var bookFkName = "FK_CliLiveAltAuthorBook_Book_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_alt_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupAlternateKeyManyToMany(connection, provider, kind, authorTable, bookTable, authorBookTable, authorFkName, bookFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveAlternateManyToManyCtx " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(authorBookTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveAlternateManyToManyCtx.cs"));

            Assert.False(File.Exists(Path.Combine(output, authorBookTable + ".cs")), "Pure alternate-key bridge table should scaffold as a many-to-many mapping, not as a payload entity.");
            Assert.Contains($"List<{bookTable}>", authorCode, StringComparison.Ordinal);
            Assert.Contains($"List<{authorTable}>", bookCode, StringComparison.Ordinal);
            Assert.Contains(".UsingTable(", contextCode, StringComparison.Ordinal);
            Assert.Contains(authorBookTable, contextCode, StringComparison.Ordinal);
            Assert.Contains("\"AuthorCode\", \"BookIsbn\"", contextCode, StringComparison.Ordinal);
            Assert.Contains("p => p.Code", contextCode, StringComparison.Ordinal);
            Assert.Contains("p => p.Isbn", contextCode, StringComparison.Ordinal);
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
                CleanupAlternateKeyManyToMany(cleanup, provider, authorTable, bookTable, authorBookTable);
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
    public void Dotnet_norm_scaffold_builds_self_referencing_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var personTable = "CliSelfPerson" + suffix;
        var relationshipTable = "CliSelfRelationship" + suffix;
        var mentorFkName = "FK_CliSelfRelationship_Mentor_" + suffix;
        var menteeFkName = "FK_CliSelfRelationship_Mentee_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_self_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSelfReferencingManyToMany(connection, provider, kind, personTable, relationshipTable, mentorFkName, menteeFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSelfManyToManyCtx " +
                $"--table {Quote(personTable)} " +
                $"--table {Quote(relationshipTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, relationshipTable + ".cs")), "Self-referencing pure join table should scaffold as skip navigations, not as a payload entity.");
            var personCode = File.ReadAllText(Path.Combine(output, personTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSelfManyToManyCtx.cs"));

            Assert.Contains("ByMenteeId", personCode, StringComparison.Ordinal);
            Assert.Contains("ByMentorId", personCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{relationshipTable}\", \"MenteeId\", \"MentorId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
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
                CleanupSelfReferencingManyToMany(cleanup, provider, personTable, relationshipTable);
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
    public void Dotnet_norm_scaffold_builds_surrogate_key_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliSurrogateAuthor" + suffix;
        var bookTable = "CliSurrogateBook" + suffix;
        var authorBookTable = "CliSurrogateAuthorBook" + suffix;
        var authorFkName = "FK_CliSurrogateAuthorBook_Author_" + suffix;
        var bookFkName = "FK_CliSurrogateAuthorBook_Book_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_surrogate_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSurrogateKeyManyToMany(connection, provider, kind, authorTable, bookTable, authorBookTable, authorFkName, bookFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSurrogateManyToManyCtx " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(authorBookTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, authorBookTable + ".cs")), "Generated-surrogate pure join table should scaffold as skip navigations, not as a payload entity.");
            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSurrogateManyToManyCtx.cs"));

            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{authorTable}> {authorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{authorBookTable}\", \"AuthorId\", \"BookId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
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
                CleanupSurrogateKeyManyToMany(cleanup, provider, authorTable, bookTable, authorBookTable);
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

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    public void Dotnet_norm_scaffold_rejects_filtered_unique_surrogate_join_table_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliFilteredStudent" + suffix;
        var courseTable = "CliFilteredCourse" + suffix;
        var studentCourseTable = "CliFilteredStudentCourse" + suffix;
        var studentFkName = "FK_CliFilteredStudentCourse_Student_" + suffix;
        var courseFkName = "FK_CliFilteredStudentCourse_Course_" + suffix;
        var uniqueIndexName = "UX_CliFilteredStudentCourse_ActivePair_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_filtered_unique_join_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupFilteredUniqueSurrogateJoin(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName, uniqueIndexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFilteredUniqueJoinCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, studentCourseTable + ".cs")), "Filtered unique surrogate join must remain explicit.");
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFilteredUniqueJoinCtx.cs"));
            Assert.DoesNotContain($".UsingTable(\"{studentCourseTable}\"", contextCode, StringComparison.Ordinal);
            AssertPossibleManyToManyDiagnosticReason(
                Path.Combine(output, "nORM.ScaffoldWarnings.json"),
                studentCourseTable,
                "missing-exact-unique-index",
                "primary-key-not-exact-bridge-columns");

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupFilteredUniqueSurrogateJoin(cleanup, provider, studentTable, courseTable, studentCourseTable);
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
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    public void Dotnet_norm_scaffold_preserves_schema_qualified_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = "CliSchema" + suffix;
        var authorTable = "CliSchemaAuthor" + suffix;
        var bookTable = "CliSchemaBook" + suffix;
        var authorBookTable = "CliSchemaAuthorBook" + suffix;
        var authorFkName = "FK_CliSchemaAuthorBook_Author_" + suffix;
        var bookFkName = "FK_CliSchemaAuthorBook_Book_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_schema_m2m_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSchemaQualifiedManyToMany(connection, provider, kind, schemaName, authorTable, bookTable, authorBookTable, authorFkName, bookFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSchemaManyToManyCtx " +
                $"--table {Quote(schemaName + "." + authorTable)} " +
                $"--table {Quote(schemaName + "." + bookTable)} " +
                $"--table {Quote(schemaName + "." + authorBookTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, authorBookTable + ".cs")), "Schema-qualified pure bridge table should scaffold as skip navigations.");
            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSchemaManyToManyCtx.cs"));

            Assert.Contains($"[Table(\"{authorTable}\", Schema = \"{schemaName}\")]", authorCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{authorTable}> {authorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{authorBookTable}\", \"AuthorId\", \"BookId\", schema: \"{schemaName}\");", contextCode, StringComparison.Ordinal);
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
                CleanupSchemaQualifiedManyToMany(cleanup, provider, kind, schemaName, authorTable, bookTable, authorBookTable);
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