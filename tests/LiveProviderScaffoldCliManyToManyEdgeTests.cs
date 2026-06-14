#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI many-to-many scaffold edge-case tests.

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
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_rejects_nullable_fk_bridge_join_table_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var studentTable = "CliNullStudent" + suffix;
        var courseTable = "CliNullCourse" + suffix;
        var studentCourseTable = "CliNullStudentCourse" + suffix;
        var studentFkName = "FK_CliNullBridge_Student_" + suffix;
        var courseFkName = "FK_CliNullBridge_Course_" + suffix;
        var uniqueIndexName = "UX_CliNullBridge_Pair_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_nullable_bridge_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupNullableBridgeManyToMany(connection, provider, kind, studentTable, courseTable, studentCourseTable, studentFkName, courseFkName, uniqueIndexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveNullableBridgeCtx " +
                $"--table {Quote(studentTable)} " +
                $"--table {Quote(courseTable)} " +
                $"--table {Quote(studentCourseTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, studentCourseTable + ".cs")), "Nullable FK bridge must remain explicit.");
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveNullableBridgeCtx.cs"));
            Assert.DoesNotContain($".UsingTable(\"{studentCourseTable}\"", contextCode, StringComparison.Ordinal);
            AssertPossibleManyToManyDiagnosticReason(
                Path.Combine(output, "nORM.ScaffoldWarnings.json"),
                studentCourseTable,
                "nullable-foreign-key");

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupNullableBridgeManyToMany(cleanup, provider, studentTable, courseTable, studentCourseTable);
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
    public void Dotnet_norm_scaffold_preserves_schema_qualified_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = kind == ProviderKind.Sqlite ? "main" : "CliSchema" + suffix;
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

            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains($"[Table(\"{authorTable}\")]", authorCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Schema = \"main\"", authorCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{authorBookTable}\", \"AuthorId\", \"BookId\");", contextCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains($"[Table(\"{authorTable}\", Schema = \"{schemaName}\")]", authorCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{authorBookTable}\", \"AuthorId\", \"BookId\", schema: \"{schemaName}\");", contextCode, StringComparison.Ordinal);
            }

            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{authorTable}> {authorTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
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
