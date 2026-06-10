#nullable enable

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", "LiveProvider")]
[Collection("LiveProviderScaffolding")]
public sealed class LiveProviderScaffoldCliParityTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_builds_composite_shared_tenant_many_to_many_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var userTable = "CliLiveUser_" + suffix;
        var tagTable = "CliLiveTag_" + suffix;
        var userTagTable = "CliLiveUserTag_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_scaffold_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeSharedTenantManyToMany(connection, provider, kind, userTable, tagTable, userTagTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCtx " +
                $"--table {Quote(userTable)} " +
                $"--table {Quote(tagTable)} " +
                $"--table {Quote(userTagTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCtx.cs"));
            Assert.Contains(".UsingTable(", contextCode, StringComparison.Ordinal);
            Assert.Contains(userTagTable, contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, userTagTable + ".cs")), "Pure shared-tenant bridge table should scaffold as a many-to-many mapping, not as a payload entity.");

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupCompositeSharedTenantManyToMany(cleanup, provider, userTable, tagTable, userTagTable);
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
    public void Dotnet_norm_scaffold_builds_mixed_single_fk_and_many_to_many_shape_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliShapeAuthor" + suffix;
        var bookTable = "CliShapeBook" + suffix;
        var labelTable = "CliShapeLabel" + suffix;
        var bookLabelTable = "CliShapeBookLabel" + suffix;
        var bookAuthorFkName = "FK_CliShapeBook_Author_" + suffix;
        var bookLabelBookFkName = "FK_CliShapeBookLabel_Book_" + suffix;
        var bookLabelLabelFkName = "FK_CliShapeBookLabel_Label_" + suffix;
        var bookAuthorTitleIndex = "IX_CliShapeBook_Author_Title_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_mixed_shape_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupMixedSingleForeignKeyAndManyToMany(connection, provider, kind, authorTable, bookTable, labelTable, bookLabelTable, bookAuthorFkName, bookLabelBookFkName, bookLabelLabelFkName, bookAuthorTitleIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveMixedShapeCtx " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(labelTable)} " +
                $"--table {Quote(bookLabelTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.False(File.Exists(Path.Combine(output, bookLabelTable + ".cs")), "Book-label bridge table should scaffold as skip navigations, not as a payload entity.");
            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var labelCode = File.ReadAllText(Path.Combine(output, labelTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveMixedShapeCtx.cs"));

            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", authorCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{bookAuthorTitleIndex}\", Order = 0)]", bookCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{bookAuthorTitleIndex}\", Order = 1)]", bookCode, StringComparison.Ordinal);
            Assert.Contains($"public {authorTable} {authorTable} {{ get; set; }} = default!;", bookCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{labelTable}> {labelTable}s {{ get; set; }} = new();", bookCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{bookTable}> {bookTable}s {{ get; set; }} = new();", labelCode, StringComparison.Ordinal);
            Assert.Contains($".HasMany(p => p.{bookTable}s)", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{authorTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.AuthorId", "p => p.Id", bookAuthorFkName), contextCode, StringComparison.Ordinal);
            Assert.Contains($".HasMany<{labelTable}>(p => p.{labelTable}s)", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithMany(p => p.{bookTable}s)", contextCode, StringComparison.Ordinal);
            Assert.Contains($".UsingTable(\"{bookLabelTable}\", \"BookId\", \"LabelId\");", StripDefaultSchemaArguments(contextCode), StringComparison.Ordinal);
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
                CleanupMixedSingleForeignKeyAndManyToMany(cleanup, provider, bookTable, authorTable, labelTable, bookLabelTable);
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
    public void Dotnet_norm_scaffold_suppresses_synthetic_fk_constraint_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliSyntheticFkParent" + suffix;
        var childTable = "CliSyntheticFkChild" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_synthetic_fk_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupUnnamedForeignKeyRelationship(connection, provider, kind, parentTable, childTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSyntheticFkCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSyntheticFkCtx.cs"));

            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id", contextCode, StringComparison.Ordinal);
            if (kind is ProviderKind.Sqlite or ProviderKind.SqlServer)
            {
                Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, cascadeDelete: false);", contextCode, StringComparison.Ordinal);
            }
            if (kind == ProviderKind.Sqlite)
            {
                Assert.DoesNotContain("sqlite_fk_", contextCode, StringComparison.Ordinal);
            }
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
                CleanupReferentialActionRelationship(cleanup, provider, childTable, parentTable);
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
    public void Dotnet_norm_scaffold_builds_composite_primary_key_fk_model_shape_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliCompositeFkParent" + suffix;
        var childTable = "CliCompositeFkChild" + suffix;
        var parentPkName = "PK_CliCompositeFkParent_" + suffix;
        var fkName = "FK_CliCompositeFkChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_fk_shape_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositePrimaryKeyForeignKey(connection, provider, kind, parentTable, childTable, parentPkName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeFkShapeCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeFkShapeCtx.cs"));

            Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{childTable}> {childTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.Contains($"mb.Entity<{parentTable}>().HasKey(e => new {{ e.TenantId, e.OrderNo }}, \"{parentPkName}\");", contextCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains($"mb.Entity<{parentTable}>().HasKey(e => new {{ e.TenantId, e.OrderNo }});", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain(parentPkName, contextCode, StringComparison.Ordinal);
            }
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.OrderNo }", "p => new { p.TenantId, p.OrderNo }", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupCompositePrimaryKeyForeignKey(cleanup, provider, parentTable, childTable);
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

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_database_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = Guid.NewGuid().ToString("N")[..8].ToLowerInvariant();
        var customerTable = "cli_live_customer_" + suffix;
        var orderLineTable = "cli_live_order_line_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_database_names_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupUseDatabaseNames(connection, provider, kind, customerTable, orderLineTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDatabaseNamesCtx " +
                "--use-database-names " +
                "--no-pluralize " +
                $"--table {Quote(customerTable)} " +
                $"--table {Quote(orderLineTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var customerCode = File.ReadAllText(Path.Combine(output, customerTable + ".cs"));
            var orderLineCode = File.ReadAllText(Path.Combine(output, orderLineTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDatabaseNamesCtx.cs"));

            Assert.Contains($"public partial class {customerTable}", customerCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {orderLineTable}", orderLineCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) customer_id \{ get; set; \}", customerCode);
            Assert.Contains("public string display_name { get; set; } = default!;", customerCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) order_line_id \{ get; set; \}", orderLineCode);
            Assert.Matches(@"public (int|long) customer_id \{ get; set; \}", orderLineCode);
            Assert.Contains("public string SKU { get; set; } = default!;", orderLineCode, StringComparison.Ordinal);
            Assert.Contains("public string? @class { get; set; }", orderLineCode, StringComparison.Ordinal);
            Assert.Contains("public string? has_space { get; set; }", orderLineCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{customerTable}> {customerTable}", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{orderLineTable}> {orderLineTable}", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("CliLiveCustomer", customerCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderLineId", orderLineCode, StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupUseDatabaseNames(cleanup, provider, customerTable, orderLineTable);
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
    public void Dotnet_norm_scaffold_generates_valid_unique_identifiers_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix().ToLowerInvariant();
        var generatedSuffix = char.ToUpperInvariant(suffix[0]) + suffix[1..];
        var invalidIdentifierTable = "cli-live-identifier-" + suffix;
        var collisionDashTable = "cli-live-collision-" + suffix;
        var collisionUnderscoreTable = "cli_live_collision_" + suffix;
        var invalidIdentifierEntity = "CliLiveIdentifier" + generatedSuffix;
        var collisionEntity = "CliLiveCollision" + generatedSuffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_identifiers_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupIdentifierCollisionScaffold(connection, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveIdentifierCtx " +
                $"--table {Quote(invalidIdentifierTable)} " +
                $"--table {Quote(collisionDashTable)} " +
                $"--table {Quote(collisionUnderscoreTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var invalidIdentifierCode = File.ReadAllText(Path.Combine(output, invalidIdentifierEntity + ".cs"));
            var firstCollisionPath = Path.Combine(output, collisionEntity + ".cs");
            var secondCollisionPath = Path.Combine(output, collisionEntity + "2.cs");
            Assert.True(File.Exists(firstCollisionPath));
            Assert.True(File.Exists(secondCollisionPath));

            var firstCollisionCode = File.ReadAllText(firstCollisionPath);
            var secondCollisionCode = File.ReadAllText(secondCollisionPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveIdentifierCtx.cs"));

            Assert.Contains($"public partial class {invalidIdentifierEntity}", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{invalidIdentifierTable}\"", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"1st-name\")]", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string _1stName { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long)\? HasSpace \{ get; set; \}", invalidIdentifierCode);
            Assert.Contains("public string FirstName { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string FirstName2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string ToString2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string Equals2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.DoesNotContain("public string ToString {", invalidIdentifierCode, StringComparison.Ordinal);

            Assert.Contains($"public partial class {collisionEntity}", firstCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {collisionEntity}2", secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{collisionDashTable}\"", firstCollisionCode + secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{collisionUnderscoreTable}\"", firstCollisionCode + secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{collisionEntity}> {collisionEntity}s", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{collisionEntity}2> {collisionEntity}2s", contextCode, StringComparison.Ordinal);
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
                CleanupIdentifierCollisionScaffold(cleanup, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);
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
    public void Dotnet_norm_scaffold_respects_project_namespace_context_dir_and_nullable_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "LiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);

            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("DbContextOptions options = null", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_reads_directory_build_props_project_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProps" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_props_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsLiveProject.csproj");
        var connectionName = "LiveProps" + suffix;
        var userSecretsId = "norm-live-props-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                $$"""
                <Project>
                  <PropertyGroup>
                    <RootNamespace>Inherited.Project.Namespace</RootNamespace>
                    <Nullable>enable</Nullable>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProjectWithoutMetadata(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLivePropsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLivePropsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Inherited.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Inherited.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Inherited.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string? Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_project_metadata_overrides_directory_build_props_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectOverrides" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_overrides_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "ProjectOverridesLiveProject.csproj");
        var connectionName = "LiveProjectOverrides" + suffix;
        var inheritedUserSecretsId = "norm-live-inherited-" + Guid.NewGuid().ToString("N");
        var projectUserSecretsId = "norm-live-project-" + Guid.NewGuid().ToString("N");
        var inheritedUserSecretsFile = GetUserSecretsFilePathForLiveTest(inheritedUserSecretsId);
        var projectUserSecretsFile = GetUserSecretsFilePathForLiveTest(projectUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(inheritedUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(projectUserSecretsFile)!);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                $$"""
                <Project>
                  <PropertyGroup>
                    <AssemblyName>Inherited.Assembly.Namespace</AssemblyName>
                    <Nullable>enable</Nullable>
                    <UserSecretsId>{{inheritedUserSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProjectWithAssemblyMetadata(root, projectPath, projectUserSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                inheritedUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=InheritedUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectOverridesCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectOverridesCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Project.Assembly.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Project.Assembly.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Project.Assembly.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Inherited.Assembly.Namespace", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Inherited.Assembly.Namespace", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=InheritedUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(inheritedUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(projectUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_accepts_project_directory_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectDir" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_dir_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "DirectoryLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectDir)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectDirectoryCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectDirectoryCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);

            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_accepts_project_and_force_short_aliases_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectForce" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_force_alias_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "ForceAliasLiveProject.csproj");
        var contextName = "CliLiveProjectForceAliasCtx";
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var output = Path.Combine(projectDir, "Models");
            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, tableName + ".cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(output, contextName + ".cs"), "stale context", Encoding.UTF8);

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"-p {Quote(projectPath)} " +
                "-o Models " +
                "-f " +
                $"--context {contextName} " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, contextName + ".cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.DoesNotContain("stale entity", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("stale context", contextCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_accepts_short_aliases_and_qualified_context_namespace_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveAliasCtx" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_alias_context_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "AliasContextLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectPath)} " +
                "-o Models " +
                "-n Live.Custom.Entities " +
                "-c Ignored.Context.Namespace.CliLiveAliasCtx " +
                "--context-namespace Live.Custom.Contexts " +
                $"-t {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveAliasCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Custom.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Custom.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Custom.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveAliasCtx", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Ignored.Context.Namespace", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_infers_current_directory_project_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_project_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentProjectCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_inferred_current_directory_project_reads_named_connection_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentNamed" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_named_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentNamedLiveProject.csproj");
        var connectionName = "LiveCurrentNamed" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentNamedCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentNamedCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_inferred_current_directory_project_user_secrets_override_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_secrets_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentSecretsLiveProject.csproj");
        var connectionName = "LiveCurrentSecrets" + suffix;
        var userSecretsId = "norm-live-current-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=InferredProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentSecretsCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=InferredProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_no_project_reads_named_connection_current_directory_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirConfig" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_config_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirConfig" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirConfigCtx " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirConfigCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_no_project_pass_through_environment_selects_current_directory_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_env_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=CurrentDirectoryBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=CurrentDirectoryBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_uses_dotnet_ef_config_startup_named_connection_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfConfig" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_config_" + kind + "_" + suffix);
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);

            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/Model",
                  "startupProject": "src/Startup",
                  "context": "Configured.Live.Contexts.ConfiguredLiveCtx",
                  "framework": "net8.0",
                  "configuration": "Release",
                  "runtime": "win-x64",
                  "verbose": false,
                  "noColor": true,
                  "prefixOutput": false
                }
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Startup.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "LiveConfigDb": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:LiveConfigDb")} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "ConfiguredLiveCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Live.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ConfiguredLiveCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_cli_options_override_dotnet_ef_config_defaults_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfConfigOverride" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_config_override_" + kind + "_" + suffix);
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var connectionName = "LiveConfigOverride" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "missing/ConfiguredModel",
                  "startupProject": "missing/ConfiguredStartup",
                  "context": "Configured.Should.Not.Win.ConfiguredCtx",
                  "framework": "net6.0",
                  "configuration": "Debug",
                  "runtime": "linux-x64",
                  "verbose": true,
                  "noColor": true,
                  "prefixOutput": true
                }
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Explicit.Startup.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context Explicit.Live.Contexts.CliLiveConfigOverrideCtx " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveConfigOverrideCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            Assert.False(File.Exists(Path.Combine(output, "ConfiguredCtx.cs")));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Explicit.Live.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveConfigOverrideCtx", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Configured.Should.Not.Win", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_named_connection_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_project_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var connectionName = "LiveStartupProject" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"-s {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupProjectCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupProjectCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_directory_named_connection_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupDir" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_project_dir_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupDirectoryApp.csproj");
        var connectionName = "LiveStartupDirectory" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Directory.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectDir)} " +
                "--output-dir Models " +
                "--context CliLiveStartupProjectDirectoryCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupProjectDirectoryCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupProjectDirectoryCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_named_connection_shorthand_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupShort" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_short_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupShortApp.csproj");
        var connectionName = "LiveStartupShort" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProject(root, startupProjectPath);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupShortCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupShortCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_user_secrets_override_target_user_secrets_and_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_secrets_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupSecretsApp.csproj");
        var connectionName = "LiveStartupSecrets" + suffix;
        var targetUserSecretsId = "norm-live-target-" + Guid.NewGuid().ToString("N");
        var startupUserSecretsId = "norm-live-startup-" + Guid.NewGuid().ToString("N");
        var targetUserSecretsFile = GetUserSecretsFilePathForLiveTest(targetUserSecretsId);
        var startupUserSecretsFile = GetUserSecretsFilePathForLiveTest(startupUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
            WriteLiveScaffoldProject(root, modelProjectPath, targetUserSecretsId);
            WriteLiveScaffoldProject(root, startupProjectPath, startupUserSecretsId);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                targetUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupSecretsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupSecretsCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_omitted_context_uses_named_connection_leaf_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedContext" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_context_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedContextLiveProject.csproj");
        const string connectionName = "ReportingContext";
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "ReportingContext.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ReportingContext", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("ReportingContextContext", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_named_connection_user_secrets_override_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveUserSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_user_secrets_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "UserSecretsLiveProject.csproj");
        var connectionName = "LiveUserSecrets" + suffix;
        var userSecretsId = "norm-live-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveUserSecretsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveUserSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_named_connection_shorthand_reads_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedShort" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_short_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedShortLiveProject.csproj");
        var connectionName = "LiveNamedShort" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveNamedShortCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveNamedShortCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvLiveProject.csproj");
        var connectionName = "LiveNamedEnv" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ARealScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            Environment.SetEnvironmentVariable(environmentKey, connectionString);
            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveNamedEnvCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveNamedEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ARealScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentKey, null);
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_project_user_secrets_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEnvSecret" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_env_secret_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "EnvSecretLiveProject.csproj");
        var connectionName = "LiveEnvSecret" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        var userSecretsId = "norm-live-env-secret-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveEnvSecretCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    [environmentKey] = connectionString
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveEnvSecretCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_startup_and_target_user_secrets_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupEnvSecret" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_env_secret_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupEnvSecretApp.csproj");
        var connectionName = "LiveStartupEnvSecret" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        var targetUserSecretsId = "norm-live-target-env-secret-" + Guid.NewGuid().ToString("N");
        var startupUserSecretsId = "norm-live-startup-env-secret-" + Guid.NewGuid().ToString("N");
        var targetUserSecretsFile = GetUserSecretsFilePathForLiveTest(targetUserSecretsId);
        var startupUserSecretsFile = GetUserSecretsFilePathForLiveTest(startupUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
            WriteLiveScaffoldProject(root, modelProjectPath, targetUserSecretsId);
            WriteLiveScaffoldProject(root, startupProjectPath, startupUserSecretsId);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                targetUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupEnvSecretCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    [environmentKey] = connectionString
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupEnvSecretCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_pass_through_environment_selects_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLivePassEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_pass_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "PassEnvironmentLiveProject.csproj");
        var connectionName = "LivePassEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ARealScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLivePassEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLivePassEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ARealScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_pass_through_environment_selects_startup_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupPassEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_pass_env_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupPassEnvironmentApp.csproj");
        var connectionName = "LiveStartupPassEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            WriteLiveScaffoldProject(root, startupProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectProductionConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupPassEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupPassEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectProductionConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_startup_project_dotnet_environment_selects_startup_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupDotnetEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_dotnet_env_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupDotnetEnvironmentApp.csproj");
        var connectionName = "LiveStartupDotnetEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            WriteLiveScaffoldProject(root, startupProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.Development.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectDevelopmentConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.Development.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupDotnetEnvCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    ["ASPNETCORE_ENVIRONMENT"] = null,
                    ["DOTNET_ENVIRONMENT"] = "Development"
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupDotnetEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectDevelopmentConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_ambient_environment_selects_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveAmbientEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ambient_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "AmbientEnvironmentLiveProject.csproj");
        var connectionName = "LiveAmbientEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=BaseScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProductionScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Staging.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveAmbientEnvCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    ["ASPNETCORE_ENVIRONMENT"] = "Staging",
                    ["DOTNET_ENVIRONMENT"] = null
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveAmbientEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=BaseScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProductionScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
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
    public void Dotnet_norm_scaffold_enforces_output_safety_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveSafety" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_output_safety_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var baseCommand =
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSafetyCtx " +
                $"--table {Quote(tableName)}";

            var dryRun = RunCli(baseCommand + " --dry-run --json", root);
            Assert.True(dryRun.ExitCode == 0,
                $"CLI dry run failed with exit code {dryRun.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{dryRun.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{dryRun.Stderr}");
            using (var dryRunJson = JsonDocument.Parse(dryRun.Stdout))
            {
                var json = dryRunJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.True(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            Assert.False(Directory.Exists(output));

            var scaffold = RunCli(baseCommand, root);
            Assert.True(scaffold.ExitCode == 0,
                $"CLI scaffold failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveSafetyCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            var originalEntity = File.ReadAllText(entityPath);

            var noOverwrite = RunCli(baseCommand + " --no-overwrite", root);
            Assert.NotEqual(0, noOverwrite.ExitCode);
            Assert.Contains("already exists", noOverwrite.Stderr + noOverwrite.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalEntity, File.ReadAllText(entityPath));

            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"), "# stale", Encoding.UTF8);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""", Encoding.UTF8);

            var force = RunCli(baseCommand + " --force --json", root);
            Assert.True(force.ExitCode == 0,
                $"CLI force scaffold failed with exit code {force.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{force.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{force.Stderr}");
            using (var forceJson = JsonDocument.Parse(force.Stdout))
            {
                var json = forceJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

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
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
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
    public void Dotnet_norm_scaffold_accepts_csv_and_multi_value_table_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var csvTableOne = "CliLiveCsvOne" + suffix;
        var csvTableTwo = "CliLiveCsvTwo" + suffix;
        var multiTableOne = "CliLiveMultiOne" + suffix;
        var multiTableTwo = "CliLiveMultiTwo" + suffix;
        var skippedTable = "CliLiveFilterSkip" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_table_filter_values_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupTableFilterValuesScaffold(
                    connection,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTableFilterValuesCtx " +
                $"--tables {Quote(csvTableOne + "," + csvTableTwo)} " +
                $"--table {multiTableOne} {multiTableTwo}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, csvTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, csvTableTwo + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableTwo + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveTableFilterValuesCtx.cs"));
            Assert.Contains($"IQueryable<{csvTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{csvTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
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
                CleanupTableFilterValuesScaffold(
                    cleanup,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
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
    public void Dotnet_norm_scaffold_accepts_csv_and_multi_value_schema_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var csvSchemaOne = "CliSchemaCsvOne" + suffix;
        var csvSchemaTwo = "CliSchemaCsvTwo" + suffix;
        var multiSchemaOne = "CliSchemaMultiOne" + suffix;
        var multiSchemaTwo = "CliSchemaMultiTwo" + suffix;
        var skippedSchema = "CliSchemaSkip" + suffix;
        var csvTableOne = "CliSchemaCsvOneTable" + suffix;
        var csvTableTwo = "CliSchemaCsvTwoTable" + suffix;
        var multiTableOne = "CliSchemaMultiOneTable" + suffix;
        var multiTableTwo = "CliSchemaMultiTwoTable" + suffix;
        var skippedTable = "CliSchemaSkipTable" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_schema_filter_values_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_schema_filter_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupSchemaFilterValuesScaffold(
                    connection,
                    provider,
                    kind,
                    csvSchemaOne,
                    csvSchemaTwo,
                    multiSchemaOne,
                    multiSchemaTwo,
                    skippedSchema,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }

            var schemaArguments = kind switch
            {
                ProviderKind.Sqlite => "--schemas main --schema main",
                ProviderKind.MySql => $"--schemas {Quote(scratchDatabase!)} --schema {Quote(scratchDatabase!)}",
                _ => $"--schemas {Quote(csvSchemaOne + "," + csvSchemaTwo)} --schema {multiSchemaOne} {multiSchemaTwo}"
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSchemaFilterValuesCtx " +
                schemaArguments,
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, csvTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, csvTableTwo + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableTwo + ".cs")));
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSchemaFilterValuesCtx.cs"));
            Assert.Contains($"IQueryable<{csvTableOne}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{csvTableTwo}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{multiTableOne}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{multiTableTwo}>", contextCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{csvSchemaOne}\"", File.ReadAllText(Path.Combine(output, csvTableOne + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{csvSchemaTwo}\"", File.ReadAllText(Path.Combine(output, csvTableTwo + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{multiSchemaOne}\"", File.ReadAllText(Path.Combine(output, multiTableOne + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{multiSchemaTwo}\"", File.ReadAllText(Path.Combine(output, multiTableTwo + ".cs")), StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain("Schema =", File.ReadAllText(Path.Combine(output, csvTableOne + ".cs")), StringComparison.Ordinal);
                Assert.DoesNotContain("Schema =", File.ReadAllText(Path.Combine(output, multiTableOne + ".cs")), StringComparison.Ordinal);
            }

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
                if (scratchDatabase is null)
                {
                    CleanupSchemaFilterValuesScaffold(
                        cleanup,
                        provider,
                        kind,
                        csvSchemaOne,
                        csvSchemaTwo,
                        multiSchemaOne,
                        multiSchemaTwo,
                        skippedSchema,
                        csvTableOne,
                        csvTableTwo,
                        multiTableOne,
                        multiTableTwo,
                        skippedTable);
                }
                else
                {
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                }
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
    public void Dotnet_norm_scaffold_unions_schema_and_table_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = "CliFilterUnionSchema" + suffix;
        var schemaTable = "CliFilterUnionSchemaTable" + suffix;
        var explicitTable = "CliFilterUnionExplicit" + suffix;
        var skippedTable = "CliFilterUnionSkip" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_filter_union_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_filter_union_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupSchemaAndTableFilterUnionScaffold(
                    connection,
                    provider,
                    kind,
                    schemaName,
                    schemaTable,
                    explicitTable,
                    skippedTable);
            }

            var schemaArgument = kind switch
            {
                ProviderKind.Sqlite => "--schema main",
                ProviderKind.MySql => $"--schema {Quote(scratchDatabase!)}",
                _ => $"--schema {Quote(schemaName)}"
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSchemaTableUnionCtx " +
                $"{schemaArgument} " +
                $"--table {Quote(explicitTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, schemaTable + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, explicitTable + ".cs")));
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var schemaEntityCode = File.ReadAllText(Path.Combine(output, schemaTable + ".cs"));
            var explicitEntityCode = File.ReadAllText(Path.Combine(output, explicitTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSchemaTableUnionCtx.cs"));

            Assert.Contains($"IQueryable<{schemaTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{explicitTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{schemaName}\"", schemaEntityCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{(kind == ProviderKind.SqlServer ? "dbo" : "public")}\"", explicitEntityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain("Schema =", schemaEntityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Schema =", explicitEntityCode, StringComparison.Ordinal);
            }

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
                if (scratchDatabase is null)
                {
                    CleanupSchemaAndTableFilterUnionScaffold(
                        cleanup,
                        provider,
                        kind,
                        schemaName,
                        schemaTable,
                        explicitTable,
                        skippedTable);
                }
                else
                {
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                }
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
    public void Dotnet_norm_scaffold_accepts_no_onconfiguring_data_annotations_and_no_pluralize_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfSwitch" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_switches_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveEfSwitchCtx " +
                "--no-onconfiguring " +
                "--data-annotations " +
                "--no-pluralize " +
                "--json " +
                "--verbose " +
                "--no-color " +
                "--prefix-output " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using (var document = JsonDocument.Parse(scaffold.Stdout))
            {
                var json = document.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveEfSwitchCtx.cs"));
            Assert.DoesNotContain("OnConfiguring", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(connectionString, contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("[Table(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Key]", entityCode, StringComparison.Ordinal);
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
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
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

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_generates_composite_fk_to_unique_index_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliUniqueParent" + suffix;
        var childTable = "CliUniqueChild" + suffix;
        var indexName = "UX_CliUniqueParent_TenantExternal_" + suffix;
        var fkName = "FK_CliUniqueChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_unique_fk_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeForeignKeyToUniqueIndex(connection, provider, kind, parentTable, childTable, indexName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeUniqueFkCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeUniqueFkCtx.cs"));

            Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{childTable}> {childTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.ExternalNo }", "p => new { p.TenantId, p.ExternalNo }", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupCompositeForeignKeyToUniqueIndex(cleanup, provider, parentTable, childTable);
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
    public void Dotnet_norm_scaffold_generates_role_named_composite_fks_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var accountTable = "CliRoleAccount" + suffix;
        var transferTable = "CliRoleTransfer" + suffix;
        var accountIndex = "UX_CliRoleAccount_TenantAccount_" + suffix;
        var primaryFkName = "FK_CliRoleTransfer_Primary_" + suffix;
        var backupFkName = "FK_CliRoleTransfer_Backup_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_roles_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeRoleForeignKeys(connection, provider, kind, accountTable, transferTable, accountIndex, primaryFkName, backupFkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeRoleCtx " +
                $"--table {Quote(accountTable)} " +
                $"--table {Quote(transferTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var accountCode = File.ReadAllText(Path.Combine(output, accountTable + ".cs"));
            var transferCode = File.ReadAllText(Path.Combine(output, transferTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeRoleCtx.cs"));

            Assert.Contains($"public List<{transferTable}> {transferTable}sByBackupAccountNo {{ get; set; }} = new();", accountCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{transferTable}> {transferTable}sByPrimaryAccountNo {{ get; set; }} = new();", accountCode, StringComparison.Ordinal);
            Assert.Contains($"public {accountTable} BackupAccount {{ get; set; }} = default!;", transferCode, StringComparison.Ordinal);
            Assert.Contains($"public {accountTable} PrimaryAccount {{ get; set; }} = default!;", transferCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {accountTable} Tenant", transferCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"{transferTable}sByTenantId", accountCode, StringComparison.Ordinal);
            Assert.Contains($".HasMany(p => p.{transferTable}sByBackupAccountNo)", contextCode, StringComparison.Ordinal);
            Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.BackupAccountNo }", "p => new { p.TenantId, p.AccountNo }", backupFkName), contextCode, StringComparison.Ordinal);
            Assert.Contains($".HasMany(p => p.{transferTable}sByPrimaryAccountNo)", contextCode, StringComparison.Ordinal);
            Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.PrimaryAccountNo }", "p => new { p.TenantId, p.AccountNo }", primaryFkName), contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain($".UsingTable(\"{transferTable}\"", contextCode, StringComparison.Ordinal);
            AssertPossibleJoinPayloadDiagnostic(Path.Combine(output, "nORM.ScaffoldWarnings.json"), transferTable);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupCompositeRoleForeignKeys(cleanup, provider, accountTable, transferTable);
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
    public void Dotnet_norm_scaffold_generates_role_named_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliRoleOneParent" + suffix;
        var profileTable = "CliRoleOneProfile" + suffix;
        var primaryFkName = "FK_CliRoleOne_Profile_Primary_" + suffix;
        var backupFkName = "FK_CliRoleOne_Profile_Backup_" + suffix;
        var primaryIndexName = "UX_CliRoleOne_Profile_Primary_" + suffix;
        var backupIndexName = "UX_CliRoleOne_Profile_Backup_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_role_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRoleNamedOneToOneForeignKeys(connection, provider, kind, parentTable, profileTable, primaryFkName, backupFkName, primaryIndexName, backupIndexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRoleOneToOneCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(profileTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var profileCode = File.ReadAllText(Path.Combine(output, profileTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRoleOneToOneCtx.cs"));

            Assert.Contains($"public {profileTable}? {profileTable}ByBackupAccountId {{ get; set; }}", parentCode, StringComparison.Ordinal);
            Assert.Contains($"public {profileTable}? {profileTable}ByPrimaryAccountId {{ get; set; }}", parentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{profileTable}>", parentCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{backupIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{primaryIndexName}\", IsUnique = true)]", profileCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(BackupAccountId))]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} BackupAccount {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(PrimaryAccountId))]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} PrimaryAccount {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{profileTable}ByBackupAccountId)", contextCode, StringComparison.Ordinal);
            Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.BackupAccountId", "p => p.Id", backupFkName), contextCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{profileTable}ByPrimaryAccountId)", contextCode, StringComparison.Ordinal);
            Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.PrimaryAccountId", "p => p.Id", primaryFkName), contextCode, StringComparison.Ordinal);
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
                CleanupRoleNamedOneToOneForeignKeys(cleanup, provider, parentTable, profileTable);
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
    public void Dotnet_norm_scaffold_generates_required_and_optional_unique_dependent_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var requiredParentTable = "CliUniqueParent" + suffix;
        var requiredProfileTable = "CliUniqueProfile" + suffix;
        var optionalParentTable = "CliOptionalUniqueParent" + suffix;
        var optionalProfileTable = "CliOptionalUniqueProfile" + suffix;
        var requiredFkName = "FK_CliUniqueProfile_Parent_" + suffix;
        var optionalFkName = "FK_CliOptionalUniqueProfile_Parent_" + suffix;
        var requiredIndexName = "UX_CliUniqueProfile_Parent_" + suffix;
        var optionalIndexName = "UX_CliOptionalUniqueProfile_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_unique_dependent_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRequiredAndOptionalUniqueDependentForeignKeys(
                    connection,
                    provider,
                    kind,
                    requiredParentTable,
                    requiredProfileTable,
                    optionalParentTable,
                    optionalProfileTable,
                    requiredFkName,
                    optionalFkName,
                    requiredIndexName,
                    optionalIndexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveUniqueDependentOneToOneCtx " +
                $"--table {Quote(requiredParentTable)} " +
                $"--table {Quote(requiredProfileTable)} " +
                $"--table {Quote(optionalParentTable)} " +
                $"--table {Quote(optionalProfileTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var requiredParentCode = File.ReadAllText(Path.Combine(output, requiredParentTable + ".cs"));
            var requiredProfileCode = File.ReadAllText(Path.Combine(output, requiredProfileTable + ".cs"));
            var optionalParentCode = File.ReadAllText(Path.Combine(output, optionalParentTable + ".cs"));
            var optionalProfileCode = File.ReadAllText(Path.Combine(output, optionalProfileTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveUniqueDependentOneToOneCtx.cs"));

            Assert.Contains($"public {requiredProfileTable}? {requiredProfileTable} {{ get; set; }}", requiredParentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{requiredProfileTable}>", requiredParentCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) ParentId \{ get; set; \}", requiredProfileCode);
            Assert.Contains("[ForeignKey(nameof(ParentId))]", requiredProfileCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{requiredIndexName}\", IsUnique = true)]", requiredProfileCode, StringComparison.Ordinal);
            Assert.Contains($"public {requiredParentTable} {requiredParentTable} {{ get; set; }} = default!;", requiredProfileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{requiredProfileTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{requiredParentTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentId", "p => p.Id", requiredFkName), contextCode, StringComparison.Ordinal);

            Assert.Contains($"public {optionalProfileTable}? {optionalProfileTable} {{ get; set; }}", optionalParentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{optionalProfileTable}>", optionalParentCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long)\? ParentId \{ get; set; \}", optionalProfileCode);
            Assert.Contains("[ForeignKey(nameof(ParentId))]", optionalProfileCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{optionalIndexName}\", IsUnique = true)]", optionalProfileCode, StringComparison.Ordinal);
            Assert.Contains($"public {optionalParentTable}? {optionalParentTable} {{ get; set; }}", optionalProfileCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {optionalParentTable} {optionalParentTable} {{ get; set; }} = default!;", optionalProfileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{optionalProfileTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{optionalParentTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentId", "p => p.Id", optionalFkName), contextCode, StringComparison.Ordinal);

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
                CleanupRequiredAndOptionalUniqueDependentForeignKeys(
                    cleanup,
                    provider,
                    requiredParentTable,
                    requiredProfileTable,
                    optionalParentTable,
                    optionalProfileTable);
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
    public void Dotnet_norm_scaffold_generates_shared_primary_key_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliSharedPkParent" + suffix;
        var profileTable = "CliSharedPkProfile" + suffix;
        var fkName = "FK_CliSharedPkProfile_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_shared_pk_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSharedPrimaryKeyForeignKey(connection, provider, kind, parentTable, profileTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSharedPkOneToOneCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(profileTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var profileCode = File.ReadAllText(Path.Combine(output, profileTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSharedPkOneToOneCtx.cs"));

            Assert.Contains($"public {profileTable}? {profileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{profileTable}>", parentCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(Id))]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{profileTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{parentTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.Id", "p => p.Id", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupSharedPrimaryKeyForeignKey(cleanup, provider, parentTable, profileTable);
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
    public void Dotnet_norm_scaffold_generates_composite_unique_dependent_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliCompositeParent" + suffix;
        var profileTable = "CliCompositeProfile" + suffix;
        var parentIndexName = "UX_CliCompositeParent_TenantAccount_" + suffix;
        var profileIndexName = "UX_CliCompositeProfile_TenantAccount_" + suffix;
        var fkName = "FK_CliCompositeProfile_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_composite_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupCompositeUniqueDependentForeignKey(connection, provider, kind, parentTable, profileTable, parentIndexName, profileIndexName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCompositeOneToOneCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(profileTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var profileCode = File.ReadAllText(Path.Combine(output, profileTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveCompositeOneToOneCtx.cs"));

            Assert.Contains($"public {profileTable}? {profileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{profileTable}>", parentCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) TenantId \{ get; set; \}", profileCode);
            Assert.Matches(@"public (int|long) AccountNo \{ get; set; \}", profileCode);
            Assert.Contains($"[Index(\"{profileIndexName}\", IsUnique = true, Order = 0)]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{profileIndexName}\", IsUnique = true, Order = 1)]", profileCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{profileTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{parentTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.AccountNo }", "p => new { p.TenantId, p.AccountNo }", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupCompositeUniqueDependentForeignKey(cleanup, provider, parentTable, profileTable);
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
    public void Dotnet_norm_scaffold_generates_optional_composite_unique_dependent_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliOptCompositeParent" + suffix;
        var profileTable = "CliOptCompositeProfile" + suffix;
        var parentIndexName = "UX_CliOptCompositeParent_TenantAccount_" + suffix;
        var profileIndexName = "UX_CliOptCompositeProfile_TenantAccount_" + suffix;
        var fkName = "FK_CliOptCompositeProfile_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_opt_composite_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupOptionalCompositeUniqueDependentForeignKey(connection, provider, kind, parentTable, profileTable, parentIndexName, profileIndexName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveOptionalCompositeOneToOneCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(profileTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var profileCode = File.ReadAllText(Path.Combine(output, profileTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveOptionalCompositeOneToOneCtx.cs"));

            Assert.Contains($"public {profileTable}? {profileTable} {{ get; set; }}", parentCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{profileTable}>", parentCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) TenantId \{ get; set; \}", profileCode);
            Assert.Matches(@"public (int|long)\? AccountNo \{ get; set; \}", profileCode);
            Assert.Contains($"[Index(\"{profileIndexName}\", IsUnique = true, Order = 0)]", profileCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{profileIndexName}\", IsUnique = true, Order = 1)]", profileCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable}? {parentTable} {{ get; set; }}", profileCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {parentTable} {parentTable} {{ get; set; }} = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{profileTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains($".WithOne(d => d.{parentTable})", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => new { d.TenantId, d.AccountNo }", "p => new { p.TenantId, p.AccountNo }", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupOptionalCompositeUniqueDependentForeignKey(cleanup, provider, parentTable, profileTable);
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
    public void Dotnet_norm_scaffold_suppresses_keyless_dependent_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliKeylessParent" + suffix;
        var dependentTable = "CliKeylessDependent" + suffix;
        var fkName = "FK_CliKeylessDependent_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_keyless_dependent_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeylessDependentRelationship(connection, provider, kind, parentTable, dependentTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveKeylessDependentCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(dependentTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var dependentCode = File.ReadAllText(Path.Combine(output, dependentTable + ".cs"));
            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveKeylessDependentCtx.cs"));

            Assert.Contains("[ReadOnlyEntity]", dependentCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
            Assert.DoesNotContain(dependentTable + "s", parentCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
            AssertRelationshipDependentKeyDiagnostic(Path.Combine(output, "nORM.ScaffoldWarnings.json"), dependentTable);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeylessDependentRelationship(cleanup, provider, parentTable, dependentTable);
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
    public void Dotnet_norm_scaffold_promotes_safe_feature_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveFeatureOwned" + suffix;
        var checkName = "CK_CliLiveFeatureOwned_Name_" + suffix;
        var defaultName = "DF_CliLiveFeatureOwned_Name_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_feature_metadata_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupFeatureOwnedMetadata(connection, provider, kind, tableName, checkName, defaultName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFeatureMetadataCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFeatureMetadataCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode, StringComparison.Ordinal);
            Assert.Contains("NameLength { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.Name).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            if (kind == ProviderKind.Postgres)
                Assert.Contains($".Entity<{tableName}>().Property(e => e.CreatedAt).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"{checkName}", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.NameLength).HasComputedColumnSql(", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.Name).HasCollation(", contextCode, StringComparison.Ordinal);

            AssertFeatureMetadataHasNoProviderOwnedDiagnostics(warningJsonPath, tableName);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupFeatureOwnedMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_suppresses_synthetic_check_constraint_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveUnnamedCheck" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_unnamed_check_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupUnnamedCheckConstraintMetadata(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveUnnamedCheckCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveUnnamedCheckCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(", contextCode, StringComparison.Ordinal);
            if (kind == ProviderKind.SqlServer)
            {
                Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"CK_{tableName}_", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("CK__", contextCode, StringComparison.Ordinal);
            }
            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"CK_{tableName}_1", contextCode, StringComparison.Ordinal);
            }

            AssertFeatureMetadataHasNoProviderOwnedDiagnostics(warningJsonPath, tableName);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupFeatureOwnedMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_suppresses_synthetic_unique_constraint_index_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveUnnamedUnique" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_unnamed_unique_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupUnnamedUniqueConstraintIndex(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveUnnamedUniqueCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("IsUnique = true", entityCode, StringComparison.Ordinal);
            if (kind is ProviderKind.Sqlite or ProviderKind.SqlServer)
            {
                Assert.Contains($"[Index(\"UX_{tableName}_Code\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
            }
            if (kind == ProviderKind.Sqlite)
            {
                Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
            }
            if (kind == ProviderKind.SqlServer)
            {
                Assert.DoesNotContain("UQ__", entityCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupFeatureOwnedMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_preserves_database_comments_as_xml_docs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCommented" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_comments_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupDatabaseComments(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveCommentCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains("/// Maps to column Name", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Table &lt;summary&gt;", entityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains("/// Table &lt;summary&gt; &amp; description", entityCode, StringComparison.Ordinal);
                Assert.Contains("/// Name &lt;tag&gt; &amp; details", entityCode, StringComparison.Ordinal);
                Assert.Contains("/// <remarks>Maps to column Name</remarks>", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Name <tag> & details", entityCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupDatabaseComments(cleanup, provider, kind, tableName);
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
    public void Dotnet_norm_scaffold_preserves_scalar_facets_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveFacet" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_facets_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupScalarFacets(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFacetCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFacetCtx.cs"));

            Assert.Contains($"public partial class {tableName}", entityCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            if (kind != ProviderKind.Sqlite)
            {
                Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(28,6)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Amount).HasPrecision(28, 6);", contextCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(40)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(12)]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Code).HasMaxLength(40)", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12)", contextCode, StringComparison.Ordinal);
            }

            if (kind == ProviderKind.SqlServer)
            {
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Code).HasMaxLength(40).IsUnicode(false);", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12).IsUnicode(false).IsFixedLength();", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
            }
            else if (kind == ProviderKind.MySql)
            {
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
            }
            else if (kind == ProviderKind.Postgres)
            {
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($".Property(e => e.Token).HasMaxLength", contextCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupScalarFacets(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_preserves_index_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveIndexedOrder" + suffix;
        var nameIndex = "IX_CliLiveIndexedOrder_Name_" + suffix;
        var uniqueIndex = "UX_CliLiveIndexedOrder_Tenant_OrderNo_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_indexes_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupIndexMetadata(connection, provider, kind, tableName, nameIndex, uniqueIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));

            Assert.Contains($"[Index(\"{nameIndex}\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{uniqueIndex}\", IsUnique = true, Order = 0)]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{uniqueIndex}\", IsUnique = true, Order = 1)]", entityCode, StringComparison.Ordinal);
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
                CleanupIndexMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_preserves_provider_index_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProviderIndex" + suffix;
        var partialIndex = "IX_CliLiveProviderIndex_Partial_" + suffix;
        var includedIndex = "IX_CliLiveProviderIndex_Included_" + suffix;
        var descendingIndex = "IX_CliLiveProviderIndex_Descending_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_indexes_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderIndexMetadata(connection, provider, kind, tableName, partialIndex, includedIndex, descendingIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProviderIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains($"[Index(\"{descendingIndex}\", IsDescending = true)]", entityCode, StringComparison.Ordinal);

            if (kind != ProviderKind.MySql)
                Assert.Contains($"[Index(\"{partialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
            else
                Assert.DoesNotContain(partialIndex, entityCode, StringComparison.Ordinal);

            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.Contains($"[Index(\"{includedIndex}\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{includedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain(includedIndex, entityCode, StringComparison.Ordinal);
            }

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
                CleanupProviderIndexMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_reports_trigger_diagnostics_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveTriggerAudit" + suffix;
        var triggerName = "TR_CliLiveTriggerAudit_Touch_" + suffix;
        var functionName = "fn_CliLiveTriggerAuditTouch_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_trigger_diag_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_trigger_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTriggerDiagnosticsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains("Touched { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath), "Trigger diagnostics must write the scaffold warning JSON report.");
            AssertTriggerDiagnostic(warningJsonPath, tableName, triggerName, kind);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                if (scratchDatabase is null)
                    CleanupTriggerDiagnostics(cleanup, provider, kind, tableName, triggerName, functionName);
                else
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
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
    [InlineData(ProviderKind.Sqlite, "Location", "GEOMETRY")]
    [InlineData(ProviderKind.SqlServer, "Location", "geometry")]
    [InlineData(ProviderKind.Postgres, "Address", "inet")]
    [InlineData(ProviderKind.MySql, "Location", "point")]
    public void Dotnet_norm_scaffold_reports_provider_specific_column_diagnostics_on_live_provider(
        ProviderKind kind,
        string columnName,
        string expectedDetail)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliProviderColumn" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderSpecificColumnDiagnostics(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProviderColumnCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains(columnName + " { get; set; }", entityCode, StringComparison.Ordinal);
            AssertProviderSpecificColumnDiagnostic(Path.Combine(output, "nORM.ScaffoldWarnings.json"), columnName, expectedDetail);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProviderSpecificColumnDiagnostics(cleanup, provider, kind, tableName);
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
    public void Dotnet_norm_scaffold_preserves_safe_provider_specific_columns_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliSafeProviderColumn" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_safe_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSafeProviderSpecificColumns(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSafeProviderColumnCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSafeProviderColumnCtx.cs"));

            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            switch (kind)
            {
                case ProviderKind.Sqlite:
                    Assert.Contains("using System;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid TraceId { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string? XmlPayload { get; set; }", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.SqlServer:
                    Assert.Contains("public string XmlPayload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.Postgres:
                    Assert.Contains("using System;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid TraceId { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public int[]? Scores { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string[]? Tags { get; set; }", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.MySql:
                    Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Flags { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("FiscalYear { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("object FiscalYear", entityCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Status_Enum\", \"Status IN ('draft', 'paid', 'cancelled')\")", contextCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Flags_Set\", \"Flags IN ('', 'read', 'write', 'read,write', 'admin', 'read,admin', 'write,admin', 'read,write,admin')\")", contextCode, StringComparison.Ordinal);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupSafeProviderSpecificColumns(cleanup, provider, kind, tableName);
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
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_writable_provider_specific_diagnostics_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliWritableProviderColumn" + suffix;
        var typeName = "CliWritableEmail" + suffix;
        var decimalTypeName = "CliWritableAmount" + suffix;
        var binaryTypeName = "CliWritableToken" + suffix;
        var arrayTypeName = "cli_writable_scores_" + suffix.ToLowerInvariant();
        var enumTypeName = "cli_writable_status_" + suffix.ToLowerInvariant();
        var enumDomainName = "cli_writable_status_domain_" + suffix.ToLowerInvariant();
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_writable_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupWritableProviderSpecificDiagnostics(connection, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);
            }

            var tableFilter = kind switch
            {
                ProviderKind.SqlServer => "dbo." + tableName,
                ProviderKind.Postgres => "public." + tableName,
                _ => tableName
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveWritableProviderColumnCtx " +
                $"--table {Quote(tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveWritableProviderColumnCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath), "Writable provider-specific diagnostics must still be reported for provider-mobility review.");

            switch (kind)
            {
                case ProviderKind.SqlServer:
                    Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(64)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public byte[] Token { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Email", "user-defined type (dbo." + typeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Amount", "user-defined type (dbo." + decimalTypeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Token", "user-defined type (dbo." + binaryTypeName);
                    break;
                case ProviderKind.Postgres:
                    Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"Score\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal Score { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public int[] Scores { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Status_Enum\", \"Status IN ('draft', 'active', 'archived')\")", contextCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Email", "DOMAIN (public." + typeName.ToLowerInvariant());
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Score", "DOMAIN (public." + decimalTypeName.ToLowerInvariant());
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Scores", "DOMAIN (public." + arrayTypeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Status", "DOMAIN (public." + enumDomainName);
                    break;
                case ProviderKind.MySql:
                    Assert.Contains("public uint UnsignedCount { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public ulong UnsignedTotal { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"UnsignedAmount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal UnsignedAmount { get; set; }", entityCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedCount", "unsigned");
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedTotal", "unsigned");
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedAmount", "decimal");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupWritableProviderSpecificDiagnostics(cleanup, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);
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
    public void Dotnet_norm_scaffold_json_fail_on_warnings_reports_live_provider_diagnostics(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var keylessTable = "CliLiveWarning_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_warning_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeylessWarning(connection, provider, kind, keylessTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveWarningCtx " +
                "--fail-on-warnings " +
                "--json " +
                $"--table {Quote(keylessTable)}",
                root);

            Assert.NotEqual(0, scaffold.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using var document = JsonDocument.Parse(scaffold.Stdout);
            var json = document.RootElement;
            var warnings = json.GetProperty("warnings");
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.Contains("Scaffolding produced warnings", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.True(warnings.GetProperty("hasDiagnostics").GetBoolean());
            Assert.True(warnings.GetProperty("reportsWritten").GetBoolean());
            Assert.Equal(1, warnings.GetProperty("codes").GetProperty("SCF116").GetInt32());
            Assert.Equal(1, warnings.GetProperty("categories").GetProperty("table-shape").GetInt32());

            var warningMarkdown = Path.Combine(output, "nORM.ScaffoldWarnings.md");
            var warningJson = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningMarkdown));
            Assert.True(File.Exists(warningJson));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeylessWarning(cleanup, provider, keylessTable);
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
    public void Dotnet_norm_scaffold_provider_option_accepts_ef_provider_package_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveProviderOption" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_option_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--connection {Quote(connectionString)} " +
                $"--provider {EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(output)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveProviderOptionCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, table + ".cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveProviderOptionCtx.cs"));
            Assert.Contains($"public partial class {table}", entityCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", contextCode, StringComparison.Ordinal);
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
                CleanupEfStyleAliasTable(cleanup, provider, table);
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
    public void Dotnet_norm_scaffold_honors_connection_provider_option_precedence_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveOptionPrecedence" + suffix;
        var explicitOutput = Path.Combine(Path.GetTempPath(), "norm_live_cli_option_precedence_explicit_" + kind + "_" + suffix);
        var providerPositionOutput = Path.Combine(Path.GetTempPath(), "norm_live_cli_option_precedence_provider_position_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var explicitScaffold = RunCli(
                "scaffold " +
                $"{Quote("Not=PositionalConnectionString")} notaprovider " +
                $"--connection {Quote(connectionString)} " +
                $"--provider {cliProvider} " +
                $"--output-dir {Quote(explicitOutput)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveExplicitOptionPrecedenceCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(explicitScaffold.ExitCode == 0,
                $"CLI failed with exit code {explicitScaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{explicitScaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{explicitScaffold.Stderr}");

            var providerPositionScaffold = RunCli(
                "scaffold " +
                $"--connection {Quote(connectionString)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(providerPositionOutput)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveProviderPositionCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(providerPositionScaffold.ExitCode == 0,
                $"CLI failed with exit code {providerPositionScaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{providerPositionScaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{providerPositionScaffold.Stderr}");

            var explicitEntityPath = Path.Combine(explicitOutput, table + ".cs");
            var providerPositionEntityPath = Path.Combine(providerPositionOutput, table + ".cs");
            Assert.True(File.Exists(explicitEntityPath));
            Assert.True(File.Exists(providerPositionEntityPath));

            var explicitContextCode = File.ReadAllText(Path.Combine(explicitOutput, "CliLiveExplicitOptionPrecedenceCtx.cs"));
            var providerPositionContextCode = File.ReadAllText(Path.Combine(providerPositionOutput, "CliLiveProviderPositionCtx.cs"));
            Assert.Contains($"IQueryable<{table}>", explicitContextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", providerPositionContextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(explicitOutput, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(explicitOutput, "nORM.ScaffoldWarnings.json")));
            Assert.False(File.Exists(Path.Combine(providerPositionOutput, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(providerPositionOutput, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, explicitOutput);
            RunDotNet("build -c Release --nologo", explicitOutput);
            WriteConsumerProject(root, providerPositionOutput);
            RunDotNet("build -c Release --nologo", providerPositionOutput);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupEfStyleAliasTable(cleanup, provider, table);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(explicitOutput);
            TryDeleteDirectory(providerPositionOutput);
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
    public void Dotnet_norm_dbcontext_scaffold_accepts_ef_provider_package_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveEfStyle" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_style_" + kind + "_" + suffix);
        var msbuildProjectExtensionsPath = Path.Combine(Path.GetTempPath(), "norm_live_cli_msbuild_ext_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var scaffold = RunCli(
                "dbcontext scaffold " +
                $"{Quote(connectionString)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(output)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveEfStyleCtx " +
                "--no-build " +
                "--framework net8.0 " +
                "--configuration Release " +
                "--runtime win-x64 " +
                $"--msbuildprojectextensionspath {Quote(msbuildProjectExtensionsPath)} " +
                "--data-annotations " +
                $"--table {Quote(table)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, table + ".cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveEfStyleCtx.cs"));
            Assert.Contains($"public partial class {table}", entityCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", contextCode, StringComparison.Ordinal);
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
                CleanupEfStyleAliasTable(cleanup, provider, table);
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
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_routine_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliRoutine" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRoutineStub(connection, provider, kind, routineName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Routine scaffolding should keep provider-owned routine metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routine = Assert.Single(
                warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                item => item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));

            Assert.Contains($"Task<List<TResult>> {routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
            Assert.Contains(
                kind == ProviderKind.Postgres
                    ? "public int? tenantid { get; init; }"
                    : "public int? tenantId { get; init; }",
                contextCode,
                StringComparison.Ordinal);
            Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", contextCode, StringComparison.Ordinal);
            Assert.Contains("/// Routine &lt;summary&gt; &amp; description", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Routine <summary> & description", contextCode, StringComparison.Ordinal);
            Assert.Equal("Routine", routine.GetProperty("kind").GetString());

            if (kind == ProviderKind.SqlServer)
            {
                Assert.Equal(1, routine.GetProperty("metadata").GetProperty("outputParameterCount").GetInt32());
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{routineName}Result>> {routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{routineName}OutputParameters()", contextCode, StringComparison.Ordinal);
            }

            if (kind == ProviderKind.Postgres)
            {
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int Id { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string Name { get; set; } = default!;", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupRoutineStub(cleanup, provider, kind, routineName);
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
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_advanced_routine_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliAdvancedRoutine" + suffix;
        var tableFunctionName = "CliAdvancedTableFunction" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_advanced_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupAdvancedRoutineStub(connection, provider, kind, routineName, tableFunctionName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveAdvancedRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveAdvancedRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Advanced routine stubs must preserve provider-owned routine metadata.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

            switch (kind)
            {
                case ProviderKind.SqlServer:
                {
                    Assert.Contains(routines, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal) &&
                        item.GetProperty("metadata").GetProperty("callShape").GetString() == "scalar-function");
                    var tableValuedFunction = Assert.Single(routines, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(tableFunctionName, StringComparison.Ordinal) &&
                        item.GetProperty("metadata").GetProperty("callShape").GetString() == "table-valued-function");
                    var resultColumns = tableValuedFunction.GetProperty("metadata").GetProperty("resultColumns").EnumerateArray().ToArray();
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Id" &&
                        item.GetProperty("dataType").GetString() == "int");
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Name" &&
                        item.GetProperty("dataType").GetString()!.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase));

                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<TValue?> {routineName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"public sealed class {tableFunctionName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"public sealed class {tableFunctionName}Result", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public int? Id { get; set; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public string? Name { get; set; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<TResult>> {tableFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<{tableFunctionName}Result>> {tableFunctionName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<TResult> Stream{tableFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<{tableFunctionName}Result> Stream{tableFunctionName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{routineName}\")", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{tableFunctionName}\")", contextCode, StringComparison.Ordinal);
                    break;
                }
                case ProviderKind.Postgres:
                {
                    var routine = Assert.Single(
                        routines,
                        item => item.GetProperty("kind").GetString() == "Routine" &&
                                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                    var parameters = routine.GetProperty("metadata").GetProperty("parameters").EnumerateArray().ToArray();
                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public int[]? ids { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid? trace_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "ids" &&
                        item.GetProperty("clrType").GetString() == "int[]?" &&
                        item.GetProperty("dbType").GetString() == "Object");
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "trace_id" &&
                        item.GetProperty("clrType").GetString() == "Guid?");
                    break;
                }
                case ProviderKind.MySql:
                {
                    var routine = Assert.Single(
                        routines,
                        item => item.GetProperty("kind").GetString() == "Routine" &&
                                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                    var metadata = routine.GetProperty("metadata");

                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public uint? customer_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public ulong? max_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public ushort? rank { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public byte? flag { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Equal("scalar-function", metadata.GetProperty("callShape").GetString());
                    Assert.Equal("int", metadata.GetProperty("dataType").GetString());
                    Assert.Contains($"Task<List<TResult>> {routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"private sealed class {routineName}ValueResult<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<TValue?> {routineName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine stubs target providers with routine catalogs.");
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupAdvancedRoutineStub(cleanup, provider, kind, routineName, tableFunctionName);
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
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_routine_output_and_non_query_wrappers_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliOutputRoutine" + suffix;
        var nonQueryRoutineName = "CliNonQueryRoutine" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_output_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRoutineOutputAndNonQueryWrappers(connection, provider, kind, routineName, nonQueryRoutineName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveOutputRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveOutputRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Routine output scaffolding should keep provider-owned routine metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();
            Assert.Contains(routines, item =>
                item.GetProperty("kind").GetString() == "Routine" &&
                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));

            Assert.Contains($"Task<StoredProcedureResult<TResult>> {routineName}WithOutputAsync<TResult>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public static OutputParameter[] Create{routineName}OutputParameters()", contextCode, StringComparison.Ordinal);
            Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal, (byte)18, (byte)2)", contextCode, StringComparison.Ordinal);

            if (kind == ProviderKind.MySql)
            {
                Assert.Contains("public string? message { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", contextCode, StringComparison.Ordinal);
            }
            else
            {
                var nonQueryRoutine = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(nonQueryRoutineName, StringComparison.Ordinal));
                var nonQueryMetadata = nonQueryRoutine.GetProperty("metadata");
                Assert.Equal("stored procedure", nonQueryMetadata.GetProperty("routineType").GetString());
                Assert.Equal(2, nonQueryMetadata.GetProperty("outputParameterCount").GetInt32());

                Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32)", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<int> {nonQueryRoutineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<StoredProcedureNonQueryResult> {nonQueryRoutineName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{nonQueryRoutineName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{nonQueryRoutineName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{nonQueryRoutineName}OutputParameters()", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Task<List<TResult>> {nonQueryRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"StoredProcedureResult<TResult> {nonQueryRoutineName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Stream{nonQueryRoutineName}Async", contextCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupRoutineOutputAndNonQueryWrappers(cleanup, provider, kind, routineName, nonQueryRoutineName);
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
    public void Dotnet_norm_scaffold_emits_sequence_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var sequenceName = "CliSequence" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sequence_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSequenceStub(connection, provider, kind, sequenceName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSequenceCtx " +
                "--emit-sequence-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSequenceCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Sequence scaffolding should keep provider-owned sequence metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var sequence = Assert.Single(
                warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                item => item.GetProperty("kind").GetString() == "Sequence" &&
                        item.GetProperty("name").GetString()!.EndsWith(sequenceName, StringComparison.Ordinal));

            Assert.Contains("public async Task<", contextCode, StringComparison.Ordinal);
            Assert.Contains($"Next{sequenceName}ValueAsync", contextCode, StringComparison.Ordinal);
            Assert.Contains("QueryUnchangedAsync<", contextCode, StringComparison.Ordinal);
            Assert.Contains(
                kind == ProviderKind.SqlServer ? "NEXT VALUE FOR" : "nextval('",
                contextCode,
                StringComparison.Ordinal);
            Assert.Contains("/// Sequence &lt;summary&gt; &amp; description", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Sequence <summary> & description", contextCode, StringComparison.Ordinal);
            Assert.Contains("dataType", sequence.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupSequenceStub(cleanup, provider, kind, sequenceName);
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
    public void Dotnet_norm_scaffold_table_filter_suppresses_unselected_principal_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var principalTable = "CliLiveFilterParent" + suffix;
        var dependentTable = "CliLiveFilterChild" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_table_filter_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupFilteredRelationship(connection, provider, kind, principalTable, dependentTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFilteredCtx " +
                $"--table {Quote(dependentTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, dependentTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, principalTable + ".cs")));
            var dependentCode = File.ReadAllText(Path.Combine(output, dependentTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFilteredCtx.cs"));

            Assert.Matches(@"public (int|long) ParentId \{ get; set; \}", dependentCode);
            Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
            Assert.DoesNotContain(principalTable, dependentCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{dependentTable}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(principalTable, contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
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
                CleanupFilteredRelationship(cleanup, provider, principalTable, dependentTable);
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
    public void Dotnet_norm_scaffold_table_filter_emits_view_query_artifact_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseTable = "CliLiveViewBase" + suffix;
        var viewName = "CliLiveViewReport" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_view_filter_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupViewQueryArtifact(connection, provider, kind, baseTable, viewName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveViewCtx " +
                $"--table {Quote(viewName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, viewName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveViewCtx.cs"));
            var warningMarkdown = Path.Combine(output, "nORM.ScaffoldWarnings.md");

            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{viewName}", viewCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {viewName}", viewCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{viewName}>", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, baseTable + ".cs")));
            AssertViewQueryArtifactCommentDocumentation(kind, viewCode);
            Assert.True(File.Exists(warningMarkdown));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);
            Assert.DoesNotContain("Skipped Database Objects", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupViewQueryArtifact(cleanup, provider, kind, baseTable, viewName);
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
    public void Dotnet_norm_scaffold_emits_provider_query_artifacts_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseName = "CliQueryArtifactBase" + suffix;
        var artifactName = "CliQueryArtifact" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_query_artifact_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderQueryArtifact(connection, provider, kind, baseName, artifactName);
            }

            var tableFilter = kind switch
            {
                ProviderKind.SqlServer => "dbo." + artifactName,
                ProviderKind.Postgres => "public." + artifactName,
                _ => artifactName
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveQueryArtifactCtx " +
                "--emit-query-artifacts " +
                $"--table {Quote(tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var artifactCode = File.ReadAllText(Path.Combine(output, artifactName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveQueryArtifactCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", artifactCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{artifactName}", artifactCode, StringComparison.Ordinal);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.Contains("Schema = ", artifactCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{artifactName}>", contextCode, StringComparison.Ordinal);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                AssertViewQueryArtifactCommentDocumentation(kind, artifactCode);
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), artifactName));

            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                    item.GetProperty("name").GetString()!.StartsWith(artifactName + "_", StringComparison.Ordinal));
            }
            else if (kind == ProviderKind.Postgres)
            {
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            }
            else
            {
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProviderQueryArtifact(cleanup, provider, kind, baseName, artifactName);
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
    public void Dotnet_norm_scaffold_default_discovery_emits_table_and_view_query_artifacts_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = "CliDefaultViewSchema" + suffix;
        var baseTable = "CliDefaultViewBase" + suffix;
        var viewName = "CliDefaultViewReport" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_default_view_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_default_view_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupDefaultDiscoveryViewQueryArtifact(connection, provider, kind, schemaName, baseTable, viewName);
            }

            var schemaArgument = kind is ProviderKind.SqlServer or ProviderKind.Postgres
                ? $" --schema {Quote(schemaName)}"
                : string.Empty;
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDefaultViewCtx" +
                schemaArgument,
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var baseCode = File.ReadAllText(Path.Combine(output, baseTable + ".cs"));
            var viewCode = File.ReadAllText(Path.Combine(output, viewName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDefaultViewCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"", baseCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"", viewCode, StringComparison.Ordinal);
            Assert.Contains(baseTable, baseCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(viewName, viewCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.Contains($"Schema = \"{schemaName}\"", baseCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{schemaName}\"", viewCode, StringComparison.Ordinal);
            }

            Assert.Contains($"IQueryable<{baseTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{viewName}>", contextCode, StringComparison.OrdinalIgnoreCase);
            AssertViewQueryArtifactCommentDocumentation(kind, viewCode);
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), viewName));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                if (scratchDatabase is null)
                    CleanupDefaultDiscoveryViewQueryArtifact(cleanup, provider, kind, schemaName, baseTable, viewName);
                else
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
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
    public void Dotnet_norm_scaffold_generates_non_nullable_alternate_key_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveRequiredAltParent" + suffix;
        var childTable = "CliLiveRequiredAltChild" + suffix;
        var indexName = "UX_CliLiveRequiredAltParent_Code_" + suffix;
        var fkName = "FK_CliLiveRequiredAltChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_required_alt_key_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRequiredAlternateKeyRelationship(connection, provider, kind, parentTable, childTable, indexName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRequiredAlternateKeyCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRequiredAlternateKeyCtx.cs"));

            Assert.Contains($"[Index(\"{indexName}\", IsUnique = true)]", parentCode, StringComparison.Ordinal);
            Assert.Contains("public string Code { get; set; } = default!;", parentCode, StringComparison.Ordinal);
            Assert.Contains($"public List<{childTable}> {childTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(ParentCode))]", childCode, StringComparison.Ordinal);
            Assert.Contains("public string ParentCode { get; set; } = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentCode", "p => p.Code", fkName), contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{parentTable}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{childTable}>", contextCode, StringComparison.Ordinal);
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
                CleanupRequiredAlternateKeyRelationship(cleanup, provider, childTable, parentTable);
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
    public void Dotnet_norm_scaffold_generates_nullable_alternate_key_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveAltParent" + suffix;
        var childTable = "CliLiveAltChild" + suffix;
        var indexName = "UX_CliLiveAltParent_Code_" + suffix;
        var fkName = "FK_CliLiveAltChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_alt_key_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupNullableAlternateKeyRelationship(connection, provider, kind, parentTable, childTable, indexName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveAlternateKeyCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var parentCode = File.ReadAllText(Path.Combine(output, parentTable + ".cs"));
            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveAlternateKeyCtx.cs"));

            Assert.Contains($"[Index(\"{indexName}\", IsUnique = true)]", parentCode, StringComparison.Ordinal);
            Assert.Contains("public string? Code { get; set; }", parentCode, StringComparison.Ordinal);
            Assert.Contains($"List<{childTable}>", parentCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(ParentCode))]", childCode, StringComparison.Ordinal);
            Assert.Contains("public string ParentCode { get; set; } = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.ParentCode", "p => p.Code", fkName), contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{parentTable}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{childTable}>", contextCode, StringComparison.Ordinal);
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
                CleanupNullableAlternateKeyRelationship(cleanup, provider, childTable, parentTable);
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
    public void Dotnet_norm_scaffold_preserves_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveRefParent" + suffix;
        var childTable = "CliLiveRefChild" + suffix;
        var fkName = "FK_CliLiveRefChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_referential_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupReferentialActionRelationship(connection, provider, kind, parentTable, childTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveReferentialCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveReferentialCtx.cs"));

            Assert.Contains($"public {parentTable}? {parentTable} {{ get; set; }}", childCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.SetNull", "ReferentialAction.Cascade", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupReferentialActionRelationship(cleanup, provider, childTable, parentTable);
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
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_restrict_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveRestrictRefParent" + suffix;
        var childTable = "CliLiveRestrictRefChild" + suffix;
        var fkName = "FK_CliLiveRestrictRefChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_restrict_referential_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRestrictReferentialActionRelationship(connection, provider, kind, parentTable, childTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRestrictReferentialCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRestrictReferentialCtx.cs"));

            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.Restrict", "ReferentialAction.Cascade", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupReferentialActionRelationship(cleanup, provider, childTable, parentTable);
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
    public void Dotnet_norm_scaffold_preserves_set_default_fk_referential_actions_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveDefaultRefParent" + suffix;
        var childTable = "CliLiveDefaultRefChild" + suffix;
        var fkName = "FK_CliLiveDefaultRefChild_Parent_" + suffix;
        var defaultName = "DF_CliLiveDefaultRefChild_Parent_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_default_referential_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSetDefaultReferentialActionRelationship(connection, provider, kind, parentTable, childTable, fkName, defaultName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDefaultReferentialCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDefaultReferentialCtx.cs"));

            Assert.Contains($"public {parentTable} {parentTable} {{ get; set; }} = default!;", childCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedReferentialForeignKey(kind, "d => d.ParentId", "p => p.Id", "ReferentialAction.SetDefault", "ReferentialAction.SetDefault", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupReferentialActionRelationship(cleanup, provider, childTable, parentTable);
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

    private static (DbConnection Connection, DatabaseProvider Provider, string ConnectionString, string CliProvider)? OpenLive(ProviderKind kind, ref string? sqliteFile)
    {
        if (kind == ProviderKind.Sqlite)
        {
            sqliteFile = Path.Combine(Path.GetTempPath(), "norm_live_cli_scaffold_" + Guid.NewGuid().ToString("N") + ".db");
            var sqliteConnectionString = "Data Source=" + sqliteFile;
            var connection = new SqliteConnection(sqliteConnectionString);
            connection.Open();
            return (connection, new SqliteProvider(), sqliteConnectionString, "sqlite");
        }

        var connectionString = kind switch
        {
            ProviderKind.SqlServer => LiveProviderEnvironment.GetConnectionString("sqlserver"),
            ProviderKind.Postgres => LiveProviderEnvironment.GetConnectionString("postgres"),
            ProviderKind.MySql => LiveProviderEnvironment.GetConnectionString("mysql"),
            _ => null
        };
        if (string.IsNullOrEmpty(connectionString))
            return null;

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is null)
            return null;

        return kind switch
        {
            ProviderKind.SqlServer => (live.Value.Connection, live.Value.Provider, connectionString, "sqlserver"),
            ProviderKind.Postgres => (live.Value.Connection, live.Value.Provider, connectionString, "postgres"),
            ProviderKind.MySql => (live.Value.Connection, live.Value.Provider, connectionString, "mysql"),
            _ => null
        };
    }

    private static DbConnection Reopen(ProviderKind kind, string connectionString)
    {
        if (kind == ProviderKind.Sqlite)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            return connection;
        }

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is not null)
            return live.Value.Connection;

        throw new InvalidOperationException($"Live provider {kind} is no longer available for cleanup.");
    }

    private static void SetupCompositeSharedTenantManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string userTable,
        string tagTable,
        string userTagTable)
    {
        CleanupCompositeSharedTenantManyToMany(connection, provider, userTable, tagTable, userTagTable);

        var tenantId = provider.Escape("TenantId");
        var userId = provider.Escape("UserId");
        var tagId = provider.Escape("TagId");
        var name = provider.Escape("Name");
        var user = provider.Escape(userTable);
        var tag = provider.Escape(tagTable);
        var userTag = provider.Escape(userTagTable);
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {user} ({tenantId} int NOT NULL, {userId} int NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {userId}))",
            $"CREATE TABLE {tag} ({tenantId} int NOT NULL, {tagId} int NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {tagId}))",
            $"CREATE TABLE {userTag} ({tenantId} int NOT NULL, {userId} int NOT NULL, {tagId} int NOT NULL, PRIMARY KEY ({tenantId}, {userId}, {tagId}), " +
            $"FOREIGN KEY ({tenantId}, {userId}) REFERENCES {user} ({tenantId}, {userId}), " +
            $"FOREIGN KEY ({tenantId}, {tagId}) REFERENCES {tag} ({tenantId}, {tagId}))");
    }

    private static void CleanupCompositeSharedTenantManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string userTable,
        string tagTable,
        string userTagTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(userTagTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(tagTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(userTable)}");
    }

    private static void SetupMixedSingleForeignKeyAndManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string labelTable,
        string bookLabelTable,
        string bookAuthorFkName,
        string bookLabelBookFkName,
        string bookLabelLabelFkName,
        string bookAuthorTitleIndex)
    {
        CleanupMixedSingleForeignKeyAndManyToMany(connection, provider, bookTable, authorTable, labelTable, bookLabelTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var label = provider.Escape(labelTable);
        var bookLabel = provider.Escape(bookLabelTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("Author_Id");
        var bookId = provider.Escape("BookId");
        var labelId = provider.Escape("LabelId");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text40} NOT NULL)",
            $"CREATE TABLE {book} ({id} {idType} NOT NULL PRIMARY KEY, {authorId} {idType} NOT NULL, {title} {text80} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(bookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}))",
            $"CREATE TABLE {label} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text40} NOT NULL)",
            $"CREATE TABLE {bookLabel} ({bookId} {idType} NOT NULL, {labelId} {idType} NOT NULL, PRIMARY KEY ({bookId}, {labelId}), " +
            $"CONSTRAINT {provider.Escape(bookLabelBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookLabelLabelFkName)} FOREIGN KEY ({labelId}) REFERENCES {label} ({id}))",
            $"CREATE INDEX {provider.Escape(bookAuthorTitleIndex)} ON {book} ({authorId}, {title})");
    }

    private static void CleanupMixedSingleForeignKeyAndManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string bookTable,
        string authorTable,
        string labelTable,
        string bookLabelTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(bookLabelTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(labelTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupUnnamedForeignKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {name} {text} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static void SetupCompositePrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string parentPkName,
        string fkName)
    {
        CleanupCompositePrimaryKeyForeignKey(connection, provider, parentTable, childTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var parentPk = provider.Escape(parentPkName);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({tenantId} {idType} NOT NULL, {orderNo} {idType} NOT NULL, {name} {text} NOT NULL, CONSTRAINT {parentPk} PRIMARY KEY ({tenantId}, {orderNo}))",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {orderNo} {idType} NOT NULL, {notes} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {orderNo}) REFERENCES {parent} ({tenantId}, {orderNo}))");
    }

    private static void CleanupCompositePrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupCompositeManyToMany(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({tenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))",
            $"CREATE TABLE {course} ({tenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {title} {text} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))",
            $"CREATE TABLE {studentCourse} ({studentTenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {courseTenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static void CleanupCompositeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupManyToManyReferentialActions(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupManyToManyReferentialActions(connection, provider, authorTable, bookTable, authorBookTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var authorBook = provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({authorId} {idType} NOT NULL, {bookId} {idType} NOT NULL, " +
            $"PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}) ON DELETE CASCADE ON UPDATE CASCADE, " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}) ON DELETE NO ACTION ON UPDATE NO ACTION)");
    }

    private static void CleanupManyToManyReferentialActions(
        DbConnection connection,
        DatabaseProvider provider,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(authorBookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupSharedTenantAlternateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName,
        string bookIndexName)
    {
        CleanupSharedTenantAlternateKeyManyToMany(connection, provider, authorTable, bookTable, authorBookTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var authorBook = provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {code} {text40} NOT NULL, {name} {text80} NOT NULL, UNIQUE ({tenantId}, {code}))",
            $"CREATE TABLE {book} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {isbn} {text40} NOT NULL, {title} {text80} NOT NULL, UNIQUE ({tenantId}, {isbn}))",
            $"CREATE TABLE {authorBook} ({tenantId} {idType} NOT NULL, {authorCode} {text40} NOT NULL, {bookIsbn} {text40} NOT NULL, " +
            $"PRIMARY KEY ({tenantId}, {authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({tenantId}, {authorCode}) REFERENCES {author} ({tenantId}, {code}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({tenantId}, {bookIsbn}) REFERENCES {book} ({tenantId}, {isbn}))");

        if (kind == ProviderKind.MySql)
            Execute(connection, $"CREATE INDEX {provider.Escape(bookIndexName)} ON {authorBook} ({tenantId}, {bookIsbn})");
    }

    private static void CleanupSharedTenantAlternateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(authorBookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupUseDatabaseNames(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string customerTable,
        string orderLineTable)
    {
        CleanupUseDatabaseNames(connection, provider, customerTable, orderLineTable);

        var customer = provider.Escape(customerTable);
        var orderLine = provider.Escape(orderLineTable);
        var customerId = provider.Escape("customer_id");
        var orderLineId = provider.Escape("order_line_id");
        var displayName = provider.Escape("display_name");
        var sku = provider.Escape("SKU");
        var classColumn = provider.Escape("class");
        var hasSpace = provider.Escape("has space");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {customer} ({customerId} int NOT NULL PRIMARY KEY, {displayName} {text} NOT NULL)",
            $"CREATE TABLE {orderLine} ({orderLineId} int NOT NULL PRIMARY KEY, {customerId} int NOT NULL, {sku} {text} NOT NULL, {classColumn} {text} NULL, {hasSpace} {text} NULL, " +
            $"FOREIGN KEY ({customerId}) REFERENCES {customer} ({customerId}))");
    }

    private static void CleanupUseDatabaseNames(
        DbConnection connection,
        DatabaseProvider provider,
        string customerTable,
        string orderLineTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(orderLineTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(customerTable)}");
    }

    private static void SetupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        CleanupIdentifierCollisionScaffold(connection, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);

        var invalid = provider.Escape(invalidIdentifierTable);
        var dash = provider.Escape(collisionDashTable);
        var underscore = provider.Escape(collisionUnderscoreTable);
        var id = provider.Escape("Id");
        var invalidLeadingDigit = provider.Escape("1st-name");
        var hasSpace = provider.Escape("has space");
        var firstNameDash = provider.Escape("first-name");
        var firstNameUnderscore = provider.Escape("first_name");
        var toString = provider.Escape("ToString");
        var equals = provider.Escape("Equals");
        var value = provider.Escape("Value");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {invalid} ({id} {idType} NOT NULL PRIMARY KEY, {invalidLeadingDigit} {text} NOT NULL, {hasSpace} {idType} NULL, {firstNameDash} {text} NOT NULL, {firstNameUnderscore} {text} NOT NULL, {toString} {text} NOT NULL, {equals} {text} NOT NULL)",
            $"CREATE TABLE {dash} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)",
            $"CREATE TABLE {underscore} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)");
    }

    private static void CleanupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        Execute(connection,
            DropTable(kind, collisionUnderscoreTable, provider.Escape(collisionUnderscoreTable)),
            DropTable(kind, collisionDashTable, provider.Escape(collisionDashTable)),
            DropTable(kind, invalidIdentifierTable, provider.Escape(invalidIdentifierTable)));
    }

    private static void SetupProjectAwareScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupProjectAwareScaffold(connection, provider, kind, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {notes} {text} NULL)");
    }

    private static void CleanupProjectAwareScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
        => Execute(connection, DropTable(kind, tableName, provider.Escape(tableName)));

    private static void SetupTableFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        params string[] tableNames)
    {
        CleanupTableFilterValuesScaffold(connection, provider, kind, tableNames);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        foreach (var tableName in tableNames)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(tableName)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
        }
    }

    private static void CleanupTableFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        params string[] tableNames)
    {
        Execute(
            connection,
            tableNames
                .Reverse()
                .Select(tableName => DropTable(kind, tableName, provider.Escape(tableName)))
                .ToArray());
    }

    private static void SetupSchemaFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string csvSchemaOne,
        string csvSchemaTwo,
        string multiSchemaOne,
        string multiSchemaTwo,
        string skippedSchema,
        string csvTableOne,
        string csvTableTwo,
        string multiTableOne,
        string multiTableTwo,
        string skippedTable)
    {
        CleanupSchemaFilterValuesScaffold(
            connection,
            provider,
            kind,
            csvSchemaOne,
            csvSchemaTwo,
            multiSchemaOne,
            multiSchemaTwo,
            skippedSchema,
            csvTableOne,
            csvTableTwo,
            multiTableOne,
            multiTableTwo,
            skippedTable);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(csvTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(csvTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(multiTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(multiTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
            return;
        }

        CreateSchema(provider, connection, kind, csvSchemaOne);
        CreateSchema(provider, connection, kind, csvSchemaTwo);
        CreateSchema(provider, connection, kind, multiSchemaOne);
        CreateSchema(provider, connection, kind, multiSchemaTwo);
        CreateSchema(provider, connection, kind, skippedSchema);

        Execute(
            connection,
            $"CREATE TABLE {Qualified(provider, csvSchemaOne, csvTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, csvSchemaTwo, csvTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, multiSchemaOne, multiTableOne)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, multiSchemaTwo, multiTableTwo)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {Qualified(provider, skippedSchema, skippedTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupSchemaFilterValuesScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string csvSchemaOne,
        string csvSchemaTwo,
        string multiSchemaOne,
        string multiSchemaTwo,
        string skippedSchema,
        string csvTableOne,
        string csvTableTwo,
        string multiTableOne,
        string multiTableTwo,
        string skippedTable)
    {
        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                DropTable(kind, multiTableTwo, provider.Escape(multiTableTwo)),
                DropTable(kind, multiTableOne, provider.Escape(multiTableOne)),
                DropTable(kind, csvTableTwo, provider.Escape(csvTableTwo)),
                DropTable(kind, csvTableOne, provider.Escape(csvTableOne)));
            return;
        }

        Execute(
            connection,
            DropTable(kind, skippedSchema + "." + skippedTable, Qualified(provider, skippedSchema, skippedTable)),
            DropTable(kind, multiSchemaTwo + "." + multiTableTwo, Qualified(provider, multiSchemaTwo, multiTableTwo)),
            DropTable(kind, multiSchemaOne + "." + multiTableOne, Qualified(provider, multiSchemaOne, multiTableOne)),
            DropTable(kind, csvSchemaTwo + "." + csvTableTwo, Qualified(provider, csvSchemaTwo, csvTableTwo)),
            DropTable(kind, csvSchemaOne + "." + csvTableOne, Qualified(provider, csvSchemaOne, csvTableOne)));

        DropSchema(provider, connection, kind, skippedSchema);
        DropSchema(provider, connection, kind, multiSchemaTwo);
        DropSchema(provider, connection, kind, multiSchemaOne);
        DropSchema(provider, connection, kind, csvSchemaTwo);
        DropSchema(provider, connection, kind, csvSchemaOne);
    }

    private static void SetupSchemaAndTableFilterUnionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string schemaTable,
        string explicitTable,
        string skippedTable)
    {
        CleanupSchemaAndTableFilterUnionScaffold(connection, provider, kind, schemaName, schemaTable, explicitTable, skippedTable);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                $"CREATE TABLE {provider.Escape(schemaTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                $"CREATE TABLE {provider.Escape(explicitTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
            return;
        }

        CreateSchema(provider, connection, kind, schemaName);
        Execute(
            connection,
            $"CREATE TABLE {Qualified(provider, schemaName, schemaTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {provider.Escape(explicitTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {provider.Escape(skippedTable)} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupSchemaAndTableFilterUnionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string schemaTable,
        string explicitTable,
        string skippedTable)
    {
        if (kind is ProviderKind.Sqlite or ProviderKind.MySql)
        {
            Execute(
                connection,
                DropTable(kind, explicitTable, provider.Escape(explicitTable)),
                DropTable(kind, schemaTable, provider.Escape(schemaTable)));
            return;
        }

        Execute(
            connection,
            DropTable(kind, skippedTable, provider.Escape(skippedTable)),
            DropTable(kind, explicitTable, provider.Escape(explicitTable)),
            DropTable(kind, schemaName + "." + schemaTable, Qualified(provider, schemaName, schemaTable)));

        DropSchema(provider, connection, kind, schemaName);
    }

    private static void CreateSchema(DatabaseProvider provider, DbConnection connection, ProviderKind kind, string schemaName)
    {
        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");
    }

    private static void DropSchema(DatabaseProvider provider, DbConnection connection, ProviderKind kind, string schemaName)
    {
        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

    private static void SetupAlternateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupAlternateKeyManyToMany(connection, provider, authorTable, bookTable, authorBookTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var authorBook = provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} int NOT NULL PRIMARY KEY, {code} {text} NOT NULL, {name} {text} NOT NULL, UNIQUE ({code}))",
            $"CREATE TABLE {book} ({id} int NOT NULL PRIMARY KEY, {isbn} {text} NOT NULL, {title} {text} NOT NULL, UNIQUE ({isbn}))",
            $"CREATE TABLE {authorBook} ({authorCode} {text} NOT NULL, {bookIsbn} {text} NOT NULL, PRIMARY KEY ({authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorCode}) REFERENCES {author} ({code}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookIsbn}) REFERENCES {book} ({isbn}))");
    }

    private static void CleanupAlternateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(authorBookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupSelfReferencingManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string personTable,
        string relationshipTable,
        string mentorFkName,
        string menteeFkName)
    {
        CleanupSelfReferencingManyToMany(connection, provider, personTable, relationshipTable);

        var person = provider.Escape(personTable);
        var relationship = provider.Escape(relationshipTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var mentorId = provider.Escape("MentorId");
        var menteeId = provider.Escape("MenteeId");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {person} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {relationship} ({mentorId} {idType} NOT NULL, {menteeId} {idType} NOT NULL, PRIMARY KEY ({mentorId}, {menteeId}), " +
            $"CONSTRAINT {provider.Escape(mentorFkName)} FOREIGN KEY ({mentorId}) REFERENCES {person} ({id}), " +
            $"CONSTRAINT {provider.Escape(menteeFkName)} FOREIGN KEY ({menteeId}) REFERENCES {person} ({id}))");
    }

    private static void CleanupSelfReferencingManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string personTable,
        string relationshipTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(relationshipTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(personTable)}");
    }

    private static void SetupSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupSurrogateKeyManyToMany(connection, provider, authorTable, bookTable, authorBookTable);

        var author = provider.Escape(authorTable);
        var book = provider.Escape(bookTable);
        var authorBook = provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({IdentityPrimaryKeyColumn(kind, id)}, {authorId} {idType} NOT NULL, {bookId} {idType} NOT NULL, UNIQUE ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");
    }

    private static void CleanupSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(authorBookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(bookTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(authorTable)}");
    }

    private static void SetupDatabaseGeneratedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupDatabaseGeneratedBridgeManyToMany(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var pairSum = provider.Escape("PairSum");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var generatedColumn = kind switch
        {
            ProviderKind.SqlServer => $"{pairSum} AS ({studentId} + {courseId}) PERSISTED",
            ProviderKind.Postgres => $"{pairSum} integer GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.MySql => $"{pairSum} INT GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.Sqlite => $"{pairSum} INTEGER GENERATED ALWAYS AS ({studentId} + {courseId}) VIRTUAL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection,
            $"CREATE TABLE {student} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {course} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {studentCourse} ({studentId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {generatedColumn}, PRIMARY KEY ({studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
    }

    private static void CleanupDatabaseGeneratedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupCompositeSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupCompositeSurrogateKeyManyToMany(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({tenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {name} {text} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))",
            $"CREATE TABLE {course} ({tenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {title} {text} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))",
            $"CREATE TABLE {studentCourse} ({IdentityPrimaryKeyColumn(kind, id)}, {studentTenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {courseTenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, " +
            $"UNIQUE ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static void CleanupCompositeSurrogateKeyManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupCompositePayloadJoin(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName)
    {
        CleanupCompositePayloadJoin(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var enrollmentCode = provider.Escape("EnrollmentCode");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({tenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {name} {text80} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))",
            $"CREATE TABLE {course} ({tenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {title} {text80} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))",
            $"CREATE TABLE {studentCourse} ({studentTenantId} {idType} NOT NULL, {studentId} {idType} NOT NULL, {courseTenantId} {idType} NOT NULL, {courseId} {idType} NOT NULL, {enrollmentCode} {text40} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static void CleanupCompositePayloadJoin(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupFilteredUniqueSurrogateJoin(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string studentTable,
        string courseTable,
        string studentCourseTable,
        string studentFkName,
        string courseFkName,
        string uniqueIndexName)
    {
        CleanupFilteredUniqueSurrogateJoin(connection, provider, studentTable, courseTable, studentCourseTable);

        var student = provider.Escape(studentTable);
        var course = provider.Escape(courseTable);
        var studentCourse = provider.Escape(studentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {student} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {course} ({id} {idType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {studentCourse} ({IdentityPrimaryKeyColumn(kind, id)}, {studentId} {idType} NOT NULL, {courseId} {idType} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(studentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(courseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(uniqueIndexName)} ON {studentCourse} ({studentId}, {courseId}) WHERE {studentId} > 0");
    }

    private static void CleanupFilteredUniqueSurrogateJoin(
        DbConnection connection,
        DatabaseProvider provider,
        string studentTable,
        string courseTable,
        string studentCourseTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(studentCourseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(courseTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(studentTable)}");
    }

    private static void SetupSchemaQualifiedManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName)
    {
        CleanupSchemaQualifiedManyToMany(connection, provider, kind, schemaName, authorTable, bookTable, authorBookTable);

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");

        var author = Qualified(provider, schemaName, authorTable);
        var book = Qualified(provider, schemaName, bookTable);
        var authorBook = Qualified(provider, schemaName, authorBookTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var text = kind == ProviderKind.SqlServer ? "nvarchar(80)" : "text";

        Execute(connection,
            $"CREATE TABLE {author} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} int NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({authorId} int NOT NULL, {bookId} int NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");
    }

    private static void CleanupSchemaQualifiedManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string authorTable,
        string bookTable,
        string authorBookTable)
    {
        Execute(connection,
            DropTable(kind, schemaName + "." + authorBookTable, Qualified(provider, schemaName, authorBookTable)),
            DropTable(kind, schemaName + "." + bookTable, Qualified(provider, schemaName, bookTable)),
            DropTable(kind, schemaName + "." + authorTable, Qualified(provider, schemaName, authorTable)));

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

    private static void SetupCompositeForeignKeyToUniqueIndex(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupCompositeForeignKeyToUniqueIndex(connection, provider, parentTable, childTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var externalNo = provider.Escape("ExternalNo");
        var name = provider.Escape("Name");
        var eventName = provider.Escape("EventName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {externalNo} {text40} NOT NULL, {name} {text80} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({tenantId}, {externalNo})",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {externalNo} {text40} NOT NULL, {eventName} {text80} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {externalNo}) REFERENCES {parent} ({tenantId}, {externalNo}))");
    }

    private static void CleanupCompositeForeignKeyToUniqueIndex(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeRoleForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string accountTable,
        string transferTable,
        string accountIndex,
        string primaryFkName,
        string backupFkName)
    {
        CleanupCompositeRoleForeignKeys(connection, provider, accountTable, transferTable);

        var account = provider.Escape(accountTable);
        var transfer = provider.Escape(transferTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var primaryAccountNo = provider.Escape("PrimaryAccountNo");
        var backupAccountNo = provider.Escape("BackupAccountNo");
        var name = provider.Escape("Name");
        var amount = provider.Escape("Amount");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {account} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {accountNo} int NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(accountIndex)} ON {account} ({tenantId}, {accountNo})",
            $"CREATE TABLE {transfer} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {primaryAccountNo} int NOT NULL, {backupAccountNo} int NOT NULL, {amount} int NOT NULL, " +
            $"CONSTRAINT {provider.Escape(primaryFkName)} FOREIGN KEY ({tenantId}, {primaryAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}), " +
            $"CONSTRAINT {provider.Escape(backupFkName)} FOREIGN KEY ({tenantId}, {backupAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}))");
    }

    private static void CleanupCompositeRoleForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string accountTable,
        string transferTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(transferTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(accountTable)}");
    }

    private static void SetupRoleNamedOneToOneForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string primaryFkName,
        string backupFkName,
        string primaryIndexName,
        string backupIndexName)
    {
        CleanupRoleNamedOneToOneForeignKeys(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var primaryAccountId = provider.Escape("PrimaryAccountId");
        var backupAccountId = provider.Escape("BackupAccountId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {primaryAccountId} {idType} NOT NULL, {backupAccountId} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(primaryFkName)} FOREIGN KEY ({primaryAccountId}) REFERENCES {parent} ({id}), " +
            $"CONSTRAINT {provider.Escape(backupFkName)} FOREIGN KEY ({backupAccountId}) REFERENCES {parent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(primaryIndexName)} ON {profile} ({primaryAccountId})",
            $"CREATE UNIQUE INDEX {provider.Escape(backupIndexName)} ON {profile} ({backupAccountId})");
    }

    private static void CleanupRoleNamedOneToOneForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupRequiredAndOptionalUniqueDependentForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string requiredParentTable,
        string requiredProfileTable,
        string optionalParentTable,
        string optionalProfileTable,
        string requiredFkName,
        string optionalFkName,
        string requiredIndexName,
        string optionalIndexName)
    {
        CleanupRequiredAndOptionalUniqueDependentForeignKeys(
            connection,
            provider,
            requiredParentTable,
            requiredProfileTable,
            optionalParentTable,
            optionalProfileTable);

        var requiredParent = provider.Escape(requiredParentTable);
        var requiredProfile = provider.Escape(requiredProfileTable);
        var optionalParent = provider.Escape(optionalParentTable);
        var optionalProfile = provider.Escape(optionalProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {requiredParent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {requiredProfile} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(requiredFkName)} FOREIGN KEY ({parentId}) REFERENCES {requiredParent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(requiredIndexName)} ON {requiredProfile} ({parentId})",
            $"CREATE TABLE {optionalParent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {optionalProfile} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(optionalFkName)} FOREIGN KEY ({parentId}) REFERENCES {optionalParent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(optionalIndexName)} ON {optionalProfile} ({parentId})");
    }

    private static void CleanupRequiredAndOptionalUniqueDependentForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string requiredParentTable,
        string requiredProfileTable,
        string optionalParentTable,
        string optionalProfileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(requiredProfileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(requiredParentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(optionalProfileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(optionalParentTable)}");
    }

    private static void SetupSharedPrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string fkName)
    {
        CleanupSharedPrimaryKeyForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({id}) REFERENCES {parent} ({id}))");
    }

    private static void CleanupSharedPrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string parentIndexName,
        string profileIndexName,
        string fkName)
    {
        CleanupCompositeUniqueDependentForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(parentIndexName)} ON {parent} ({tenantId}, {accountNo})",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))",
            $"CREATE UNIQUE INDEX {provider.Escape(profileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static void CleanupCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupOptionalCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string parentIndexName,
        string profileIndexName,
        string fkName)
    {
        CleanupOptionalCompositeUniqueDependentForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(parentIndexName)} ON {parent} ({tenantId}, {accountNo})",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))",
            $"CREATE UNIQUE INDEX {provider.Escape(profileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static void CleanupOptionalCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupKeylessDependentRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string dependentTable,
        string fkName)
    {
        CleanupKeylessDependentRelationship(connection, provider, parentTable, dependentTable);

        var parent = provider.Escape(parentTable);
        var dependent = provider.Escape(dependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var payload = provider.Escape("Payload");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY)",
            $"CREATE TABLE {dependent} ({parentId} {idType} NOT NULL, {payload} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static void CleanupKeylessDependentRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string dependentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(dependentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string checkName,
        string defaultName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var createdAt = provider.Escape("CreatedAt");
        var nameLength = provider.Escape("NameLength");
        var check = provider.Escape(checkName);
        var defaultConstraint = provider.Escape(defaultName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) COLLATE Latin1_General_BIN2 NOT NULL CONSTRAINT {defaultConstraint} DEFAULT ('new'), {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {check} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) COLLATE \"C\" NOT NULL DEFAULT 'new', {createdAt} timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'utc'), {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {check} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'new', {nameLength} int GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {check} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {name} TEXT COLLATE NOCASE NOT NULL DEFAULT 'new', {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {check} CHECK (length({name}) > 0))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, createSql);
    }

    private static void SetupUnnamedCheckConstraintMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} int NOT NULL, CHECK ({amount} > 0))");
    }

    private static void SetupUnnamedUniqueConstraintIndex(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {code} {text} NOT NULL UNIQUE, {name} {text} NOT NULL)");
    }

    private static void CleanupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupDatabaseComments(connection, provider, kind, tableName);

        const string tableComment = "Table <summary> & description";
        const string columnComment = "Name <tag> & details";
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL COMMENT {SqlLiteral(columnComment)}) COMMENT={SqlLiteral(tableComment)}");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(tableComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(columnComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"COMMENT ON TABLE {table} IS {SqlLiteral(tableComment)}",
                    $"COMMENT ON COLUMN {table}.{name} IS {SqlLiteral(columnComment)}");
                break;
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        Execute(connection, $"DROP TABLE IF EXISTS {table}");
    }

    private static void SetupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupScalarFacets(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BLOB"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
    }

    private static void CleanupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string nameIndex,
        string uniqueIndex)
    {
        CleanupIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {orderNo} int NOT NULL, {name} {text} NOT NULL)",
            $"CREATE INDEX {provider.Escape(nameIndex)} ON {table} ({name})",
            $"CREATE UNIQUE INDEX {provider.Escape(uniqueIndex)} ON {table} ({tenantId}, {orderNo})");
    }

    private static void CleanupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string partialIndex,
        string includedIndex,
        string descendingIndex)
    {
        CleanupProviderIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var activeType = kind == ProviderKind.SqlServer
            ? "bit"
            : kind == ProviderKind.Postgres
                ? "boolean"
                : "int";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {active} {activeType} NOT NULL, {includedValue} int NOT NULL)",
            $"CREATE INDEX {provider.Escape(descendingIndex)} ON {table} ({name} DESC)");

        if (kind != ProviderKind.MySql)
            Execute(connection, $"CREATE INDEX {provider.Escape(partialIndex)} ON {table} ({name}) WHERE {activePredicate}");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            Execute(connection, $"CREATE INDEX {provider.Escape(includedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static void CleanupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        CleanupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var touched = provider.Escape("Touched");
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection, $"CREATE TABLE {table} ({id} {intType} NOT NULL PRIMARY KEY, {touched} {intType} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, triggerName)}} ON {{table}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                        UPDATE target
                        SET {{touched}} = 1
                        FROM {{table}} AS target
                        INNER JOIN inserted AS source ON source.{{id}} = target.{{id}};
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                Execute(connection, $$"""
                    CREATE FUNCTION {{provider.Escape(functionName)}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        NEW."Touched" := 1;
                        RETURN NEW;
                    END
                    $$
                    """);
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {provider.Escape(functionName)}()");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW SET NEW.{touched} = 1");
                break;
            case ProviderKind.Sqlite:
                Execute(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(triggerName)}} AFTER INSERT ON {{table}}
                    BEGIN
                        UPDATE {{table}} SET {{touched}} = 1 WHERE {{id}} = NEW.{{id}};
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{triggerName}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, triggerName)}",
                    DropTable(kind, "dbo." + tableName, SqlServerQualified(provider, tableName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)} ON {provider.Escape(tableName)}",
                    $"DROP FUNCTION IF EXISTS {provider.Escape(functionName)}()",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            case ProviderKind.MySql:
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)}",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void SetupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupProviderSpecificColumnDiagnostics(connection, provider, kind, tableName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var providerColumnSql = kind switch
        {
            ProviderKind.SqlServer => $"{provider.Escape("Location")} geometry NULL",
            ProviderKind.Postgres => $"{provider.Escape("Address")} inet NULL",
            ProviderKind.MySql => $"{provider.Escape("Location")} POINT NULL",
            ProviderKind.Sqlite => $"{provider.Escape("Location")} GEOMETRY NULL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {providerColumnSql})");
    }

    private static void CleanupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        Execute(connection, DropTable(kind, tableName, table));
    }

    private static void SetupSafeProviderSpecificColumns(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupSafeProviderSpecificColumns(connection, provider, kind, tableName);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");

        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} UUID NOT NULL, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("XmlPayload")} XML NULL)");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {provider.Escape("XmlPayload")} xml NOT NULL)");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL)");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("FiscalYear")} YEAR NOT NULL, {provider.Escape("Status")} ENUM('draft','paid','cancelled') NOT NULL, {provider.Escape("Flags")} SET('read','write','admin') NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupSafeProviderSpecificColumns(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var rawName = kind == ProviderKind.SqlServer ? "dbo." + tableName : tableName;
        Execute(connection, DropTable(kind, rawName, table));
    }

    private static void SetupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        CleanupWritableProviderSpecificDiagnostics(connection, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
            {
                var table = Qualified(provider, "dbo", tableName);
                var aliasType = Qualified(provider, "dbo", typeName);
                var decimalAliasType = Qualified(provider, "dbo", decimalTypeName);
                var binaryAliasType = Qualified(provider, "dbo", binaryTypeName);
                Execute(connection,
                    $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL",
                    $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL",
                    $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");
                break;
            }
            case ProviderKind.Postgres:
            {
                var table = Qualified(provider, "public", tableName);
                var domain = Qualified(provider, "public", typeName.ToLowerInvariant());
                var scoreDomain = Qualified(provider, "public", decimalTypeName.ToLowerInvariant());
                var scoreArrayDomain = Qualified(provider, "public", arrayTypeName);
                var statusEnum = Qualified(provider, "public", enumTypeName);
                var statusDomain = Qualified(provider, "public", enumDomainName);
                Execute(connection,
                    $"CREATE DOMAIN {domain} AS varchar(320) CHECK (VALUE LIKE '%@%')",
                    $"CREATE DOMAIN {scoreDomain} AS numeric(18,4) CHECK (VALUE >= 0)",
                    $"CREATE DOMAIN {scoreArrayDomain} AS integer[]",
                    $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active', 'archived')",
                    $"CREATE DOMAIN {statusDomain} AS {statusEnum}",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("Email")} {domain} NOT NULL, {provider.Escape("Score")} {scoreDomain} NOT NULL, {provider.Escape("Scores")} {scoreArrayDomain} NOT NULL, {provider.Escape("Status")} {statusDomain} NOT NULL)");
                break;
            }
            case ProviderKind.MySql:
            {
                var table = provider.Escape(tableName);
                Execute(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("UnsignedCount")} INT UNSIGNED NOT NULL, {provider.Escape("UnsignedTotal")} BIGINT UNSIGNED NOT NULL, {provider.Escape("UnsignedAmount")} DECIMAL(18,4) UNSIGNED NOT NULL)");
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
        }
    }

    private static void CleanupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    DropTable(kind, "dbo." + tableName, Qualified(provider, "dbo", tableName)),
                    $"IF TYPE_ID(N'dbo.{binaryTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", binaryTypeName)}",
                    $"IF TYPE_ID(N'dbo.{decimalTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", decimalTypeName)}",
                    $"IF TYPE_ID(N'dbo.{typeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", typeName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    DropTable(kind, "public." + tableName, Qualified(provider, "public", tableName)),
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", enumDomainName)}",
                    $"DROP TYPE IF EXISTS {Qualified(provider, "public", enumTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", arrayTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", decimalTypeName.ToLowerInvariant())}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", typeName.ToLowerInvariant())}");
                break;
            case ProviderKind.MySql:
                Execute(connection, DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
        }
    }

    private static void SetupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string keylessTable)
    {
        CleanupKeylessWarning(connection, provider, keylessTable);

        var table = provider.Escape(keylessTable);
        var status = provider.Escape("Status");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({status} {text} NOT NULL)");
    }

    private static void CleanupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        string keylessTable)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(keylessTable)}");
    }

    private static void SetupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupEfStyleAliasTable(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static string EfProviderPackageName(ProviderKind kind)
        => kind switch
        {
            ProviderKind.SqlServer => "Microsoft.EntityFrameworkCore.SqlServer",
            ProviderKind.Postgres => "Npgsql.EntityFrameworkCore.PostgreSQL",
            ProviderKind.MySql => "Pomelo.EntityFrameworkCore.MySql",
            _ => "Microsoft.EntityFrameworkCore.Sqlite"
        };

    private static void SetupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        CleanupRoutineStub(connection, provider, kind, routineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT AS SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Routine <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'PROCEDURE', @level1name=" + SqlServerLiteral(routineName));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(tenantId integer) RETURNS TABLE(\"Id\" integer, \"Name\" text) LANGUAGE SQL AS $$ SELECT tenantId, 'ok'::text $$",
                    $"COMMENT ON FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(integer) IS {SqlLiteral("Routine <summary> & description")}");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT) COMMENT {SqlLiteral("Routine <summary> & description")} SELECT tenantId AS Id, 'ok' AS Name");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}",
            ProviderKind.Postgres => $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer)",
            ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.")
        });
    }

    private static void SetupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        CleanupAdvancedRoutineStub(connection, provider, kind, routineName, tableFunctionName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)} (@customerId INT) RETURNS INT AS BEGIN RETURN @customerId + 7; END",
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)} (@tenantId INT) RETURNS TABLE AS RETURN SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(ids integer[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) $$");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape(routineName)}(customer_id INT UNSIGNED, max_id BIGINT UNSIGNED, {provider.Escape("rank")} SMALLINT UNSIGNED, flag TINYINT UNSIGNED) RETURNS INT DETERMINISTIC NO SQL RETURN customer_id");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{tableFunctionName}', N'IF') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'FN') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer[], uuid)");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void SetupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        CleanupRoutineOutputAndNonQueryWrappers(connection, provider, kind, routineName, nonQueryRoutineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT, @total DECIMAL(18,2) OUTPUT, @message NVARCHAR(32) OUTPUT AS BEGIN SET @total = 12.34; SET @message = N'ok'; SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name; END",
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)} @tenantId INT, @status NVARCHAR(32) OUTPUT AS BEGIN SET NOCOUNT ON; SET @status = N'ok'; DECLARE @ignored INT = @tenantId; END");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT, OUT total DECIMAL(18,2), INOUT message VARCHAR(32)) BEGIN SET total = 12.34; SET message = CONCAT(COALESCE(message, ''), 'ok'); SELECT tenantId AS Id, 'ok' AS Name; END");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void CleanupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{nonQueryRoutineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void SetupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        CleanupSequenceStub(connection, provider, kind, sequenceName);

        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            ProviderKind.Postgres => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Sequence <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'SEQUENCE', @level1name=" + SqlServerLiteral(sequenceName),
            ProviderKind.Postgres => $"COMMENT ON SEQUENCE {qualifiedName} IS {SqlLiteral("Sequence <summary> & description")}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void CleanupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{sequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {qualifiedName}",
            ProviderKind.Postgres => $"DROP SEQUENCE IF EXISTS {qualifiedName}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void SetupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string dependentTable)
    {
        CleanupFilteredRelationship(connection, provider, principalTable, dependentTable);

        var principal = provider.Escape(principalTable);
        var dependent = provider.Escape(dependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {principal} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {dependent} ({id} int NOT NULL PRIMARY KEY, {parentId} int NOT NULL, {name} {text} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {principal} ({id}))");
    }

    private static void CleanupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string principalTable,
        string dependentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(dependentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(principalTable)}");
    }

    private static void SetupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        CleanupViewQueryArtifact(connection, provider, kind, baseTable, viewName);

        var table = provider.Escape(baseTable);
        var view = provider.Escape(viewName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void AssertViewQueryArtifactCommentDocumentation(ProviderKind kind, string viewCode)
    {
        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            Assert.Contains("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// Name &lt;view&gt; &amp; details", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// <remarks>Maps to column Name</remarks>", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Name <view> & details", viewCode, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("/// Maps to column Name", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// VIEW", viewCode, StringComparison.Ordinal);
        }
    }

    private static void CleanupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        Execute(connection,
            DropView(kind, viewName, provider.Escape(viewName)),
            $"DROP TABLE IF EXISTS {provider.Escape(baseTable)}");
    }

    private static void SetupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        CleanupProviderQueryArtifact(connection, provider, kind, baseName, artifactName);

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE VIRTUAL TABLE {provider.Escape(artifactName)} USING fts5({provider.Escape("Content")})");
                break;
            case ProviderKind.SqlServer:
            {
                var baseTable = Qualified(provider, "dbo", baseName);
                var synonym = Qualified(provider, "dbo", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName) + ", @level2type=N'COLUMN', @level2name=N'Name'",
                    $"CREATE SYNONYM {synonym} FOR {baseTable}");
                break;
            }
            case ProviderKind.Postgres:
            {
                var baseTable = Qualified(provider, "public", baseName);
                var matView = Qualified(provider, "public", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} integer NOT NULL PRIMARY KEY, {name} text NOT NULL)",
                    $"CREATE MATERIALIZED VIEW {matView} AS SELECT {id}, {name} FROM {baseTable}",
                    $"COMMENT ON MATERIALIZED VIEW {matView} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {matView}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, and PostgreSQL.");
        }
    }

    private static void CleanupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection, DropTable(kind, artifactName, provider.Escape(artifactName)));
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{artifactName}', N'SN') IS NOT NULL DROP SYNONYM {Qualified(provider, "dbo", artifactName)}",
                    DropTable(kind, "dbo." + baseName, Qualified(provider, "dbo", baseName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP MATERIALIZED VIEW IF EXISTS {Qualified(provider, "public", artifactName)}",
                    DropTable(kind, "public." + baseName, Qualified(provider, "public", baseName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, and PostgreSQL.");
        }
    }

    private static void SetupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        CleanupDefaultDiscoveryViewQueryArtifact(connection, provider, kind, schemaName, baseTable, viewName);

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");

        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void CleanupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
        var rawTable = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + baseTable
            : baseTable;
        var rawView = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + viewName
            : viewName;

        Execute(connection,
            DropView(kind, rawView, view),
            DropTable(kind, rawTable, table));

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

    private static string DropView(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'V') IS NOT NULL DROP VIEW {escapedName}"
        : $"DROP VIEW IF EXISTS {escapedName}";

    private static void SetupRequiredAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupRequiredAlternateKeyRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {code} {text40} NOT NULL, {name} {text80} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({code})",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentCode} {text40} NOT NULL, {notes} {text80} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static void CleanupRequiredAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupNullableAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupNullableAlternateKeyRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} int NOT NULL PRIMARY KEY, {code} {text} NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({code})",
            $"CREATE TABLE {child} ({id} int NOT NULL PRIMARY KEY, {parentCode} {text} NOT NULL, {notes} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static void CleanupNullableAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} int NOT NULL PRIMARY KEY, {parentId} int NULL, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET NULL ON UPDATE CASCADE)");
    }

    private static void SetupRestrictReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE)");
    }

    private static void SetupSetDefaultReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName,
        string defaultName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind == ProviderKind.SqlServer ? "nvarchar(80)" : "text";
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape(defaultName)} DEFAULT (0)"
            : "DEFAULT 0";

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL {defaultClause}, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)");
    }

    private static void CleanupReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static string ExpectedCascadeForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, cascadeDelete: false);"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, \"{constraintName}\", false);";

    private static string ExpectedReferentialForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string onDelete,
        string onUpdate,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate});"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate}, \"{constraintName}\");";

    private static void AssertPossibleJoinPayloadDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                if (reason.GetString() == "payload-columns")
                    return;
            }
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic for {tableName}.");
    }

    private static void AssertPossibleJoinPayloadDiagnosticWithoutCompositeForeignKey(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            var hasPayloadReason = false;
            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                if (reasonText == "payload-columns")
                {
                    hasPayloadReason = true;
                    continue;
                }

                Assert.NotEqual("composite-foreign-key", reasonText);
            }

            if (hasPayloadReason)
                return;
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic without composite-FK rejection for {tableName}.");
    }

    private static void AssertPossibleManyToManyDiagnosticReason(string warningJsonPath, string tableName, params string[] expectedReasons)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                foreach (var expectedReason in expectedReasons)
                {
                    if (reasonText == expectedReason)
                        return;
                }
            }
        }

        Assert.Fail($"Expected possible many-to-many diagnostic for {tableName} with one of: {string.Join(", ", expectedReasons)}.");
    }

    private static void AssertRelationshipDependentKeyDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("suggestedAction").GetString()?.Contains("primary key", StringComparison.OrdinalIgnoreCase) == true)
            {
                return;
            }
        }

        Assert.Fail($"Expected relationship dependent-key diagnostic for {tableName}.");
    }

    private static void AssertTriggerDiagnostic(string warningJsonPath, string tableName, string triggerName, ProviderKind kind)
    {
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        JsonElement? triggerDiagnostic = null;

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("code").GetString() == "SCF110" &&
                item.GetProperty("category").GetString() == "database-object" &&
                item.GetProperty("kind").GetString() == "Trigger" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == triggerName)
            {
                Assert.False(triggerDiagnostic.HasValue, "Expected exactly one trigger diagnostic.");
                triggerDiagnostic = item;
            }
        }

        Assert.True(triggerDiagnostic.HasValue, "Expected trigger diagnostic SCF110 in scaffold warning JSON.");
        var metadata = triggerDiagnostic.Value.GetProperty("metadata");
        Assert.Equal("Trigger", metadata.GetProperty("providerObjectKind").GetString());
        Assert.True(LastTableNameEquals(metadata.GetProperty("table").GetString(), tableName));
        Assert.Equal(triggerName, metadata.GetProperty("triggerName").GetString());
        Assert.True(metadata.GetProperty("providerOwnedDdl").GetBoolean());
        Assert.False(metadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
        Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
        Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
        Assert.Equal("provider-owned-trigger", metadata.GetProperty("reason").GetString());

        if (kind == ProviderKind.Sqlite)
        {
            Assert.True(metadata.GetProperty("definitionAvailable").GetBoolean());
            Assert.Contains("CREATE TRIGGER", metadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
        }
    }

    private static void AssertProviderSpecificColumnDiagnostic(string warningJsonPath, string columnName, string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-column-type", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertWritableProviderSpecificColumnDiagnostic(
        string warningJsonPath,
        string tableName,
        string columnName,
        string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.False(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected writable provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertFeatureMetadataHasNoProviderOwnedDiagnostics(string warningJsonPath, string tableName)
    {
        if (!File.Exists(warningJsonPath))
            return;

        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        foreach (var item in providerOwned.EnumerateArray())
        {
            var kind = item.GetProperty("kind").GetString();
            var table = item.GetProperty("table").GetString();
            Assert.False(
                LastTableNameEquals(table, tableName) &&
                kind is "Default" or "CheckConstraint" or "Computed" or "Collation",
                $"Expected {tableName} {kind} metadata to be promoted instead of reported as provider-owned diagnostics.");
        }
    }

    private static bool LastTableNameEquals(string? actual, string expected)
    {
        if (string.IsNullOrEmpty(actual))
            return false;

        var candidate = actual;
        var dot = candidate.LastIndexOf('.');
        if (dot >= 0 && dot < candidate.Length - 1)
            candidate = candidate[(dot + 1)..];

        candidate = candidate.Trim('[', ']', '"', '`');
        return string.Equals(candidate, expected, StringComparison.OrdinalIgnoreCase);
    }

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static string SqlServerQualified(DatabaseProvider provider, string name)
        => provider.Escape("dbo") + "." + provider.Escape(name);

    private static string Qualified(DatabaseProvider provider, string schemaName, string tableName)
        => provider.Escape(schemaName) + "." + provider.Escape(tableName);

    private static string ConnectionStringWithDatabase(string connectionString, string databaseName)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        builder["Database"] = databaseName;
        return builder.ConnectionString;
    }

    private static void Execute(DbConnection connection, params string[] statements)
    {
        foreach (var statement in statements)
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    private static void WriteConsumerProject(string root, string output)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(output, "CliLiveScaffolded.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>enable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProject(string root, string projectPath, string? userSecretsId = null)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        var userSecretsProperty = string.IsNullOrWhiteSpace(userSecretsId)
            ? string.Empty
            : $"{Environment.NewLine}                <UserSecretsId>{userSecretsId}</UserSecretsId>";
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <RootNamespace>Live.Project.Namespace</RootNamespace>
                <Nullable>disable</Nullable>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>{{userSecretsProperty}}
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProjectWithoutMetadata(string root, string projectPath)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProjectWithAssemblyMetadata(string root, string projectPath, string userSecretsId)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <AssemblyName>Project.Assembly.Namespace</AssemblyName>
                <Nullable>disable</Nullable>
                <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static string GetUserSecretsFilePathForLiveTest(string userSecretsId)
    {
        if (OperatingSystem.IsWindows())
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            return Path.Combine(appData, "Microsoft", "UserSecrets", userSecretsId, "secrets.json");
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".microsoft", "usersecrets", userSecretsId, "secrets.json");
    }

    private static CliResult RunCli(
        string arguments,
        string workingDirectory,
        IReadOnlyDictionary<string, string?>? environment = null)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}.");

        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory, environment);
    }

    private static void RunDotNet(string arguments, string workingDirectory)
    {
        var result = RunProcess("dotnet", arguments, workingDirectory);
        Assert.True(result.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
    }

    private static CliResult RunProcess(
        string fileName,
        string arguments,
        string workingDirectory,
        IReadOnlyDictionary<string, string?>? environment = null)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        if (environment is not null)
        {
            foreach (var entry in environment)
            {
                if (entry.Value is null)
                    startInfo.Environment.Remove(entry.Key);
                else
                    startInfo.Environment[entry.Key] = entry.Value;
            }
        }

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(ProcessTimeout))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // The process may exit between timeout detection and Kill.
            }

            process.WaitForExit();
            var timedOutStdout = stdoutTask.GetAwaiter().GetResult();
            var timedOutStderr = stderrTask.GetAwaiter().GetResult();
            throw new TimeoutException(
                $"{fileName} {arguments} did not exit within {ProcessTimeout.TotalSeconds:N0} seconds.{Environment.NewLine}STDOUT:{Environment.NewLine}{timedOutStdout}{Environment.NewLine}STDERR:{Environment.NewLine}{timedOutStderr}");
        }

        process.WaitForExit();
        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        return new CliResult(process.ExitCode, stdout, stderr);
    }

    private static string IdentifierSuffix()
    {
        var value = Guid.NewGuid().ToString("N");
        return "A" + value[..6] + "A";
    }

    private static string Quote(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string SqlServerLiteral(string value) => "N" + SqlLiteral(value);

    private static string StripDefaultSchemaArguments(string generatedCode)
        => generatedCode
            .Replace(", schema: \"dbo\"", string.Empty, StringComparison.Ordinal)
            .Replace(", schema: \"public\"", string.Empty, StringComparison.Ordinal);

    private static string IdentityPrimaryKeyColumn(ProviderKind kind, string escapedColumnName) => kind switch
    {
        ProviderKind.SqlServer => $"{escapedColumnName} INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
        ProviderKind.Postgres => $"{escapedColumnName} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
        ProviderKind.MySql => $"{escapedColumnName} INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        _ => $"{escapedColumnName} INTEGER PRIMARY KEY AUTOINCREMENT"
    };

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; failed deletion only leaves a temp directory.
        }
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
