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
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, cascadeDelete: false);", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasForeignKey(d => d.ParentId, p => p.Id, \"", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("sqlite_fk_", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("FK__", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("_fkey", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("_ibfk_", contextCode, StringComparison.OrdinalIgnoreCase);
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
    public void Dotnet_norm_scaffold_maps_temporal_store_types_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliTemporalStoreTypes" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_temporal_store_types_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupTemporalStoreTypes(connection, provider, kind, tableName);
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
                "--context CliLiveTemporalStoreTypeCtx " +
                $"--table {Quote(tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("public DateOnly BusinessDate { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public DateTime CreatedAt { get; set; }", entityCode, StringComparison.Ordinal);
            if (kind == ProviderKind.MySql)
            {
                Assert.Contains("public TimeSpan? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("OffsetAt", entityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public DateTimeOffset? OffsetAt { get; set; }", entityCode, StringComparison.Ordinal);
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
                CleanupTemporalStoreTypes(cleanup, provider, kind, tableName);
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
