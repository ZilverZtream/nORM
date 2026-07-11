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
    public void Dotnet_norm_scaffold_no_relationships_keeps_scalar_fk_columns_and_join_entity_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var authorTable = "CliNoRelAuthor" + suffix;
        var bookTable = "CliNoRelBook" + suffix;
        var labelTable = "CliNoRelLabel" + suffix;
        var bookLabelTable = "CliNoRelBookLabel" + suffix;
        var bookAuthorFkName = "FK_CliNoRelBook_Author_" + suffix;
        var bookLabelBookFkName = "FK_CliNoRelBookLabel_Book_" + suffix;
        var bookLabelLabelFkName = "FK_CliNoRelBookLabel_Label_" + suffix;
        var bookAuthorTitleIndex = "IX_CliNoRelBook_Author_Title_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_no_relationships_" + kind + "_" + suffix);
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
                "--context CliLiveNoRelationshipsCtx " +
                "--no-relationships " +
                $"--table {Quote(authorTable)} " +
                $"--table {Quote(bookTable)} " +
                $"--table {Quote(labelTable)} " +
                $"--table {Quote(bookLabelTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var authorCode = File.ReadAllText(Path.Combine(output, authorTable + ".cs"));
            var bookCode = File.ReadAllText(Path.Combine(output, bookTable + ".cs"));
            var labelCode = File.ReadAllText(Path.Combine(output, labelTable + ".cs"));
            var joinCode = File.ReadAllText(Path.Combine(output, bookLabelTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveNoRelationshipsCtx.cs"));

            Assert.DoesNotContain($"List<{bookTable}>", authorCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{bookTable}>", labelCode, StringComparison.Ordinal);
            Assert.Matches(@"public (?:int|long) AuthorId \{ get; set; \}", bookCode);
            Assert.Matches(@"public (?:int|long) BookId \{ get; set; \}", joinCode);
            Assert.Matches(@"public (?:int|long) LabelId \{ get; set; \}", joinCode);
            Assert.DoesNotContain("[ForeignKey(", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {authorTable}", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{labelTable}>", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasMany", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("WithOne", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("WithMany", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("UsingTable", contextCode, StringComparison.Ordinal);
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
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres or ProviderKind.Sqlite)
            {
                // These providers store the user-declared PK constraint name, and the
                // scaffolder preserves explicit names in the generated HasKey call.
                Assert.Contains($"mb.Entity<{parentTable}>().HasKey(e => new {{ e.TenantId, e.OrderNo }}, \"{parentPkName}\");", contextCode, StringComparison.Ordinal);
            }
            else
            {
                // MySQL always reports the fixed name PRIMARY regardless of the declared
                // constraint name, so the generated HasKey stays nameless.
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

}
