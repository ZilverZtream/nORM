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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
