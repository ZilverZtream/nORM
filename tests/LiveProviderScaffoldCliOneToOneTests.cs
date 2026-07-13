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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
