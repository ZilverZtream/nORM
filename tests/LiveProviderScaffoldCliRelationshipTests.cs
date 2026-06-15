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
    public void Dotnet_norm_scaffold_suppresses_keyless_principal_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var principalTable = "CliKeylessPrincipal" + suffix;
        var childTable = "CliKeylessPrincipalChild" + suffix;
        var uniqueName = "UX_CliKeylessPrincipal_ExternalId_" + suffix;
        var fkName = "FK_CliKeylessPrincipalChild_Principal_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_keyless_principal_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeylessPrincipalRelationship(connection, provider, kind, principalTable, childTable, uniqueName, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveKeylessPrincipalCtx " +
                $"--table {Quote(principalTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var principalCode = File.ReadAllText(Path.Combine(output, principalTable + ".cs"));
            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveKeylessPrincipalCtx.cs"));

            Assert.Contains("[ReadOnlyEntity]", principalCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{uniqueName}\", IsUnique = true)]", principalCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {principalTable}", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain(childTable + "s", principalCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
            AssertRelationshipPrincipalKeyDiagnostic(
                Path.Combine(output, "nORM.ScaffoldWarnings.json"),
                childTable,
                principalTable,
                fkName,
                "PrincipalExternalId",
                "ExternalId");

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeylessPrincipalRelationship(cleanup, provider, principalTable, childTable);
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
