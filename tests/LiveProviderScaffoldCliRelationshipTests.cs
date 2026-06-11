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
}