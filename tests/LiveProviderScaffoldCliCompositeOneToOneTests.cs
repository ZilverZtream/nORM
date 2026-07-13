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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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
}
