#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
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
    public void Dotnet_norm_scaffold_promotes_safe_feature_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveFeatureOwned" + suffix;
        var checkName = "CK_CliLiveFeatureOwned_Name_" + suffix;
        var defaultName = "DF_CliLiveFeatureOwned_Name_" + suffix;
        var statusDefaultName = "DF_CliLiveFeatureOwned_Status_" + suffix;
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
                SetupFeatureOwnedMetadata(connection, provider, kind, tableName, checkName, defaultName, statusDefaultName);
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
            Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("public byte[] Payload { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode, StringComparison.Ordinal);
            Assert.Contains("NameLength { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            if (kind == ProviderKind.SqlServer)
                Assert.Contains($".Entity<{tableName}>().Property(e => e.Name).HasDefaultValueSql(\"'new'\", constraintName: \"{defaultName}\");", contextCode, StringComparison.Ordinal);
            else
                Assert.Contains($".Entity<{tableName}>().Property(e => e.Name).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.Status).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            if (kind == ProviderKind.SqlServer)
                Assert.Contains($"constraintName: \"{statusDefaultName}\"", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.Payload).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            if (kind == ProviderKind.Postgres)
                Assert.Contains($".Entity<{tableName}>().Property(e => e.CreatedAt).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"{checkName}", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.NameLength).HasComputedColumnSql(", contextCode, StringComparison.Ordinal);
            Assert.Contains($".Entity<{tableName}>().Property(e => e.Name).HasCollation(", contextCode, StringComparison.Ordinal);

            AssertFeatureMetadataHasNoProviderOwnedDiagnostics(warningJsonPath, tableName);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
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
            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"CK_{tableName}_1", contextCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains($".Entity<{tableName}>().HasCheckConstraint(\"CK_{tableName}_", contextCode, StringComparison.Ordinal);
            }

            Assert.DoesNotContain("CK__", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(tableName + "_check", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(tableName + "_chk_", contextCode, StringComparison.OrdinalIgnoreCase);

            AssertFeatureMetadataHasNoProviderOwnedDiagnostics(warningJsonPath, tableName);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
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
            Assert.Contains($"[Index(\"UX_{tableName}_Code\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("UQ__", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain(tableName + "_Code_key", entityCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("[Index(\"Code\"", entityCode, StringComparison.OrdinalIgnoreCase);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
