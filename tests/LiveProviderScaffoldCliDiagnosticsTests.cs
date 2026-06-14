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
            Assert.Contains($"[Index(\"UX_{tableName}_Code\", IsUnique = true)]", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("sqlite_autoindex", entityCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("UQ__", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain(tableName + "_Code_key", entityCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("[Index(\"Code\"", entityCode, StringComparison.OrdinalIgnoreCase);

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

            Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(28,6)\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Amount).HasPrecision(28, 6);", contextCode, StringComparison.Ordinal);
            Assert.Contains("[MaxLength(40)]", entityCode, StringComparison.Ordinal);
            Assert.Contains("[MaxLength(12)]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Code).HasMaxLength(40)", contextCode, StringComparison.Ordinal);
            Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12)", contextCode, StringComparison.Ordinal);

            if (kind == ProviderKind.SqlServer)
            {
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Code).HasMaxLength(40).IsUnicode(false);", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.FixedCode).HasMaxLength(12).IsUnicode(false).IsFixedLength();", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{tableName}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
            }
            else if (kind is ProviderKind.MySql or ProviderKind.Sqlite)
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

}
