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
    // Live CLI comment and scalar-facet scaffold diagnostics tests.

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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
