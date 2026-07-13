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
    [Fact]
    public void Dotnet_norm_scaffold_reports_postgres_deferrable_fk_semantics_as_relationship_diagnostic()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliLiveDeferrableParent" + suffix;
        var childTable = "CliLiveDeferrableChild" + suffix;
        var fkName = "FK_CliLiveDeferrable_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_deferrable_fk_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.Postgres, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupPostgresDeferrableReferentialActionRelationship(connection, provider, parentTable, childTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDeferrableFkCtx " +
                $"--table {Quote(parentTable)} " +
                $"--table {Quote(childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDeferrableFkCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("public int ParentId { get; set; }", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"public {parentTable}", childCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasForeignKey(", contextCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
            var diagnostic = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "ReferentialAction" &&
                item.GetProperty("name").GetString() == fkName);
            Assert.Equal("SCF106", diagnostic.GetProperty("code").GetString());
            Assert.Contains("DEFERRABLE", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
            var metadata = diagnostic.GetProperty("metadata");
            Assert.True(metadata.GetProperty("navigationSuppressed").GetBoolean());
            Assert.False(metadata.GetProperty("generatedNavigationSupported").GetBoolean());
            Assert.Equal("referential-action-not-scaffoldable", metadata.GetProperty("reason").GetString());
            Assert.Contains("DEFERRABLE", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.Postgres, connectionString);
                CleanupReferentialActionRelationship(cleanup, provider, childTable, parentTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupPostgresDeferrableReferentialActionRelationship(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        Execute(connection,
            $"CREATE TABLE {parent} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY)",
            $"CREATE TABLE {child} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("ParentId")} integer NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({provider.Escape("ParentId")}) REFERENCES {parent} ({provider.Escape("Id")}) DEFERRABLE INITIALLY DEFERRED)");
    }
}
