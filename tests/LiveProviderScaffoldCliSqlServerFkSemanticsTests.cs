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
    public void Dotnet_norm_scaffold_reports_sqlserver_disabled_fk_state_as_relationship_diagnostic()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var parentTable = "CliSqlServerFkParent" + suffix;
        var childTable = "CliSqlServerFkChild" + suffix;
        var fkName = "FK_CliSqlServerFkState_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sqlserver_fk_state_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.SqlServer, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSqlServerForeignKeyState(connection, provider, parentTable, childTable, fkName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliSqlServerFkStateCtx " +
                $"--table {Quote("dbo." + parentTable)} " +
                $"--table {Quote("dbo." + childTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var childCode = File.ReadAllText(Path.Combine(output, childTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliSqlServerFkStateCtx.cs"));
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
            Assert.Contains("NOT TRUSTED", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("DISABLED", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("NOT FOR REPLICATION", diagnostic.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);

            var metadata = diagnostic.GetProperty("metadata");
            Assert.True(metadata.GetProperty("navigationSuppressed").GetBoolean());
            Assert.False(metadata.GetProperty("generatedNavigationSupported").GetBoolean());
            Assert.Equal("referential-action-not-scaffoldable", metadata.GetProperty("reason").GetString());
            Assert.Contains("NOT TRUSTED", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("DISABLED", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("NOT FOR REPLICATION", metadata.GetProperty("onUpdate").GetString(), StringComparison.OrdinalIgnoreCase);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.SqlServer, connectionString);
                CleanupSqlServerForeignKeyState(cleanup, provider, parentTable, childTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupSqlServerForeignKeyState(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupSqlServerForeignKeyState(connection, provider, parentTable, childTable);

        var parent = Qualified(provider, "dbo", parentTable);
        var child = Qualified(provider, "dbo", childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        Execute(connection,
            $"CREATE TABLE {parent} ({id} int NOT NULL PRIMARY KEY)",
            $"CREATE TABLE {child} ({id} int NOT NULL PRIMARY KEY, {parentId} int NOT NULL)",
            $"ALTER TABLE {child} WITH NOCHECK ADD CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) NOT FOR REPLICATION",
            $"ALTER TABLE {child} NOCHECK CONSTRAINT {provider.Escape(fkName)}");
    }

    private static void CleanupSqlServerForeignKeyState(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable)
    {
        Execute(connection,
            DropTable(ProviderKind.SqlServer, "dbo." + childTable, Qualified(provider, "dbo", childTable)),
            DropTable(ProviderKind.SqlServer, "dbo." + parentTable, Qualified(provider, "dbo", parentTable)));
    }
}
