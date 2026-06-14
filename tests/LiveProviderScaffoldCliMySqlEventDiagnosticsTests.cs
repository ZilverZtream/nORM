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
    public void Dotnet_norm_scaffold_reports_mysql_event_diagnostics_on_live_provider()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliMySqlEventDiag" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_mysql_event_diag_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.MySql, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                try
                {
                    SetupMySqlEventDiagnostics(connection, provider, tableName);
                }
                catch (Exception ex)
                {
                    CleanupMySqlEventDiagnostics(connection, provider, tableName);
                    if (Skip.If(true, $"MySQL EVENT privilege is not available in this live database: {ex.Message}")) return;
                }
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveMySqlEventCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("public int Id { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();
            Assert.Contains(skippedObjects, item =>
                item.GetProperty("kind").GetString() == "Event" &&
                item.GetProperty("code").GetString() == "SCF205" &&
                item.GetProperty("category").GetString() == "routine" &&
                item.GetProperty("name").GetString() == tableName &&
                item.GetProperty("suggestedAction").GetString()!.Contains("scheduled event", StringComparison.OrdinalIgnoreCase));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.MySql, connectionString);
                CleanupMySqlEventDiagnostics(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupMySqlEventDiagnostics(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        CleanupMySqlEventDiagnostics(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        Execute(connection,
            $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY)",
            $"CREATE EVENT {provider.Escape(tableName)} ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY DO UPDATE {table} SET {id} = {id}");
    }

    private static void CleanupMySqlEventDiagnostics(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection,
            $"DROP EVENT IF EXISTS {provider.Escape(tableName)}",
            $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
