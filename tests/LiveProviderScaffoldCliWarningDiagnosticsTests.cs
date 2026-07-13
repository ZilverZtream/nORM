#nullable enable

using System;
using System.IO;
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
    public void Dotnet_norm_scaffold_reports_trigger_diagnostics_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveTriggerAudit" + suffix;
        var triggerName = "TR_CliLiveTriggerAudit_Touch_" + suffix;
        var functionName = "fn_CliLiveTriggerAuditTouch_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_trigger_diag_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_trigger_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTriggerDiagnosticsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains("Touched { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath), "Trigger diagnostics must write the scaffold warning JSON report.");
            AssertTriggerDiagnostic(warningJsonPath, tableName, triggerName, kind);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                if (scratchDatabase is null)
                    CleanupTriggerDiagnostics(cleanup, provider, kind, tableName, triggerName, functionName);
                else
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
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
    public void Dotnet_norm_scaffold_json_fail_on_warnings_reports_live_provider_diagnostics(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var keylessTable = "CliLiveWarning_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_warning_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeylessWarning(connection, provider, kind, keylessTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveWarningCtx " +
                "--fail-on-warnings " +
                "--json " +
                $"--table {Quote(keylessTable)}",
                root);

            Assert.NotEqual(0, scaffold.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using var document = JsonDocument.Parse(scaffold.Stdout);
            var json = document.RootElement;
            var warnings = json.GetProperty("warnings");
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.Contains("Scaffolding produced warnings", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.True(warnings.GetProperty("hasDiagnostics").GetBoolean());
            Assert.True(warnings.GetProperty("reportsWritten").GetBoolean());
            Assert.Equal(1, warnings.GetProperty("codes").GetProperty("SCF116").GetInt32());
            Assert.Equal(1, warnings.GetProperty("categories").GetProperty("table-shape").GetInt32());

            var warningMarkdown = Path.Combine(output, "nORM.ScaffoldWarnings.md");
            var warningJson = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningMarkdown));
            Assert.True(File.Exists(warningJson));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeylessWarning(cleanup, provider, keylessTable);
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
