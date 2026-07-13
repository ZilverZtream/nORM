#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_postgres_serial_primary_key_does_not_emit_default_or_owned_sequence_warnings()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresSerial" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_postgres_serial_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.Postgres, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupPostgresSerialPrimaryKey(connection, provider, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLivePostgresSerialCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Identity)]", entityCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.Postgres, connectionString);
                CleanupPostgresSerialPrimaryKey(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupPostgresSerialPrimaryKey(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        CleanupPostgresSerialPrimaryKey(connection, provider, tableName);

        Execute(connection,
            $"CREATE TABLE {provider.Escape(tableName)} ({provider.Escape("Id")} SERIAL PRIMARY KEY, {provider.Escape("Name")} text NOT NULL)");
    }

    private static void CleanupPostgresSerialPrimaryKey(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
