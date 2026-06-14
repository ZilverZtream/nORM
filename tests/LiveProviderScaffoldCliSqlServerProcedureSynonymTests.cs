#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_rejects_sqlserver_procedure_synonym_as_entity_filter()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var procedureName = "CliProcedureSynonymTarget" + suffix;
        var synonymName = "CliProcedureSynonym" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sqlserver_proc_synonym_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.SqlServer, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSqlServerProcedureSynonym(connection, provider, procedureName, synonymName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProcedureSynonymCtx " +
                "--emit-query-artifacts " +
                $"--table {Quote("dbo." + synonymName)}",
                root);

            Assert.NotEqual(0, scaffold.ExitCode);
            var combinedOutput = scaffold.Stdout + Environment.NewLine + scaffold.Stderr;
            Assert.Contains("matched database object", combinedOutput, StringComparison.Ordinal);
            Assert.Contains("Synonym dbo." + synonymName, combinedOutput, StringComparison.Ordinal);
            Assert.Contains("does not emit as entity classes", combinedOutput, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, synonymName + ".cs")));
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.SqlServer, connectionString);
                CleanupSqlServerProcedureSynonym(cleanup, provider, procedureName, synonymName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupSqlServerProcedureSynonym(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string procedureName,
        string synonymName)
    {
        CleanupSqlServerProcedureSynonym(connection, provider, procedureName, synonymName);

        var procedure = Qualified(provider, "dbo", procedureName);
        var synonym = Qualified(provider, "dbo", synonymName);
        Execute(connection,
            $"CREATE PROCEDURE {procedure} AS SELECT 1 AS {provider.Escape("Value")}",
            $"CREATE SYNONYM {synonym} FOR {procedure}");
    }

    private static void CleanupSqlServerProcedureSynonym(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string procedureName,
        string synonymName)
    {
        Execute(connection,
            $"IF OBJECT_ID(N'dbo.{synonymName}', N'SN') IS NOT NULL DROP SYNONYM {Qualified(provider, "dbo", synonymName)}",
            $"IF OBJECT_ID(N'dbo.{procedureName}', N'P') IS NOT NULL DROP PROCEDURE {Qualified(provider, "dbo", procedureName)}");
    }
}
