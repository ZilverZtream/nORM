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
    public void Dotnet_norm_scaffold_marks_sqlserver_rowversion_as_timestamp_and_database_generated()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliRowVersion" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sqlserver_rowversion_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.SqlServer, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSqlServerRowVersion(connection, provider, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRowVersionCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRowVersionCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[Timestamp]", entityCode, StringComparison.Ordinal);
            Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode, StringComparison.Ordinal);
            Assert.Contains("public byte[] RowVersion { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".Property(e => e.RowVersion).HasComputedColumnSql", contextCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
            var rowVersionDiagnostic = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "RowVersion" &&
                item.GetProperty("code").GetString() == "SCF108" &&
                item.GetProperty("table").GetString()!.EndsWith(tableName, StringComparison.Ordinal));
            var metadata = rowVersionDiagnostic.GetProperty("metadata");
            Assert.Contains(
                metadata.GetProperty("providerType").GetString(),
                new[] { "rowversion", "timestamp" },
                StringComparer.OrdinalIgnoreCase);
            Assert.True(metadata.GetProperty("providerOwnedDdl").GetBoolean());
            Assert.True(metadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
            Assert.True(metadata.GetProperty("concurrencyToken").GetBoolean());
            Assert.True(metadata.GetProperty("databaseGenerated").GetBoolean());
            Assert.False(metadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.True(metadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.Equal("provider-managed-rowversion", metadata.GetProperty("reason").GetString());
            Assert.Contains("[Timestamp]", rowVersionDiagnostic.GetProperty("suggestedAction").GetString(), StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.SqlServer, connectionString);
                CleanupSqlServerRowVersion(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupSqlServerRowVersion(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        CleanupSqlServerRowVersion(connection, provider, tableName);

        var table = Qualified(provider, "dbo", tableName);
        Execute(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} int NOT NULL PRIMARY KEY, {provider.Escape("Name")} nvarchar(80) NOT NULL, {provider.Escape("RowVersion")} rowversion NOT NULL)");
    }

    private static void CleanupSqlServerRowVersion(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE {Qualified(provider, "dbo", tableName)}");
    }
}
