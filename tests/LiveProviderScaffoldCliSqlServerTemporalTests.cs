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
    public void Dotnet_norm_scaffold_reports_sqlserver_native_temporal_tables_and_marks_them_read_only()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliTemporalOrder" + suffix;
        var historyName = "CliTemporalOrderHistory" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sqlserver_temporal_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.SqlServer, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSqlServerTemporalTables(connection, provider, tableName, historyName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTemporalCtx " +
                $"--table {Quote("dbo." + tableName)} " +
                $"--table {Quote("dbo." + historyName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var baseCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var historyCode = File.ReadAllText(Path.Combine(output, historyName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
            Assert.Contains("[ReadOnlyEntity]", historyCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
            var baseTemporal = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "TemporalTable" &&
                item.GetProperty("table").GetString() == "dbo." + tableName &&
                item.GetProperty("code").GetString() == "SCF115");
            var baseMetadata = baseTemporal.GetProperty("metadata");
            Assert.Equal("TemporalTable", baseMetadata.GetProperty("providerObjectKind").GetString());
            Assert.True(baseMetadata.GetProperty("providerNativeTemporal").GetBoolean());
            Assert.False(baseMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
            Assert.True(baseMetadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.False(baseMetadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.Equal("provider-native-temporal", baseMetadata.GetProperty("reason").GetString());
            Assert.Equal("system-versioned", baseMetadata.GetProperty("temporalType").GetString());
            Assert.Contains(historyName, baseMetadata.GetProperty("historyTable").GetString(), StringComparison.OrdinalIgnoreCase);

            var historyTemporal = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "TemporalTable" &&
                item.GetProperty("table").GetString() == "dbo." + historyName &&
                item.GetProperty("detail").GetString()!.Contains("history table", StringComparison.OrdinalIgnoreCase));
            var historyMetadata = historyTemporal.GetProperty("metadata");
            Assert.Equal("history", historyMetadata.GetProperty("temporalType").GetString());
            Assert.True(historyMetadata.GetProperty("providerNativeTemporal").GetBoolean());
            Assert.False(historyMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
            Assert.True(historyMetadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.False(historyMetadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.Equal("provider-native-temporal", historyMetadata.GetProperty("reason").GetString());

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.SqlServer, connectionString);
                CleanupSqlServerTemporalTables(cleanup, provider, tableName, historyName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupSqlServerTemporalTables(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string historyName)
    {
        CleanupSqlServerTemporalTables(connection, provider, tableName, historyName);

        var table = Qualified(provider, "dbo", tableName);
        var history = Qualified(provider, "dbo", historyName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var validFrom = provider.Escape("ValidFrom");
        var validTo = provider.Escape("ValidTo");

        Execute(connection, $$"""
            CREATE TABLE {{table}} (
                {{id}} int NOT NULL PRIMARY KEY,
                {{name}} nvarchar(80) NOT NULL,
                {{validFrom}} datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                {{validTo}} datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                PERIOD FOR SYSTEM_TIME ({{validFrom}}, {{validTo}})
            ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {{history}}))
            """);
    }

    private static void CleanupSqlServerTemporalTables(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string historyName)
    {
        Execute(connection, $$"""
            IF OBJECT_ID(N'dbo.{{tableName}}', N'U') IS NOT NULL
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM sys.tables
                    WHERE object_id = OBJECT_ID(N'dbo.{{tableName}}')
                      AND temporal_type = 2
                )
                BEGIN
                    ALTER TABLE {{Qualified(provider, "dbo", tableName)}} SET (SYSTEM_VERSIONING = OFF);
                END;

                DROP TABLE {{Qualified(provider, "dbo", tableName)}};
            END;

            IF OBJECT_ID(N'dbo.{{historyName}}', N'U') IS NOT NULL
                DROP TABLE {{Qualified(provider, "dbo", historyName)}};
            """);
    }
}
