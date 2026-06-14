#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_emits_sqlserver_local_view_synonym_as_read_only_query_artifact()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseTable = "CliViewSynonymBase" + suffix;
        var viewName = "CliViewSynonymReport" + suffix;
        var synonymName = "CliViewSynonym" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_view_synonym_sqlserver_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.SqlServer, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSqlServerViewSynonymQueryArtifact(connection, provider, baseTable, viewName, synonymName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveViewSynonymCtx " +
                "--emit-query-artifacts " +
                $"--table {Quote("dbo." + synonymName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var synonymCode = File.ReadAllText(Path.Combine(output, synonymName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveViewSynonymCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", synonymCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{synonymName}", synonymCode, StringComparison.Ordinal);
            Assert.Contains("Schema = \"dbo\"", synonymCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {synonymName}", synonymCode, StringComparison.Ordinal);
            Assert.Contains("/// View &lt;summary&gt; &amp; description", synonymCode, StringComparison.Ordinal);
            Assert.Contains("/// Name &lt;view&gt; &amp; details", synonymCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[Key]", synonymCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{synonymName}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasKey", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, baseTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, viewName + ".cs")));
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "dbo." + synonymName);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.SqlServer, connectionString);
                CleanupSqlServerViewSynonymQueryArtifact(cleanup, provider, baseTable, viewName, synonymName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupSqlServerViewSynonymQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        string baseTable,
        string viewName,
        string synonymName)
    {
        CleanupSqlServerViewSynonymQueryArtifact(connection, provider, baseTable, viewName, synonymName);

        var table = Qualified(provider, "dbo", baseTable);
        var view = Qualified(provider, "dbo", viewName);
        var synonym = Qualified(provider, "dbo", synonymName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        Execute(connection,
            $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) NOT NULL)",
            $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
            "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'",
            $"CREATE SYNONYM {synonym} FOR {view}");
    }

    private static void CleanupSqlServerViewSynonymQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        string baseTable,
        string viewName,
        string synonymName)
    {
        Execute(connection,
            $"IF OBJECT_ID(N'dbo.{synonymName}', N'SN') IS NOT NULL DROP SYNONYM {Qualified(provider, "dbo", synonymName)}",
            DropView(ProviderKind.SqlServer, "dbo." + viewName, Qualified(provider, "dbo", viewName)),
            DropTable(ProviderKind.SqlServer, "dbo." + baseTable, Qualified(provider, "dbo", baseTable)));
    }
}
