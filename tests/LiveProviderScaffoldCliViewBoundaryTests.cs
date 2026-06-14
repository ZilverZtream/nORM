#nullable enable

using System;
using System.Data.Common;
using System.IO;
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
    public void Dotnet_norm_scaffold_key_looking_view_columns_stay_read_only_query_artifact_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseTable = "CliLiveViewBoundaryBase" + suffix;
        var viewName = "CliLiveViewBoundaryReport" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_view_boundary_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeyLookingViewBoundary(connection, provider, kind, baseTable, viewName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveViewBoundaryCtx " +
                $"--table {Quote(viewName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, viewName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveViewBoundaryCtx.cs"));

            AssertKeyLookingCliViewStayedReadOnly(viewCode, contextCode, viewName);
            Assert.False(File.Exists(Path.Combine(output, baseTable + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md")), StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeyLookingViewBoundary(cleanup, provider, kind, baseTable, viewName);
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

    private static void SetupKeyLookingViewBoundary(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        CleanupKeyLookingViewBoundary(connection, provider, kind, baseTable, viewName);

        var table = provider.Escape(baseTable);
        var view = provider.Escape(viewName);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE VIEW {view} AS SELECT {id}, {id} AS {parentId}, {name} AS {displayName} FROM {table}");
    }

    private static void CleanupKeyLookingViewBoundary(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        Execute(connection,
            DropView(kind, viewName, provider.Escape(viewName)),
            DropTable(kind, baseTable, provider.Escape(baseTable)));
    }

    private static void AssertKeyLookingCliViewStayedReadOnly(
        string viewCode,
        string contextCode,
        string entityName)
    {
        Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
        Assert.Contains($"[Table(\"{entityName}", viewCode, StringComparison.Ordinal);
        Assert.Contains($"public partial class {entityName}", viewCode, StringComparison.Ordinal);
        Assert.Matches(@"public (?:int|long)\?? Id \{ get; set; \}", viewCode);
        Assert.Matches(@"public (?:int|long)\?? ParentId \{ get; set; \}", viewCode);
        Assert.Matches(@"public string\?? DisplayName \{ get; set; \}(?: = default!;)?", viewCode);
        Assert.Contains($"IQueryable<{entityName}>", contextCode, StringComparison.Ordinal);
        Assert.DoesNotContain("[Key]", viewCode, StringComparison.Ordinal);
        Assert.DoesNotContain("[ForeignKey(", viewCode, StringComparison.Ordinal);
        Assert.DoesNotContain("HasKey", contextCode, StringComparison.Ordinal);
        Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
    }
}
