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
    public void Dotnet_norm_scaffold_singularizes_plural_table_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var prefix = "CliPlural" + suffix;
        var blogTable = prefix + "Blogs";
        var categoryTable = prefix + "Categories";
        var classTable = prefix + "Classes";
        var statusTable = prefix + "Statuses";
        var blogEntity = prefix + "Blog";
        var categoryEntity = prefix + "Category";
        var classEntity = prefix + "Class";
        var statusEntity = prefix + "Status";
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_pluralizer_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupPluralizerTables(connection, provider, kind, blogTable, categoryTable, classTable, statusTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLivePluralizerCtx " +
                $"--table {Quote(blogTable)} " +
                $"--table {Quote(categoryTable)} " +
                $"--table {Quote(classTable)} " +
                $"--table {Quote(statusTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            AssertLiveEntity(output, blogEntity, blogTable);
            AssertLiveEntity(output, categoryEntity, categoryTable);
            AssertLiveEntity(output, classEntity, classTable);
            AssertLiveEntity(output, statusEntity, statusTable);

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLivePluralizerCtx.cs"));
            Assert.Contains($"public IQueryable<{blogEntity}> {blogTable}", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public IQueryable<{categoryEntity}> {categoryTable}", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public IQueryable<{classEntity}> {classTable}", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public IQueryable<{statusEntity}> {statusTable}", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Blogses", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Categorieses", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, blogTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, categoryTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, classTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, statusTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupPluralizerTables(cleanup, provider, blogTable, categoryTable, classTable, statusTable);
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

    private static void SetupPluralizerTables(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        params string[] tableNames)
    {
        CleanupPluralizerTables(connection, provider, tableNames);

        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var textType = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        foreach (var tableName in tableNames)
        {
            var table = provider.Escape(tableName);
            Execute(connection,
                $"CREATE TABLE {table} ({provider.Escape("Id")} {idType} NOT NULL PRIMARY KEY, {provider.Escape("Name")} {textType} NOT NULL)");
        }
    }

    private static void CleanupPluralizerTables(
        DbConnection connection,
        DatabaseProvider provider,
        params string[] tableNames)
    {
        foreach (var tableName in tableNames)
            Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void AssertLiveEntity(string output, string entityName, string tableName)
    {
        var entityCode = File.ReadAllText(Path.Combine(output, entityName + ".cs"));
        Assert.Contains("public partial class " + entityName, entityCode, StringComparison.Ordinal);
        Assert.Contains($"[Table(\"{tableName}\"", entityCode, StringComparison.Ordinal);
    }
}
