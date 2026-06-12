using System;
using System.IO;
using Microsoft.Data.Sqlite;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Scaffold_no_pluralize_preserves_entity_and_query_names()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_cli_no_pluralizer_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_cli_no_pluralizer_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Blogs (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliPluralCtx --no-pluralize",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "Blogs.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliPluralCtx.cs"));
            Assert.Contains("public partial class Blogs", entityCode, StringComparison.Ordinal);
            Assert.Contains("public IQueryable<Blogs> Blogs", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Blogses", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }
}
