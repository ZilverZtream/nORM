using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Scaffold_pass_through_environment_without_value_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite -- --environment",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("EF-style application argument '--environment' requires a value", result.Stderr, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("-- --environment \"\"")]
    [InlineData("-- --environment=")]
    public void Scaffold_pass_through_environment_blank_value_fails(string applicationArgs)
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite {applicationArgs}",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("EF-style application argument '--environment' requires a value", result.Stderr, StringComparison.Ordinal);
    }


    [Fact]
    public void Scaffold_ignores_ef_style_application_args_after_double_dash()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_app_args_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(output, "app_args.db");

        try
        {
            Directory.CreateDirectory(output);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -- --environment Production --tenant Contoso",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
        }
        finally
        {
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --conection typo",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("'--environment' is used for named-connection appsettings lookup", result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_is_not_masked_by_matching_app_arg()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --bad -- --bad",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
    }

}
