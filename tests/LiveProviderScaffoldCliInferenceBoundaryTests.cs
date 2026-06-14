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
    public void Dotnet_norm_scaffold_does_not_infer_owned_types_or_inheritance_from_column_conventions_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveInferenceBoundary" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_inference_boundary_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupInferenceBoundary(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveInferenceBoundaryCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveInferenceBoundaryCtx.cs"));
            var generatedCode = entityCode + contextCode;

            Assert.Contains("public string Discriminator { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string ShippingAddressStreet { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string ShippingAddressCity { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"ShippingAddress_Street\")]", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OwnsOne", generatedCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OwnsMany", generatedCode, StringComparison.Ordinal);
            Assert.DoesNotContain("DiscriminatorColumn", generatedCode, StringComparison.Ordinal);
            Assert.DoesNotContain("DiscriminatorValue", generatedCode, StringComparison.Ordinal);
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
                CleanupInferenceBoundary(cleanup, provider, tableName);
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

    private static void SetupInferenceBoundary(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupInferenceBoundary(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var discriminator = provider.Escape("Discriminator");
        var name = provider.Escape("Name");
        var street = provider.Escape("ShippingAddress_Street");
        var city = provider.Escape("ShippingAddress_City");
        var breed = provider.Escape("Dog_Breed");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {discriminator} {text40} NOT NULL, {name} {text80} NOT NULL, {street} {text80} NOT NULL, {city} {text80} NOT NULL, {breed} {text80} NULL)");
    }

    private static void CleanupInferenceBoundary(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
        => Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
}
