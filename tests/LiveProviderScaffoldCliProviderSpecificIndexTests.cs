#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_reports_provider_specific_index_implementations_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliProviderIndexImpl" + suffix;
        var indexName = "IX_CliProviderIndexImpl_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_index_impl_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                try
                {
                    SetupProviderSpecificIndexImplementation(connection, provider, kind, tableName, indexName);
                }
                catch (DbException ex)
                {
                    CleanupProviderSpecificIndexImplementation(connection, provider, tableName);
                    if (Skip.If(true, $"{kind} provider-specific index implementation is not available on this server: {ex.Message}")) return;
                }
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProviderIndexImplCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveProviderIndexImplCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain(indexName, entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"HasExpressionIndex(\"{indexName}\"", contextCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
            var providerSpecific = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                item.GetProperty("name").GetString() == indexName);
            Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
            Assert.Equal("index", providerSpecific.GetProperty("category").GetString());

            var metadata = providerSpecific.GetProperty("metadata");
            if (kind == ProviderKind.SqlServer)
            {
                Assert.Equal("SQL Server", metadata.GetProperty("provider").GetString());
                Assert.Contains("COLUMNSTORE", metadata.GetProperty("indexType").GetString(), StringComparison.OrdinalIgnoreCase);
            }
            else if (kind == ProviderKind.Postgres)
            {
                Assert.Equal("PostgreSQL", metadata.GetProperty("provider").GetString());
                Assert.Equal("hash", metadata.GetProperty("accessMethod").GetString());
                Assert.Contains("USING hash", metadata.GetProperty("indexSql").GetString(), StringComparison.OrdinalIgnoreCase);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                    item.GetProperty("name").GetString() == indexName);
            }
            else
            {
                Assert.Equal("MySQL", metadata.GetProperty("provider").GetString());
                Assert.Equal("FULLTEXT", metadata.GetProperty("indexType").GetString());
            }

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProviderSpecificIndexImplementation(cleanup, provider, tableName);
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

    private static void SetupProviderSpecificIndexImplementation(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string indexName)
    {
        CleanupProviderSpecificIndexImplementation(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        var text = kind == ProviderKind.SqlServer ? "nvarchar(160)" : "varchar(160)";

        Execute(connection,
            $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {score} int NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection, $"CREATE NONCLUSTERED COLUMNSTORE INDEX {provider.Escape(indexName)} ON {table} ({score})");
                break;
            case ProviderKind.Postgres:
                Execute(connection, $"CREATE INDEX {provider.Escape(indexName)} ON {table} USING hash (lower({name}))");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"CREATE FULLTEXT INDEX {provider.Escape(indexName)} ON {table} ({name})");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider-specific index implementation CLI test excludes SQLite.");
        }
    }

    private static void CleanupProviderSpecificIndexImplementation(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
