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
    public void Dotnet_norm_scaffold_generates_self_referencing_one_to_one_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var personTable = "CliSelfOnePerson" + suffix;
        var fkName = "FK_CliSelfOnePerson_Spouse_" + suffix;
        var indexName = "UX_CliSelfOnePerson_Spouse_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_self_one_to_one_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSelfReferencingOneToOneForeignKey(connection, provider, kind, personTable, fkName, indexName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSelfOneToOneCtx " +
                $"--table {Quote(personTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var personCode = File.ReadAllText(Path.Combine(output, personTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSelfOneToOneCtx.cs"));

            Assert.Contains($"[Index(\"{indexName}\", IsUnique = true)]", personCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(SpouseId))]", personCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long)\? SpouseId \{ get; set; \}", personCode);
            Assert.Contains($"public {personTable}? Spouse {{ get; set; }}", personCode, StringComparison.Ordinal);
            Assert.Contains($"public {personTable}? {personTable}BySpouseId {{ get; set; }}", personCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"List<{personTable}>", personCode, StringComparison.Ordinal);
            Assert.Contains($".HasOne(p => p.{personTable}BySpouseId)", contextCode, StringComparison.Ordinal);
            Assert.Contains(".WithOne(d => d.Spouse)", contextCode, StringComparison.Ordinal);
            Assert.Contains(ExpectedCascadeForeignKey(kind, "d => d.SpouseId", "p => p.Id", fkName), contextCode, StringComparison.Ordinal);
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
                CleanupSelfReferencingOneToOneForeignKey(cleanup, provider, personTable);
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

    private static void SetupSelfReferencingOneToOneForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string personTable,
        string fkName,
        string indexName)
    {
        CleanupSelfReferencingOneToOneForeignKey(connection, provider, personTable);

        var person = provider.Escape(personTable);
        var id = provider.Escape("Id");
        var spouseId = provider.Escape("SpouseId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {person} ({id} {idType} NOT NULL PRIMARY KEY, {spouseId} {idType} NULL, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({spouseId}) REFERENCES {person} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {person} ({spouseId})");
    }

    private static void CleanupSelfReferencingOneToOneForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string personTable)
        => Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(personTable)}");
}
