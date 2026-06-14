#nullable enable

using System;
using System.IO;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    public void Dotnet_norm_scaffold_emits_sequence_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var sequenceName = "CliSequence" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_sequence_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSequenceStub(connection, provider, kind, sequenceName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSequenceCtx " +
                "--emit-sequence-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSequenceCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Sequence scaffolding should keep provider-owned sequence metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var sequence = Assert.Single(
                warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                item => item.GetProperty("kind").GetString() == "Sequence" &&
                        item.GetProperty("name").GetString()!.EndsWith(sequenceName, StringComparison.Ordinal));

            Assert.Contains("public async Task<", contextCode, StringComparison.Ordinal);
            Assert.Contains($"Next{sequenceName}ValueAsync", contextCode, StringComparison.Ordinal);
            Assert.Contains("QueryUnchangedAsync<", contextCode, StringComparison.Ordinal);
            Assert.Contains(
                kind == ProviderKind.SqlServer ? "NEXT VALUE FOR" : "nextval('",
                contextCode,
                StringComparison.Ordinal);
            Assert.Contains("/// Sequence &lt;summary&gt; &amp; description", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Sequence <summary> & description", contextCode, StringComparison.Ordinal);
            Assert.Contains("dataType", sequence.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupSequenceStub(cleanup, provider, kind, sequenceName);
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
}
