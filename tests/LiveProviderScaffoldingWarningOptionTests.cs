#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_fail_on_warnings_writes_diagnostics_then_throws_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupWarningDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_fail_on_warnings_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldFailOnWarningsContext",
                        new ScaffoldOptions
                        {
                            Tables = new[] { WarningTable, KeylessTable },
                            FailOnWarnings = true,
                            OverwriteFiles = false
                        }));

                Assert.Contains("Scaffolding produced warnings", ex.Message, StringComparison.Ordinal);
                Assert.True(File.Exists(DefaultScaffoldEntityPath(dir, WarningTable)));
                Assert.True(File.Exists(DefaultScaffoldEntityPath(dir, KeylessTable)));
                Assert.True(File.Exists(Path.Combine(dir, "LiveScaffoldFailOnWarningsContext.cs")));

                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                var warningJson = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json"));
                Assert.Contains("MissingPrimaryKey", warnings, StringComparison.Ordinal);
                Assert.Contains(KeylessTable, warnings, StringComparison.Ordinal);
                Assert.Contains("\"providerOwnedSchemaFeatures\"", warningJson, StringComparison.Ordinal);
                Assert.Contains(KeylessTable, warningJson, StringComparison.Ordinal);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownWarningDiagnosticsAsync(connection, provider, kind);
            }
        }
    }
}
