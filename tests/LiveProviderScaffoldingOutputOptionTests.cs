#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
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
    public async Task ScaffoldAsync_respects_context_directory_namespace_and_nullable_options_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupOutputOptionsTableAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_output_options_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold.Entities",
                    "LiveScaffoldOutputOptionsContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { OutputOptionsTable },
                        ContextDirectory = Path.Combine("Data", "Contexts"),
                        ContextNamespace = "LiveScaffold.Contexts",
                        UseNullableReferenceTypes = false,
                        OverwriteFiles = false
                    });

                var entityPath = Path.Combine(dir, OutputOptionsTable + ".cs");
                var contextPath = Path.Combine(dir, "Data", "Contexts", "LiveScaffoldOutputOptionsContext.cs");
                Assert.True(File.Exists(entityPath));
                Assert.True(File.Exists(contextPath));

                var entityCode = await File.ReadAllTextAsync(entityPath);
                var contextCode = await File.ReadAllTextAsync(contextPath);
                Assert.Contains("namespace LiveScaffold.Entities;", entityCode, StringComparison.Ordinal);
                Assert.Contains("namespace LiveScaffold.Contexts;", contextCode, StringComparison.Ordinal);
                Assert.Contains("using LiveScaffold.Entities;", contextCode, StringComparison.Ordinal);
                Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
                Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{OutputOptionsTable}> {OutputOptionsTable}s", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownOutputOptionsTableAsync(connection, provider, kind);
            }
        }
    }
}
