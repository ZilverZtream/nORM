#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider index scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificIndexesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderIndexContext.cs"));
                var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
                var warnings = File.Exists(warningPath) ? await File.ReadAllTextAsync(warningPath) : string.Empty;
                using var warningJson = File.Exists(warningJsonPath)
                    ? JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath))
                    : JsonDocument.Parse("{\"providerOwnedSchemaFeatures\":[]}");
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains($"[Index(\"{ProviderPartialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{ProviderDescendingIndex}\", IsDescending = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("PartialIndex", warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderPartialIndex, warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderDescendingIndex, warnings, StringComparison.Ordinal);

                if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
                {
                    Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderPartialExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.Contains("filterSql:", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionDescendingIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("name").GetString() == ProviderExpressionDescendingIndex);
                }

                if (kind is ProviderKind.Postgres)
                {
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionLiteralDescIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "DescendingIndex" &&
                        item.GetProperty("name").GetString() == ProviderExpressionLiteralDescIndex);
                }

                if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                {
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("IncludedColumnIndex", warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderIncludedIndex, warnings, StringComparison.Ordinal);
                }

                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PartialIndex" &&
                    item.GetProperty("name").GetString() == ProviderPartialIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderIncludedIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "DescendingIndex" &&
                    item.GetProperty("name").GetString() == ProviderDescendingIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
            }
        }
    }

}
