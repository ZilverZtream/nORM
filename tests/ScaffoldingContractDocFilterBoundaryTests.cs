#nullable enable

using System;
using Xunit;

namespace nORM.Tests;

public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Docs_and_gate_pin_direct_table_filter_relationship_boundary()
    {
        var doc = ReadDoc();
        var liveScaffoldTests = ReadLiveProviderScaffoldingParitySource();
        var liveScaffoldCliTests = ReadLiveProviderScaffoldCliParitySource();

        Assert.Contains("Table-filtered direct API and CLI", doc, StringComparison.Ordinal);
        Assert.Contains("relationships to unselected principal or dependent tables are", doc, StringComparison.Ordinal);
        Assert.Contains("suppressed rather than emitted as broken navigations", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_table_filter_suppresses_unselected_principal_relationship_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_table_filter_suppresses_unselected_dependent_relationship_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_table_filter_suppresses_unselected_principal_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_table_filter_suppresses_unselected_dependent_relationship_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Assert.DoesNotContain(\"HasForeignKey\"", liveScaffoldTests, StringComparison.Ordinal);
    }
}
