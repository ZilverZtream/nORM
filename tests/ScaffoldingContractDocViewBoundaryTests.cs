#nullable enable

using System;
using Xunit;

namespace nORM.Tests;

public partial class ScaffoldingContractDocTests
{
    [Fact]
    public void Docs_and_gate_pin_key_looking_view_boundary()
    {
        var doc = ReadDoc();
        var liveScaffoldTests = ReadLiveProviderScaffoldingParitySource();
        var liveScaffoldCliTests = ReadLiveProviderScaffoldCliParitySource();

        Assert.Contains("Key-looking view columns such as `Id` and `ParentId` remain scalar properties", doc, StringComparison.Ordinal);
        Assert.Contains("does not infer keys, relationships,", doc, StringComparison.Ordinal);
        Assert.Contains("Key-looking view columns are verified through the", doc, StringComparison.Ordinal);
        Assert.Contains("does not create generated keys, relationships, or write semantics", doc, StringComparison.Ordinal);
        Assert.Contains("key-looking view read-only boundary coverage", doc, StringComparison.Ordinal);
        Assert.Contains("ScaffoldAsync_key_looking_view_columns_stay_read_only_query_artifact_on_live_provider", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("Dotnet_norm_scaffold_key_looking_view_columns_stay_read_only_query_artifact_on_live_provider", liveScaffoldCliTests, StringComparison.Ordinal);
        Assert.Contains("Assert.DoesNotContain(\"[Key]\"", liveScaffoldTests, StringComparison.Ordinal);
        Assert.Contains("Assert.DoesNotContain(\"HasForeignKey\"", liveScaffoldCliTests, StringComparison.Ordinal);
    }
}
