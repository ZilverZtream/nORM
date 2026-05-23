using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Every source-generation scenario listed in <c>docs/source-generation.md</c> must be
/// exercised by an in-process test class. The package-consumer test
/// (<see cref="PackageConsumerIntegrationTests"/>) covers the end-to-end nupkg consumption
/// path; this contract additionally pins the per-feature test files so the scenarios cannot
/// regress unnoticed if someone removes one of those files.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SourceGenCoverageContractTests
{
    private static string RepoRoot()
    {
        var asmDir = Path.GetDirectoryName(typeof(SourceGenCoverageContractTests).Assembly.Location)!;
        return Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
    }

    [Theory]
    [InlineData("SourceGenFluentRenameTests.cs", "fluent rename")]
    [InlineData("SourceGenRenamedColumnTests.cs", "RenameColumn attribute")]
    [InlineData("SourceGenOwnedNavigationTests.cs", "owned navigation")]
    [InlineData("SourceGenColumnEscapingTests.cs", "[Column] attribute / escaping")]
    [InlineData("SourceGenConverterParityTests.cs", "value converters")]
    [InlineData("SourceGenMaterializerCorrectnesTests.cs", "materializer correctness")]
    [InlineData("SourceGenMultiModelMaterializerTests.cs", "multi-model materializer")]
    [InlineData("SourceGenRuntimeParityTests.cs", "runtime parity")]
    [InlineData("SourceGenGlobalNamespaceTests.cs", "global namespace generation")]
    [InlineData("SourceGeneratorIntegrationTests.cs", "generator integration / diagnostics")]
    [InlineData("PackageConsumerIntegrationTests.cs", "package-consumer end-to-end")]
    [InlineData("CompiledMaterializerMultiModelTests.cs", "compiled materializer registry")]
    [InlineData("CompileTimeQueryLifecycleTests.cs", "compile-time query")]
    [InlineData("CompileTimeQueryParameterParityTests.cs", "compile-time query parameter parity")]
    public void Source_generation_scenario_test_file_exists(string fileName, string scenario)
    {
        var path = Path.Combine(RepoRoot(), "tests", fileName);
        Assert.True(
            File.Exists(path),
            $"Source-gen scenario '{scenario}' is missing its test file at tests/{fileName}.");
    }

    [Fact]
    public void Source_generation_docs_promise_v1_supported_scenarios()
    {
        var doc = File.ReadAllText(Path.Combine(RepoRoot(), "docs", "source-generation.md"));
        Assert.Contains("v1 Materializer Support", doc, StringComparison.Ordinal);
        Assert.Contains("[GenerateMaterializer]", doc, StringComparison.Ordinal);
        Assert.Contains("[CompileTimeQuery", doc, StringComparison.Ordinal);
    }
}
