using System;
using System.IO;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the structure of <c>eng/benchmark-thresholds.json</c> so a malformed thresholds file
/// is caught here instead of inside a multi-hour BenchmarkDotNet run. The script
/// <c>eng/check-benchmark-thresholds.ps1</c> reads this file and the BenchmarkDotNet artifacts
/// to fail the release gate on regressions; that script trusts the schema enforced below.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BenchmarkThresholdContractTests
{
    private static JsonElement ReadThresholds()
    {
        var asmDir = Path.GetDirectoryName(typeof(BenchmarkThresholdContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        var path = Path.Combine(repoRoot, "eng", "benchmark-thresholds.json");
        Assert.True(File.Exists(path), $"benchmark-thresholds.json not found at {path}");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return doc.RootElement.Clone();
    }

    [Fact]
    public void Thresholds_file_is_valid_json_with_v1_schema()
    {
        var root = ReadThresholds();
        Assert.Equal(1, root.GetProperty("schemaVersion").GetInt32());
        Assert.True(root.GetProperty("rules").GetArrayLength() >= 5, "expected at least 5 v1 RC rules");
    }

    [Fact]
    public void Every_rule_declares_required_fields()
    {
        var root = ReadThresholds();
        foreach (var rule in root.GetProperty("rules").EnumerateArray())
        {
            Assert.True(rule.TryGetProperty("name", out _), $"rule missing 'name'");
            Assert.True(rule.TryGetProperty("targetMethods", out _), $"rule missing 'targetMethods'");
            Assert.True(rule.TryGetProperty("baselineMethods", out _), $"rule missing 'baselineMethods'");
            Assert.True(rule.TryGetProperty("maxMeanRatio", out _), $"rule missing 'maxMeanRatio'");
            Assert.True(rule.TryGetProperty("rationale", out _), $"rule missing 'rationale'");
        }
    }

    [Fact]
    public void Required_rules_cover_core_v1_targets()
    {
        var root = ReadThresholds();
        var targetMethodNames = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var rule in root.GetProperty("rules").EnumerateArray())
        {
            foreach (var m in rule.GetProperty("targetMethods").EnumerateArray())
            {
                targetMethodNames.Add(m.GetString() ?? string.Empty);
            }
        }
        Assert.Contains("Query_Simple_nORM", targetMethodNames);
        Assert.Contains("Query_Simple_nORM_Compiled", targetMethodNames);
        Assert.Contains("Query_Complex_nORM", targetMethodNames);
        Assert.Contains("Query_Complex_nORM_Compiled", targetMethodNames);
        Assert.Contains("Query_Join_nORM_Compiled", targetMethodNames);
        Assert.Contains("BulkInsert_Idiomatic_nORM", targetMethodNames);
    }
}
