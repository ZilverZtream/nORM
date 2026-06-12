#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ScaffoldLiveProviderParityInventoryTests
{
    private static readonly string RepoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
    private static readonly string[] AllProviders = { "Sqlite", "SqlServer", "Postgres", "MySql" };

    private static readonly IReadOnlyDictionary<string, PartialCoverageExpectation> IntentionalPartialCoverage =
        new Dictionary<string, PartialCoverageExpectation>(StringComparer.Ordinal)
        {
            ["LiveProviderScaffoldCliManyToManyTests.cs::Dotnet_norm_scaffold_rejects_filtered_unique_surrogate_join_table_on_live_provider"] =
                Expect("MySQL does not expose filtered unique indexes; this diagnostic is for providers with partial-index metadata.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliManyToManyTests.cs::Dotnet_norm_scaffold_preserves_schema_qualified_many_to_many_on_live_provider"] =
                Expect("MySQL has database/catalog names rather than schema-qualified table namespaces in this scaffold shape.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliReferentialActionTests.cs::Dotnet_norm_scaffold_preserves_restrict_fk_referential_actions_on_live_provider"] =
                Expect("SQL Server does not accept RESTRICT as a foreign-key referential action.", "Sqlite", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliReferentialActionTests.cs::Dotnet_norm_scaffold_preserves_set_default_fk_referential_actions_on_live_provider"] =
                Expect("MySQL does not support executable SET DEFAULT foreign-key actions in the live schema setup.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_routine_stubs_on_live_provider"] =
                Expect("SQLite has no stored routine catalog to scaffold.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_advanced_routine_stubs_on_live_provider"] =
                Expect("SQLite has no stored routine catalog to scaffold.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_routine_output_and_non_query_wrappers_on_live_provider"] =
                Expect("PostgreSQL and SQLite do not expose this stored-procedure output/non-query wrapper shape.", "SqlServer", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_sequence_stubs_on_live_provider"] =
                Expect("SQLite and MySQL have no standalone sequence catalog equivalent for this scaffold surface.", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliWarningDiagnosticsTests.cs::Dotnet_norm_scaffold_preserves_writable_provider_specific_diagnostics_on_live_provider"] =
                Expect("This test covers writable provider-specific type systems that SQLite does not expose beyond its declared-type path.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldingIndexTests.cs::ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider"] =
                Expect("MySQL provider-specific access-method diagnostics are covered by the dedicated MySQL access-method test.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingIndexTests.cs::ScaffoldAsync_reports_provider_specific_index_access_methods_as_provider_owned"] =
                Expect("SQLite has no provider-specific index access-method catalog equivalent.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldingManyToManyTests.cs::ScaffoldAsync_preserves_restrict_fk_referential_actions_on_live_provider"] =
                Expect("SQL Server does not accept RESTRICT as a foreign-key referential action.", "Sqlite", "Postgres", "MySql"),
            ["LiveProviderScaffoldingManyToManyTests.cs::ScaffoldAsync_preserves_set_default_fk_referential_actions_on_live_provider"] =
                Expect("MySQL does not support executable SET DEFAULT foreign-key actions in the live schema setup.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingManyToManyTests.cs::ScaffoldAsync_rejects_filtered_unique_surrogate_join_table_on_live_provider"] =
                Expect("MySQL does not expose filtered unique indexes; this diagnostic is for providers with partial-index metadata.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingManyToManyTests.cs::ScaffoldAsync_preserves_schema_qualified_many_to_many_on_live_provider"] =
                Expect("MySQL has database/catalog names rather than schema-qualified table namespaces in this scaffold shape.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingRoutineOutputTests.cs::ScaffoldAsync_emits_routine_output_factories_on_live_provider"] =
                Expect("PostgreSQL and SQLite do not expose this stored-procedure output factory shape.", "SqlServer", "MySql"),
            ["LiveProviderScaffoldingRoutineOutputTests.cs::Scaffolded_routine_output_invocation_name_executes_on_live_provider"] =
                Expect("PostgreSQL and SQLite do not expose this stored-procedure output invocation shape.", "SqlServer", "MySql"),
            ["LiveProviderScaffoldingRoutineOutputTests.cs::ScaffoldAsync_emits_sequence_wrappers_on_live_provider"] =
                Expect("SQLite and MySQL have no standalone sequence catalog equivalent for this scaffold surface.", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_routine_stubs_on_live_provider"] =
                Expect("SQLite has no stored routine catalog to scaffold.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_postgres_set_returning_function_as_table_valued_wrapper"] =
                Expect("This is a PostgreSQL-specific set-returning-function scaffold shape.", "Postgres")
        };

    [Fact]
    public void Live_provider_scaffold_tests_are_all_four_or_explicitly_justified()
    {
        var partialCoverage = FindPartialLiveProviderCoverage().ToArray();
        var unexpected = partialCoverage
            .Where(item => !IntentionalPartialCoverage.ContainsKey(item.Key))
            .Select(item => $"{item.Key} has only {string.Join(", ", item.Providers)} at {item.File}:{item.Line}")
            .ToArray();

        Assert.True(
            unexpected.Length == 0,
            "Every scaffold live-provider test should cover all four providers or document the provider capability reason: " + string.Join("; ", unexpected));

        foreach (var item in partialCoverage)
        {
            var expectation = IntentionalPartialCoverage[item.Key];
            Assert.True(
                SameProviders(item.Providers, expectation.Providers),
                $"{item.Key} provider set changed. Expected {string.Join(", ", expectation.Providers)} because {expectation.Reason}; actual {string.Join(", ", item.Providers)}.");
        }

        var missingExpectedEntries = IntentionalPartialCoverage.Keys
            .Except(partialCoverage.Select(item => item.Key), StringComparer.Ordinal)
            .ToArray();
        Assert.True(
            missingExpectedEntries.Length == 0,
            "Remove stale scaffold live-provider partial-coverage justifications: " + string.Join("; ", missingExpectedEntries));
    }

    private static IEnumerable<PartialCoverage> FindPartialLiveProviderCoverage()
    {
        var providerRegex = new Regex(@"InlineData\(ProviderKind\.(Sqlite|SqlServer|Postgres|MySql)\)", RegexOptions.Compiled);
        var methodRegex = new Regex(@"public\s+(?:async\s+)?(?:Task|void)\s+([A-Za-z0-9_]+)\s*\(", RegexOptions.Compiled);
        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot, "tests"), "LiveProviderScaffold*.cs"))
        {
            var lines = File.ReadAllLines(file);
            var providers = new List<string>();
            for (var i = 0; i < lines.Length; i++)
            {
                var providerMatch = providerRegex.Match(lines[i]);
                if (providerMatch.Success)
                    providers.Add(providerMatch.Groups[1].Value);

                var methodMatch = methodRegex.Match(lines[i]);
                if (!methodMatch.Success)
                    continue;

                var providerSet = providers.Distinct(StringComparer.Ordinal).OrderBy(ProviderOrder).ToArray();
                if (providerSet.Length is > 0 and < 4)
                {
                    var fileName = Path.GetFileName(file);
                    var methodName = methodMatch.Groups[1].Value;
                    yield return new PartialCoverage(fileName, i + 1, methodName, providerSet);
                }

                providers.Clear();
            }
        }
    }

    private static PartialCoverageExpectation Expect(string reason, params string[] providers)
    {
        Assert.False(string.IsNullOrWhiteSpace(reason));
        Assert.NotEmpty(providers);
        return new PartialCoverageExpectation(providers.Distinct(StringComparer.Ordinal).OrderBy(ProviderOrder).ToArray(), reason);
    }

    private static int ProviderOrder(string provider)
    {
        var index = Array.IndexOf(AllProviders, provider);
        return index < 0 ? int.MaxValue : index;
    }

    private static bool SameProviders(IReadOnlyCollection<string> left, IReadOnlyCollection<string> right)
        => left.Count == right.Count && left.OrderBy(ProviderOrder).SequenceEqual(right.OrderBy(ProviderOrder), StringComparer.Ordinal);

    private sealed record PartialCoverage(string File, int Line, string Method, IReadOnlyList<string> Providers)
    {
        public string Key => File + "::" + Method;
    }

    private sealed record PartialCoverageExpectation(IReadOnlyList<string> Providers, string Reason);
}
