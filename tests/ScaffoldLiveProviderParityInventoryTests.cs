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
            ["LiveProviderScaffoldCliManyToManyEdgeTests.cs::Dotnet_norm_scaffold_rejects_filtered_unique_surrogate_join_table_on_live_provider"] =
                Expect("MySQL does not expose filtered unique indexes; this diagnostic is for providers with partial-index metadata.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliManyToManyEdgeTests.cs::Dotnet_norm_scaffold_preserves_schema_qualified_many_to_many_on_live_provider"] =
                Expect("MySQL has database/catalog names rather than schema-qualified table namespaces in this scaffold shape.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliReferentialActionTests.cs::Dotnet_norm_scaffold_preserves_restrict_fk_referential_actions_on_live_provider"] =
                Expect("SQL Server does not accept RESTRICT as a foreign-key referential action.", "Sqlite", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliReferentialActionTests.cs::Dotnet_norm_scaffold_preserves_set_default_fk_referential_actions_on_live_provider"] =
                Expect("MySQL does not support executable SET DEFAULT foreign-key actions in the live schema setup.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliExpressionIndexTests.cs::Dotnet_norm_scaffold_emits_expression_index_metadata_on_live_provider"] =
                Expect("SQL Server has no expression-index DDL equivalent; generated/computed columns plus ordinary indexes are the SQL Server path.", "Sqlite", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliProviderSpecificIndexTests.cs::Dotnet_norm_scaffold_reports_provider_specific_index_implementations_on_live_provider"] =
                Expect("SQLite has no provider-specific index access-method catalog equivalent.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_routine_stubs_on_live_provider"] =
                Expect("SQLite has no stored routine catalog to scaffold.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_advanced_routine_stubs_on_live_provider"] =
                Expect("SQLite has no stored routine catalog to scaffold.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldCliRoutineSequenceTests.cs::Dotnet_norm_scaffold_emits_routine_output_and_non_query_wrappers_on_live_provider"] =
                Expect("PostgreSQL and SQLite do not expose this stored-procedure output/non-query wrapper shape.", "SqlServer", "MySql"),
            ["LiveProviderScaffoldCliSequenceTests.cs::Dotnet_norm_scaffold_emits_sequence_stubs_on_live_provider"] =
                Expect("SQLite and MySQL have no standalone sequence catalog equivalent for this scaffold surface.", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldCliProviderSpecificDiagnosticsTests.cs::Dotnet_norm_scaffold_preserves_writable_provider_specific_diagnostics_on_live_provider"] =
                Expect("This test covers writable provider-specific type systems that SQLite does not expose beyond its declared-type path.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldingIndexTests.cs::ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider"] =
                Expect("MySQL provider-specific access-method diagnostics are covered by the dedicated MySQL access-method test.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_reports_provider_specific_index_access_methods_as_provider_owned"] =
                Expect("SQLite has no provider-specific index access-method catalog equivalent.", "SqlServer", "Postgres", "MySql"),
            ["LiveProviderScaffoldingReferentialActionTests.cs::ScaffoldAsync_preserves_restrict_fk_referential_actions_on_live_provider"] =
                Expect("SQL Server does not accept RESTRICT as a foreign-key referential action.", "Sqlite", "Postgres", "MySql"),
            ["LiveProviderScaffoldingReferentialActionTests.cs::ScaffoldAsync_preserves_set_default_fk_referential_actions_on_live_provider"] =
                Expect("MySQL does not support executable SET DEFAULT foreign-key actions in the live schema setup.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingManyToManyEdgeTests.cs::ScaffoldAsync_rejects_filtered_unique_surrogate_join_table_on_live_provider"] =
                Expect("MySQL does not expose filtered unique indexes; this diagnostic is for providers with partial-index metadata.", "Sqlite", "SqlServer", "Postgres"),
            ["LiveProviderScaffoldingManyToManyEdgeTests.cs::ScaffoldAsync_preserves_schema_qualified_many_to_many_on_live_provider"] =
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

    private static readonly IReadOnlyDictionary<string, PartialCoverageExpectation> ProviderSpecificFactCoverage =
        new Dictionary<string, PartialCoverageExpectation>(StringComparer.Ordinal)
        {
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_reports_mysql_prefix_index_without_emitting_normal_index"] =
                Expect("MySQL prefix indexes are a MySQL-only index metadata shape.", "MySql"),
            ["LiveProviderScaffoldCliMySqlPrefixIndexTests.cs::Dotnet_norm_scaffold_reports_mysql_prefix_index_without_emitting_normal_index"] =
                Expect("MySQL prefix indexes are a MySQL-only index metadata shape.", "MySql"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_emits_supported_mysql_expression_index_metadata"] =
                Expect("MySQL expression index catalog metadata is exposed through MySQL-specific DDL and emitted as provider-bound expression-index metadata when representable.", "MySql"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_emits_postgres_expression_index_with_include_metadata"] =
                Expect("PostgreSQL expression INCLUDE metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldCliPostgresExpressionIndexFacetTests.cs::Dotnet_norm_scaffold_emits_postgres_expression_index_include_metadata"] =
                Expect("PostgreSQL expression INCLUDE metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldCliPostgresExpressionIndexFacetTests.cs::Dotnet_norm_scaffold_emits_postgres_expression_index_null_semantics"] =
                Expect("PostgreSQL expression index NULLS FIRST/LAST and NULLS NOT DISTINCT metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_emits_postgres_null_sort_order_index_metadata"] =
                Expect("PostgreSQL NULLS FIRST/LAST index metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldCliPostgresIndexFacetTests.cs::Dotnet_norm_scaffold_emits_postgres_null_sort_order_index_metadata"] =
                Expect("PostgreSQL NULLS FIRST/LAST index metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_reports_postgres_expression_btree_key_options_as_provider_owned"] =
                Expect("PostgreSQL expression B-tree operator-class/key options are PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingIndexProviderSpecificTests.cs::ScaffoldAsync_emits_postgres_nulls_not_distinct_unique_index_metadata"] =
                Expect("PostgreSQL NULLS NOT DISTINCT unique index metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldCliPostgresIndexFacetTests.cs::Dotnet_norm_scaffold_emits_postgres_nulls_not_distinct_unique_index_metadata"] =
                Expect("PostgreSQL NULLS NOT DISTINCT unique index metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_reports_sqlserver_native_temporal_tables_and_marks_them_read_only"] =
                Expect("SQL Server system-versioned temporal tables are SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_rejects_sqlserver_procedure_synonym_as_entity_filter"] =
                Expect("SQL Server procedure synonyms are SQL Server-specific provider objects.", "SqlServer"),
            ["LiveProviderScaffoldCliSqlServerProcedureSynonymTests.cs::Dotnet_norm_scaffold_rejects_sqlserver_procedure_synonym_as_entity_filter"] =
                Expect("SQL Server procedure synonyms are SQL Server-specific provider objects.", "SqlServer"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_reports_mysql_event_diagnostics_on_live_provider"] =
                Expect("MySQL scheduled events are a MySQL-only routine/object catalog shape.", "MySql"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_emits_sqlite_virtual_table_as_read_only_query_artifact"] =
                Expect("SQLite virtual tables and shadow-table diagnostics are SQLite-specific.", "Sqlite"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_postgres_serial_primary_key_does_not_emit_default_or_owned_sequence_warnings"] =
                Expect("PostgreSQL serial columns and owned sequences are PostgreSQL-specific catalog behavior.", "Postgres"),
            ["LiveProviderScaffoldingProviderObjectTests.cs::ScaffoldAsync_marks_sqlserver_rowversion_as_timestamp_and_database_generated"] =
                Expect("SQL Server rowversion/timestamp concurrency metadata is SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingProviderTypePostgresTests.cs::ScaffoldAsync_emits_postgres_uuid_and_array_columns_on_live_provider"] =
                Expect("PostgreSQL UUID and native array column DDL is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingProviderTypePostgresTests.cs::Dynamic_scaffolding_handles_postgres_uuid_and_array_columns_on_live_provider"] =
                Expect("PostgreSQL UUID and native array runtime metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingProviderTypeMySqlTests.cs::ScaffoldAsync_emits_mysql_json_and_year_columns_on_live_provider"] =
                Expect("MySQL JSON/YEAR/ENUM/SET column metadata is MySQL-specific.", "MySql"),
            ["LiveProviderScaffoldingProviderTypeMySqlTests.cs::Dynamic_scaffolding_handles_mysql_json_year_enum_set_columns_on_live_provider"] =
                Expect("MySQL JSON/YEAR/ENUM/SET runtime metadata is MySQL-specific.", "MySql"),
            ["LiveProviderScaffoldingProviderTypePostgresTests.cs::ScaffoldAsync_reports_postgres_domain_columns_with_underlying_type_on_live_provider"] =
                Expect("PostgreSQL domains and enum-base domains are PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingProviderTypePostgresTests.cs::Dynamic_scaffolding_handles_postgres_domain_columns_on_live_provider"] =
                Expect("PostgreSQL domain runtime metadata is PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingProviderTypeSqlServerTests.cs::ScaffoldAsync_reports_sqlserver_alias_type_columns_with_base_type_on_live_provider"] =
                Expect("SQL Server alias/user-defined types are SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingProviderTypeSqlServerTests.cs::Dynamic_scaffolding_handles_sqlserver_alias_type_columns_on_live_provider"] =
                Expect("SQL Server alias/user-defined runtime metadata is SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingProviderTypeMySqlTests.cs::ScaffoldAsync_reports_mysql_unsigned_columns_as_provider_specific_on_live_provider"] =
                Expect("MySQL unsigned integer and unsigned decimal DDL is MySQL-specific.", "MySql"),
            ["LiveProviderScaffoldingProviderTypeMySqlTests.cs::Dynamic_scaffolding_handles_mysql_unsigned_columns_on_live_provider"] =
                Expect("MySQL unsigned runtime metadata is MySQL-specific.", "MySql"),
            ["LiveProviderScaffoldingProviderTypeMySqlTests.cs::Dynamic_scaffolding_marks_unsafe_mysql_set_columns_read_only_on_live_provider"] =
                Expect("Unsafe MySQL SET write-blocking metadata is MySQL-specific.", "MySql"),
            ["LiveProviderScaffoldingQueryArtifactTests.cs::ScaffoldAsync_emits_postgres_materialized_view_as_read_only_query_artifact"] =
                Expect("PostgreSQL materialized views are PostgreSQL-specific query artifacts.", "Postgres"),
            ["LiveProviderScaffoldingQueryArtifactTests.cs::ScaffoldAsync_emits_sqlserver_local_table_synonym_as_read_only_query_artifact"] =
                Expect("SQL Server local table/view synonyms are SQL Server-specific query artifacts.", "SqlServer"),
            ["LiveProviderScaffoldingSqlServerSynonymQueryArtifactTests.cs::ScaffoldAsync_emits_sqlserver_local_view_synonym_as_read_only_query_artifact"] =
                Expect("SQL Server local view synonyms are SQL Server-specific query artifacts.", "SqlServer"),
            ["LiveProviderScaffoldCliSqlServerSynonymQueryArtifactTests.cs::Dotnet_norm_scaffold_emits_sqlserver_local_view_synonym_as_read_only_query_artifact"] =
                Expect("SQL Server local view synonyms are SQL Server-specific query artifacts.", "SqlServer"),
            ["LiveProviderScaffoldCliManyToManyEdgeTests.cs::Dotnet_norm_scaffold_accepts_mysql_catalog_qualified_many_to_many_filters_on_live_provider"] =
                Expect("MySQL catalog-qualified table filters are the MySQL counterpart to schema-qualified many-to-many filtering; MySQL catalogs are not emitted as nORM schemas.", "MySql"),
            ["LiveProviderScaffoldingManyToManyEdgeTests.cs::ScaffoldAsync_accepts_mysql_catalog_qualified_many_to_many_filters_on_live_provider"] =
                Expect("MySQL catalog-qualified table filters are the MySQL counterpart to schema-qualified many-to-many filtering; MySQL catalogs are not emitted as nORM schemas.", "MySql"),
            ["LiveProviderScaffoldingRoutineOutputTests.cs::ScaffoldAsync_emits_sqlserver_table_valued_parameter_routine_stub_on_live_provider"] =
                Expect("SQL Server table-valued parameters are SQL Server-specific routine metadata.", "SqlServer"),
            ["LiveProviderScaffoldingRoutineOutputTests.cs::ScaffoldAsync_emits_sqlserver_scalar_and_table_valued_function_wrappers_on_live_provider"] =
                Expect("SQL Server scalar and table-valued function catalog shapes are SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_sqlserver_no_result_procedure_as_non_query_wrapper"] =
                Expect("SQL Server no-result stored procedures with output metadata are SQL Server-specific.", "SqlServer"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_postgres_array_and_uuid_routine_parameters_on_live_provider"] =
                Expect("PostgreSQL array and UUID routine parameters are PostgreSQL-specific.", "Postgres"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_postgres_overloaded_function_wrappers_without_collisions"] =
                Expect("PostgreSQL overloaded functions are PostgreSQL-specific routine metadata.", "Postgres"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_postgres_quoted_parameter_function_as_positional_arguments"] =
                Expect("PostgreSQL quoted routine parameter names require PostgreSQL-specific invocation handling.", "Postgres"),
            ["LiveProviderScaffoldingRoutineTests.cs::ScaffoldAsync_emits_mysql_unsigned_routine_parameters_on_live_provider"] =
                Expect("MySQL unsigned routine parameter metadata is MySQL-specific.", "MySql")
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

    [Fact]
    public void Provider_specific_live_provider_scaffold_facts_are_explicitly_justified()
    {
        var providerSpecificFacts = FindProviderSpecificFactCoverage().ToArray();
        var unexpected = providerSpecificFacts
            .Where(item => !ProviderSpecificFactCoverage.ContainsKey(item.Key))
            .Select(item => $"{item.Key} references {string.Join(", ", item.Providers)} at {item.File}:{item.Line}")
            .ToArray();

        Assert.True(
            unexpected.Length == 0,
            "Provider-specific scaffold live-provider [Fact] tests must be listed with a capability reason instead of bypassing the all-four provider inventory: " + string.Join("; ", unexpected));

        foreach (var item in providerSpecificFacts)
        {
            var expectation = ProviderSpecificFactCoverage[item.Key];
            Assert.True(
                SameProviders(item.Providers, expectation.Providers),
                $"{item.Key} provider set changed. Expected {string.Join(", ", expectation.Providers)} because {expectation.Reason}; actual {string.Join(", ", item.Providers)}.");
        }

        var missingExpectedEntries = ProviderSpecificFactCoverage.Keys
            .Except(providerSpecificFacts.Select(item => item.Key), StringComparer.Ordinal)
            .ToArray();
        Assert.True(
            missingExpectedEntries.Length == 0,
            "Remove stale scaffold live-provider provider-specific fact justifications: " + string.Join("; ", missingExpectedEntries));
    }

    [Fact]
    public void Live_provider_scaffold_coverage_keeps_each_provider_substantial()
        => AssertProviderCoverageSubstantial(
            "all live scaffold tests",
            "LiveProviderScaffold*.cs",
            minReferenceRatio: 0.60,
            minFileRatio: 0.80);

    [Fact]
    public void Live_provider_scaffold_coverage_keeps_each_surface_substantial()
    {
        AssertProviderCoverageSubstantial(
            "live scaffold CLI tests",
            "LiveProviderScaffoldCli*.cs",
            minReferenceRatio: 0.60,
            minFileRatio: 0.80);
        AssertProviderCoverageSubstantial(
            "live direct scaffolder tests",
            "LiveProviderScaffolding*.cs",
            minReferenceRatio: 0.50,
            minFileRatio: 0.75);
    }

    private static void AssertProviderCoverageSubstantial(
        string label,
        string filePattern,
        double minReferenceRatio,
        double minFileRatio)
    {
        var summaries = BuildProviderCoverageSummaries(filePattern).ToArray();
        Assert.Equal(AllProviders, summaries.Select(summary => summary.Provider).OrderBy(ProviderOrder));

        var maxReferences = summaries.Max(summary => summary.ReferenceCount);
        var maxFiles = summaries.Max(summary => summary.FileCount);
        var minReferenceCount = (int)Math.Floor(maxReferences * minReferenceRatio);
        var minFileCount = (int)Math.Floor(maxFiles * minFileRatio);
        var weakProviders = summaries
            .Where(summary => summary.ReferenceCount < minReferenceCount || summary.FileCount < minFileCount)
            .Select(summary =>
                $"{summary.Provider}: {summary.ReferenceCount} ProviderKind references across {summary.FileCount} files; " +
                $"minimums are {minReferenceCount} references and {minFileCount} files. Current spread: {FormatCoverageSummaries(summaries)}")
            .ToArray();

        Assert.True(
            weakProviders.Length == 0,
            $"{label} coverage must stay materially all-four-provider even after legitimate provider-specific tests are added: " +
            string.Join("; ", weakProviders));
    }

    private static IEnumerable<PartialCoverage> FindPartialLiveProviderCoverage()
    {
        var providerRegex = new Regex(@"InlineData\(ProviderKind\.(Sqlite|SqlServer|Postgres|MySql)(?:\s*,|\))", RegexOptions.Compiled);
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

    private static IEnumerable<PartialCoverage> FindProviderSpecificFactCoverage()
    {
        var factMethodRegex = new Regex(
            @"(?s)\[Fact(?:\([^\]]*\))?\]\s*public\s+(?:async\s+)?(?:Task|void)\s+([A-Za-z0-9_]+)\s*\([^)]*\)(?<body>.*?)(?=\n\s*\[(?:Fact|Theory)(?:\(|\])|\z)",
            RegexOptions.Compiled);
        var providerRegex = new Regex(@"ProviderKind\.(Sqlite|SqlServer|Postgres|MySql)", RegexOptions.Compiled);

        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot, "tests"), "LiveProviderScaffold*.cs"))
        {
            var text = File.ReadAllText(file);
            foreach (Match match in factMethodRegex.Matches(text))
            {
                var providers = providerRegex.Matches(match.Groups["body"].Value)
                    .Select(provider => provider.Groups[1].Value)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(ProviderOrder)
                    .ToArray();
                if (providers.Length == 0)
                    continue;

                var fileName = Path.GetFileName(file);
                var methodName = match.Groups[1].Value;
                var line = text[..match.Index].Count(c => c == '\n') + 1;
                yield return new PartialCoverage(fileName, line, methodName, providers);
            }
        }
    }

    private static IEnumerable<ProviderCoverageSummary> BuildProviderCoverageSummaries(string filePattern)
    {
        var providerRegex = new Regex(@"ProviderKind\.(Sqlite|SqlServer|Postgres|MySql)", RegexOptions.Compiled);
        var summaries = AllProviders.ToDictionary(
            provider => provider,
            provider => new ProviderCoverageBuilder(provider),
            StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot, "tests"), filePattern))
        {
            var fileName = Path.GetFileName(file);
            var providerCounts = providerRegex.Matches(File.ReadAllText(file))
                .Select(match => match.Groups[1].Value)
                .GroupBy(provider => provider, StringComparer.Ordinal);

            foreach (var providerCount in providerCounts)
            {
                summaries[providerCount.Key].AddFile(fileName, providerCount.Count());
            }
        }

        return summaries.Values
            .Select(builder => builder.ToSummary())
            .OrderBy(summary => ProviderOrder(summary.Provider));
    }

    private static string FormatCoverageSummaries(IEnumerable<ProviderCoverageSummary> summaries)
        => string.Join(
            ", ",
            summaries
                .OrderBy(summary => ProviderOrder(summary.Provider))
                .Select(summary => $"{summary.Provider}={summary.ReferenceCount}/{summary.FileCount}"));

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

    private sealed class ProviderCoverageBuilder(string provider)
    {
        private readonly HashSet<string> _files = new(StringComparer.Ordinal);

        public string Provider { get; } = provider;

        public int ReferenceCount { get; private set; }

        public void AddFile(string file, int references)
        {
            ReferenceCount += references;
            _files.Add(file);
        }

        public ProviderCoverageSummary ToSummary()
            => new(Provider, ReferenceCount, _files.Count);
    }

    private sealed record ProviderCoverageSummary(string Provider, int ReferenceCount, int FileCount);

    private sealed record PartialCoverageExpectation(IReadOnlyList<string> Providers, string Reason);
}
