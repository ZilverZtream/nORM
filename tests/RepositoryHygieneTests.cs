using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class RepositoryHygieneTests
{
    private static readonly string RepoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
    private const int MaxProductionScaffoldingFileLines = 250;
    private const int MaxCliScaffoldingFileLines = 200;
    private const int MaxCliDesignTimeFileLines = 200;
    private const int MaxProviderMobilityCertificationFileLines = 200;
    private const int MaxProviderMobilitySourceScannerFileLines = 200;
    private const int MaxProviderMobilitySchemaInspectorFileLines = 200;
    private const int MaxProviderMobilityTranslationFileLines = 250;
    private const int MaxCliIntegrationFileLines = 1300;
    private const int MaxCoreQueryTranslatorFileLines = 1000;
    private const int MaxQueryTranslatorPartialFileLines = 1200;
    private const int MaxNormQueryProviderPartialFileLines = 1500;
    private const int MaxSelectClauseVisitorPartialFileLines = 1500;
    private const int MaxExpressionToSqlVisitorPartialFileLines = 1500;
    private const int MaxSqliteProviderPartialFileLines = 1500;
    private const int MaxConcreteProviderPartialFileLines = 1500;
    private const int MaxDatabaseProviderPartialFileLines = 1500;
    private const int MaxDbContextPartialFileLines = 1000;
    private const int MaxSchemaSnapshotFileLines = 1500;
    private const int MaxEntityTypeBuilderFileLines = 1500;
    private const int MaxMaterializerFactoryFileLines = 1500;

    [Fact]
    public void Test_project_does_not_suppress_async_warning_as_release_exception()
    {
        var project = File.ReadAllText(Path.Combine(RepoRoot, "tests", "nORM.Tests.csproj"));

        Assert.DoesNotContain("CS1998", project, StringComparison.Ordinal);
        Assert.DoesNotContain("WarningsNotAsErrors", project, StringComparison.Ordinal);
    }

    [Fact]
    public void Generated_test_artifacts_are_ignored_and_not_tracked()
    {
        var gitignore = File.ReadAllText(Path.Combine(RepoRoot, ".gitignore"));
        var hygiene = File.ReadAllText(Path.Combine(RepoRoot, "docs", "repository-hygiene.md"));
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));

        Assert.Contains("tests/TestResults/", gitignore, StringComparison.Ordinal);
        Assert.Contains(".tmp/", gitignore, StringComparison.Ordinal);
        Assert.Contains("*.trx", gitignore, StringComparison.Ordinal);
        Assert.Contains("*.coverage", gitignore, StringComparison.Ordinal);
        Assert.Contains(".tmp/", hygiene, StringComparison.Ordinal);
        Assert.Contains("tests/TestResults/", hygiene, StringComparison.Ordinal);
        Assert.Contains("test-suite-ownership.md", hygiene, StringComparison.Ordinal);
        Assert.Contains("Do not add new catch-all `CoverageBoost` files.", ownership, StringComparison.Ordinal);

        var trackedArtifacts = GetTrackedFiles()
            .Where(path => path.StartsWith("tests/TestResults/", StringComparison.OrdinalIgnoreCase) ||
                           path.EndsWith(".trx", StringComparison.OrdinalIgnoreCase) ||
                           path.EndsWith(".coverage", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.Empty(trackedArtifacts);
    }

    [Fact]
    public void Mixed_scaffolding_runtime_coverage_group_stays_decomposed()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Do not recreate `CoverageBoostScaffoldingRuntimeGroupsTests.cs`", ownership, StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Combine(RepoRoot, "tests", "CoverageBoostScaffoldingRuntimeGroupsTests.cs")));
    }

    [Fact]
    public void Scaffolding_contract_doc_tests_keep_source_readers_split()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        var assertions = File.ReadAllText(Path.Combine(RepoRoot, "tests", "ScaffoldingContractDocTests.cs"));
        var sources = File.ReadAllText(Path.Combine(RepoRoot, "tests", "ScaffoldingContractDocTestSources.cs"));

        Assert.Contains("Scaffolding contract source-reader helpers stay in", ownership, StringComparison.Ordinal);
        Assert.DoesNotContain("private static string ReadRepoFile", assertions, StringComparison.Ordinal);
        Assert.Contains("private static string ReadRepoFile", sources, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_live_provider_partial_coverage_stays_explicit()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        var inventory = File.ReadAllText(Path.Combine(RepoRoot, "tests", "ScaffoldLiveProviderParityInventoryTests.cs"));

        Assert.Contains("Scaffolding Live-Provider Matrix", ownership, StringComparison.Ordinal);
        Assert.Contains("Scaffold live-provider tests should cover SQLite, SQL Server, PostgreSQL, and", ownership, StringComparison.Ordinal);
        Assert.Contains("provider coverage must be listed", ownership, StringComparison.Ordinal);
        Assert.Contains("ScaffoldLiveProviderParityInventoryTests", ownership, StringComparison.Ordinal);
        Assert.Contains("Live_provider_scaffold_tests_are_all_four_or_explicitly_justified", inventory, StringComparison.Ordinal);
        Assert.Contains("IntentionalPartialCoverage", inventory, StringComparison.Ordinal);
    }

    [Fact]
    public void Encoding_scan_rejects_replacement_and_mojibake_markers()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        var script = File.ReadAllText(Path.Combine(RepoRoot, "eng", "scripts", "check-encoding.ps1"));

        Assert.Contains("Encoding Gate", ownership, StringComparison.Ordinal);
        Assert.Contains("0xFFFD", script, StringComparison.Ordinal);
        Assert.Contains("0x00E2", script, StringComparison.Ordinal);
        Assert.Contains("0x00C3", script, StringComparison.Ordinal);
        Assert.Contains("Test-TextHasMojibakeMarker", script, StringComparison.Ordinal);
    }

    [Fact]
    public void Production_scaffolding_files_stay_split_by_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Production scaffolding files stay below 250 lines", ownership, StringComparison.Ordinal);

        var scaffoldingDirectory = Path.Combine(RepoRoot, "src", "nORM", "Scaffolding");
        var oversizedFiles = Directory.EnumerateFiles(scaffoldingDirectory, "*.cs", SearchOption.AllDirectories)
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProductionScaffoldingFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split production scaffolding code before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Cli_scaffolding_files_stay_split_by_command_area()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("CLI scaffold command files stay below 200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "Program.Scaffolding*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxCliScaffoldingFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split CLI scaffold command code before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Cli_design_time_files_stay_split_by_loader_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("CLI design-time loading files stay below 200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "Program.DesignTime*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxCliDesignTimeFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split CLI design-time loading code before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Provider_mobility_certification_files_stay_split_by_report_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Provider mobility certification files stay below 200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "ProviderMobilityCertification*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProviderMobilityCertificationFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split provider mobility certification code before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Provider_mobility_source_scanner_files_stay_split_by_scan_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Provider mobility source scanner files stay below 200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "ProviderMobilitySourceScanner*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProviderMobilitySourceScannerFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split provider mobility source scanning by traversal, rules, project files, source files, and finding classification before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Provider_mobility_schema_inspector_files_stay_split_by_schema_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Provider mobility schema inspector files stay below 200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "dotnet-norm"), "ProviderMobilitySchemaInspector*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProviderMobilitySchemaInspectorFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split provider mobility schema inspection by table traversal, columns, defaults, foreign keys, provider generation, and type resolution before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Provider_mobility_translation_files_stay_split_by_contract_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Provider mobility translation files stay below 250 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Configuration"), "ProviderMobility*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxProviderMobilityTranslationFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split provider mobility translation by public contract models, finding classification, runtime strict checks, provider capabilities, versions, implementation strategy groups, and SQL probes before it becomes a god object: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Cli_integration_tests_stay_split_by_command_area()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("CLI integration tests stay below 1300 lines per file", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "tests"), "CliIntegration*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxCliIntegrationFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split CLI integration tests by command area before they become god files: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Core_query_translator_file_stays_split_from_plan_and_client_tail_helpers()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("The central `QueryTranslator.cs` file stays below 1000 lines", ownership, StringComparison.Ordinal);

        var queryTranslatorPath = Path.Combine(RepoRoot, "src", "nORM", "Query", "QueryTranslator.cs");
        var lineCount = File.ReadLines(queryTranslatorPath).Count();

        Assert.True(
            lineCount <= MaxCoreQueryTranslatorFileLines,
            $"Keep QueryTranslator.cs focused on translator state and dispatch; split plan generation, post-materialization, or sub-context helpers before it grows back into a god file ({lineCount} lines).");
    }

    [Fact]
    public void Query_translator_partials_stay_split_by_operator_family()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `QueryTranslator*.cs` partial stays below 1200 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Query"), "QueryTranslator*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxQueryTranslatorPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split QueryTranslator partials by operator family before they become god files: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Sqlite_provider_partials_stay_split_by_provider_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `SqliteProvider*.cs` partial stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Providers"), "SqliteProvider*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxSqliteProviderPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split SQLite provider code by translation, schema/temporal, and bulk responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Concrete_provider_partials_stay_split_by_provider_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `SqlServerProvider*.cs`, `PostgresProvider*.cs`, and `MySqlProvider*.cs` file stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = new[] { "SqlServerProvider*.cs", "PostgresProvider*.cs", "MySqlProvider*.cs" }
            .SelectMany(pattern => Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Providers"), pattern))
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxConcreteProviderPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split concrete provider code by core dialect, scalar SQL, regex, translation, temporal/tenant/runtime, and bulk responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Database_provider_partials_stay_split_by_base_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `DatabaseProvider*.cs` partial stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Providers"), "DatabaseProvider*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxDatabaseProviderPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split DatabaseProvider base code by capability/dialect, temporal/tenant, scalar SQL expression, runtime/connection, bulk, and DML SQL responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void DbContext_partials_stay_split_by_context_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `DbContext*.cs` partial stays below 1000 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Core"), "DbContext*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxDbContextPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split DbContext code by construction, connection/command infrastructure, mapping/query roots, transactions, tenant/temporal APIs, disposal, raw SQL, prepared statements, change tracking, and write operations before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Schema_snapshot_files_stay_split_by_migration_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `SchemaSnapshot*.cs` file stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Migration"), "SchemaSnapshot*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxSchemaSnapshotFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split schema snapshot code by snapshot DTOs, model scanning, destructive-change diagnostics, and diffing before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Entity_type_builder_files_stay_split_by_configuration_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `EntityTypeBuilder*.cs` file stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Configuration"), "EntityTypeBuilder*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxEntityTypeBuilderFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split EntityTypeBuilder code by configuration storage, core entity API, property builders, reference builders, and collection/many-to-many builders before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Materializer_factory_files_stay_split_by_query_pipeline_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `MaterializerFactory*.cs` file stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Query"), "MaterializerFactory*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxMaterializerFactoryFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split MaterializerFactory code by cache state, compiled guards, IL precompile, public APIs, schema-aware mapping, core materialization, constructor/projection helpers, projection-column extraction, reader emitters, conversions, and support types before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Norm_query_provider_partials_stay_split_by_execution_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `NormQueryProvider*.cs` partial stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Query"), "NormQueryProvider*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxNormQueryProviderPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split NormQueryProvider code by aggregate rewrite, execution, simple-query, CUD, and streaming/plan responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Select_clause_visitor_partials_stay_split_by_projection_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `SelectClauseVisitor*.cs` partial stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Query"), "SelectClauseVisitor*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxSelectClauseVisitorPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split SelectClauseVisitor code by method-call, navigation, formatting, operator, and helper responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    [Fact]
    public void Expression_to_sql_visitor_partials_stay_split_by_translation_responsibility()
    {
        var ownership = File.ReadAllText(Path.Combine(RepoRoot, "docs", "test-suite-ownership.md"));
        Assert.Contains("Every `ExpressionToSqlVisitor*.cs` partial stays below 1500 lines", ownership, StringComparison.Ordinal);

        var oversizedFiles = Directory.EnumerateFiles(Path.Combine(RepoRoot, "src", "nORM", "Query"), "ExpressionToSqlVisitor*.cs")
            .Select(path => new
            {
                Path = Path.GetRelativePath(RepoRoot, path).Replace(Path.DirectorySeparatorChar, '/'),
                LineCount = File.ReadLines(path).Count()
            })
            .Where(file => file.LineCount > MaxExpressionToSqlVisitorPartialFileLines)
            .OrderByDescending(file => file.LineCount)
            .Select(file => $"{file.Path} ({file.LineCount} lines)")
            .ToArray();

        Assert.True(
            oversizedFiles.Length == 0,
            "Split ExpressionToSqlVisitor code by binary/null, member/constant, control-flow, method-call, navigation, and support responsibilities before it becomes a god file: " + string.Join(", ", oversizedFiles));
    }

    private static string[] GetTrackedFiles()
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "git",
            WorkingDirectory = RepoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        process.StartInfo.ArgumentList.Add("ls-files");
        process.Start();
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0, error);
        return output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
    }
}
