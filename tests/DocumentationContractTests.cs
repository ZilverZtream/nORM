using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

public class DocumentationContractTests
{
    [Fact]
    public void Readme_links_to_precise_linq_support_matrix()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var matrix = File.ReadAllText(Path.Combine(root, "docs", "linq-support.md"));

        Assert.Contains("docs/linq-support.md", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Complete LINQ Support", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("full LINQ", readme, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("| Feature | Status | Notes |", matrix, StringComparison.Ordinal);
        Assert.Contains("ExecuteUpdateAsync", matrix, StringComparison.Ordinal);
        Assert.Contains("AsAsyncEnumerable", matrix, StringComparison.Ordinal);
        Assert.Contains("Unsupported", matrix, StringComparison.Ordinal);
    }

    [Fact]
    public void Linq_support_matrix_rows_have_executable_coverage_inventory()
    {
        var root = FindRepositoryRoot();
        var matrix = File.ReadAllText(Path.Combine(root, "docs", "linq-support.md"));
        var coveragePath = Path.Combine(root, "docs", "linq-support-coverage.md");
        var coverage = File.ReadAllText(coveragePath);

        Assert.Contains("docs/linq-support-coverage.md", matrix, StringComparison.Ordinal);

        foreach (var line in matrix.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None))
        {
            if (!line.StartsWith("| ", StringComparison.Ordinal) ||
                line.StartsWith("| Feature", StringComparison.Ordinal) ||
                line.StartsWith("| ---", StringComparison.Ordinal))
                continue;

            var columns = line.Split('|');
            if (columns.Length < 4)
                continue;

            var feature = columns[1].Trim();
            var status = columns[2].Trim();
            if (status.Equals("Unsupported", StringComparison.OrdinalIgnoreCase))
                continue;

            Assert.Contains($"| {feature} |", coverage, StringComparison.Ordinal);
        }

        var referencedTests = Regex.Matches(coverage, @"`(tests/[^`]+\.cs)`")
            .Select(match => match.Groups[1].Value)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(referencedTests);
        foreach (var referencedTest in referencedTests)
        {
            var path = Path.Combine(root, referencedTest.Replace('/', Path.DirectorySeparatorChar));
            Assert.True(File.Exists(path), $"{coveragePath} references missing test file {referencedTest}.");
        }
    }

    [Fact]
    public void Readme_uses_bounded_v1_claims()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));

        Assert.DoesNotContain("drop-in replacement", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("outperforms", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("beats raw ADO", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Enterprise Ready", readme, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("built-in pooling", readme, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Benchmark Governance", readme, StringComparison.Ordinal);
        Assert.Contains("Documented LINQ Support", readme, StringComparison.Ordinal);
    }

    [Fact]
    public void Generated_api_docs_do_not_reference_removed_public_types()
    {
        var root = FindRepositoryRoot();
        var apiDirectory = Path.Combine(root, "docs", "api");
        var exportedTypes = typeof(DbContext).Assembly
            .GetExportedTypes()
            .Select(t => t.FullName?.Replace('+', '.') ?? t.Name)
            .ToHashSet(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(apiDirectory, "*.yml", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(file);
            Assert.DoesNotContain("nORM.Core.ConnectionPool", text, StringComparison.Ordinal);

            var pageType = Regex.Match(
                text,
                @"(?ms)^items:\s*\r?\n-\s+uid:\s+(nORM\.[^\r\n]+).*?^\s+type:\s+(Class|Struct|Enum|Interface)");
            if (pageType.Success)
                Assert.True(exportedTypes.Contains(pageType.Groups[1].Value), $"{file} documents missing public type {pageType.Groups[1].Value}.");
        }
    }

    [Fact]
    public void Readme_links_to_aot_and_trimming_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "aot-trimming.md"));

        Assert.Contains("docs/aot-trimming.md", readme, StringComparison.Ordinal);
        Assert.Contains("NativeAOT", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresDynamicCode", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresUnreferencedCode", policy, StringComparison.Ordinal);
        Assert.Contains("negative `PublishTrimmed=true` smoke test", policy, StringComparison.Ordinal);
        Assert.Contains("explicitly unsupported for v1", policy, StringComparison.Ordinal);
        Assert.Contains("not supported", policy, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Source_generation_docs_define_v1_diagnostics_and_limits()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "source-generation.md"));

        Assert.Contains("docs/source-generation.md", readme, StringComparison.Ordinal);
        Assert.Contains("v1 Materializer Support", contract, StringComparison.Ordinal);
        Assert.Contains("nORMSG005", contract, StringComparison.Ordinal);
        Assert.Contains("nORMSG006", contract, StringComparison.Ordinal);
        Assert.Contains("Fluent-only column renames", contract, StringComparison.Ordinal);
        Assert.Contains("value converters", contract, StringComparison.Ordinal);
        Assert.Contains("[NotMapped]", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_cache_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "cache-policy.md"));

        Assert.Contains("docs/cache-policy.md", readme, StringComparison.Ordinal);
        Assert.Contains("Compiled materializer store", policy, StringComparison.Ordinal);
        Assert.Contains("Evictions", policy, StringComparison.Ordinal);
        Assert.Contains("Per-Context Caches", policy, StringComparison.Ordinal);
        Assert.Contains("process-wide", policy, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Cache_policy_names_the_release_memory_bounds_gate()
    {
        var root = FindRepositoryRoot();
        var policy = File.ReadAllText(Path.Combine(root, "docs", "cache-policy.md"));
        var gate = File.ReadAllText(Path.Combine(root, "eng", "v1-release-gate.ps1"));

        Assert.Contains("Release Gate Evidence", policy, StringComparison.Ordinal);
        Assert.Contains("CacheMemoryBoundReleaseGateTests", policy, StringComparison.Ordinal);
        Assert.Contains("cache memory bounds gate", gate, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_migration_provider_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "migration-provider-contract.md"));

        Assert.Contains("docs/migration-provider-contract.md", readme, StringComparison.Ordinal);
        Assert.Contains("MigrationOptions", contract, StringComparison.Ordinal);
        Assert.Contains("SQL Server", contract, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", contract, StringComparison.Ordinal);
        Assert.Contains("MySQL", contract, StringComparison.Ordinal);
        Assert.Contains("SQLite", contract, StringComparison.Ordinal);
        Assert.Contains("Partial", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_scaffolding_preview_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "scaffolding.md"));

        Assert.Contains("docs/scaffolding.md", readme, StringComparison.Ordinal);
        Assert.Contains("preview", readme, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SQL Server", contract, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", contract, StringComparison.Ordinal);
        Assert.Contains("MySQL", contract, StringComparison.Ordinal);
        Assert.Contains("SQLite", contract, StringComparison.Ordinal);
        Assert.Contains("Relationship", contract, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_documents_design_time_migration_factory()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "design-time-migrations.md"));

        Assert.Contains("docs/design-time-migrations.md", readme, StringComparison.Ordinal);
        Assert.Contains("INormDesignTimeDbContextFactory", readme, StringComparison.Ordinal);
        Assert.Contains("INormDesignTimeDbContextFactory", contract, StringComparison.Ordinal);
        Assert.Contains("--attribute-only", contract, StringComparison.Ordinal);
        Assert.Contains("silently falls back", contract, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_links_to_transaction_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "transactions.md"));

        Assert.Contains("docs/transactions.md", readme, StringComparison.Ordinal);
        Assert.Contains("Database.BeginTransactionAsync", contract, StringComparison.Ordinal);
        Assert.Contains("AmbientTransactionPolicy", contract, StringComparison.Ordinal);
        Assert.Contains("FailFast", contract, StringComparison.Ordinal);
        Assert.Contains("BestEffort", contract, StringComparison.Ordinal);
        Assert.Contains("Ignore", contract, StringComparison.Ordinal);
        Assert.Contains("CancellationToken.None", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_sync_policy_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "sync-policy.md"));

        Assert.Contains("docs/sync-policy.md", readme, StringComparison.Ordinal);
        Assert.Contains("There is no synchronous `SaveChanges` API", contract, StringComparison.Ordinal);
        Assert.Contains("ToListSync", contract, StringComparison.Ordinal);
        Assert.Contains("CountSync", contract, StringComparison.Ordinal);
        Assert.Contains("Unsupported Sync Patterns", contract, StringComparison.Ordinal);
        Assert.Contains("not thread safe", contract, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_links_to_multi_tenancy_security_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "multi-tenancy-security.md"));

        Assert.Contains("docs/multi-tenancy-security.md", readme, StringComparison.Ordinal);
        Assert.Contains("Boundary Inventory", contract, StringComparison.Ordinal);
        Assert.Contains("Bypass-capable APIs", contract, StringComparison.Ordinal);
        Assert.Contains("Enforced Paths", contract, StringComparison.Ordinal);
        Assert.Contains("Caller-Controlled Paths", contract, StringComparison.Ordinal);
        Assert.Contains("FromSqlRawAsync", contract, StringComparison.Ordinal);
        Assert.Contains("ExecuteStoredProcedure", contract, StringComparison.Ordinal);
        Assert.Contains("BulkUpdateAsync", contract, StringComparison.Ordinal);
        Assert.Contains("row-level security", contract, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_links_to_logging_redaction_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "logging-redaction.md"));

        Assert.Contains("docs/logging-redaction.md", readme, StringComparison.Ordinal);
        Assert.Contains("Parameter values", contract, StringComparison.Ordinal);
        Assert.Contains("Connection strings", contract, StringComparison.Ordinal);
        Assert.Contains("Command interceptors", contract, StringComparison.Ordinal);
        Assert.Contains("FromSqlInterpolatedAsync", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_raw_sql_security_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "raw-sql-security.md"));
        var rawSql = File.ReadAllText(Path.Combine(root, "src", "nORM", "Core", "DbContext.RawSql.cs"));

        Assert.Contains("docs/raw-sql-security.md", readme, StringComparison.Ordinal);
        Assert.Contains("ValidateRawQuerySql", contract, StringComparison.Ordinal);
        Assert.Contains("provider-aware", contract, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT", contract, StringComparison.Ordinal);
        Assert.Contains("Privileged Escape Hatches", contract, StringComparison.Ordinal);
        Assert.Contains("ValidateRawQuerySql(sql, ctx.Provider", rawSql, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_stored_procedure_security_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "stored-procedure-security.md"));
        var tenantContract = File.ReadAllText(Path.Combine(root, "docs", "multi-tenancy-security.md"));

        Assert.Contains("docs/stored-procedure-security.md", readme, StringComparison.Ordinal);
        Assert.Contains("privileged execution paths", contract, StringComparison.Ordinal);
        Assert.Contains("TenantId", contract, StringComparison.Ordinal);
        Assert.Contains("allowlisted", contract, StringComparison.Ordinal);
        Assert.Contains("docs/stored-procedure-security.md", tenantContract, StringComparison.Ordinal);
        Assert.Contains("ExecuteStoredProcedureAsync<UserStats>", tenantContract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_interceptor_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "interceptors.md"));

        Assert.Contains("docs/interceptors.md", readme, StringComparison.Ordinal);
        Assert.Contains("registration order", contract, StringComparison.Ordinal);
        Assert.Contains("SuppressWithResult", contract, StringComparison.Ordinal);
        Assert.Contains("CommandFailed", contract, StringComparison.Ordinal);
        Assert.Contains("SavedChangesAsync", contract, StringComparison.Ordinal);
        Assert.Contains("CancellationToken.None", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_provider_capabilities()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "provider-capabilities.md"));

        Assert.Contains("docs/provider-capabilities.md", readme, StringComparison.Ordinal);
        Assert.Contains("DatabaseProvider", contract, StringComparison.Ordinal);
        Assert.Contains("Capabilities", contract, StringComparison.Ordinal);
        Assert.Contains("IsAvailableAsync", contract, StringComparison.Ordinal);
        Assert.Contains("InitializeConnectionAsync", contract, StringComparison.Ordinal);
        Assert.Contains("NormConfigurationException", contract, StringComparison.Ordinal);
        Assert.Contains("MinimumServerVersion", contract, StringComparison.Ordinal);
        Assert.Contains("SQL Server", contract, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", contract, StringComparison.Ordinal);
        Assert.Contains("MySQL", contract, StringComparison.Ordinal);
        Assert.Contains("SQLite", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_temporal_versioning_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "temporal-versioning.md"));

        Assert.Contains("docs/temporal-versioning.md", readme, StringComparison.Ordinal);
        Assert.Contains("EnableTemporalVersioning", contract, StringComparison.Ordinal);
        Assert.Contains("stable v1 feature", contract, StringComparison.Ordinal);
        Assert.Contains("nORM-managed temporal history", contract, StringComparison.Ordinal);
        Assert.Contains("__NormTemporalTags", contract, StringComparison.Ordinal);
        Assert.Contains("_History", contract, StringComparison.Ordinal);
        Assert.Contains("AsOf(DateTime)", contract, StringComparison.Ordinal);
        Assert.Contains("Release candidates must run temporal tests in the live provider gate", contract, StringComparison.Ordinal);
        Assert.Contains("Rollback is also explicit", contract, StringComparison.Ordinal);
        Assert.Contains("SQL Server", contract, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", contract, StringComparison.Ordinal);
        Assert.Contains("MySQL", contract, StringComparison.Ordinal);
        Assert.Contains("SQLite", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_bulk_operation_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "bulk-operations.md"));

        Assert.Contains("docs/bulk-operations.md", readme, StringComparison.Ordinal);
        Assert.Contains("BulkInsertAsync", contract, StringComparison.Ordinal);
        Assert.Contains("BulkUpdateAsync", contract, StringComparison.Ordinal);
        Assert.Contains("BulkDeleteAsync", contract, StringComparison.Ordinal);
        Assert.Contains("tenant", contract, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CancellationToken.None", contract, StringComparison.Ordinal);
        Assert.Contains("SupportsNativeBulkInsert", contract, StringComparison.Ordinal);
        Assert.Contains("`ExecuteUpdateAsync` Set Values", contract, StringComparison.Ordinal);
        Assert.Contains("precomputed captured local values", contract, StringComparison.Ordinal);
        Assert.Contains("inline computed values", contract, StringComparison.Ordinal);
        Assert.Contains("Server-side computed updates are a post-v1 feature", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Bulk_cud_uses_structural_query_shape_not_sql_text_parsing()
    {
        var root = FindRepositoryRoot();
        var builder = File.ReadAllText(Path.Combine(root, "src", "nORM", "Query", "BulkCudBuilder.cs"));
        var provider = File.ReadAllText(Path.Combine(root, "src", "nORM", "Query", "NormQueryProvider.cs"));

        Assert.Contains("ValidateCudPlan(BulkCudQueryShape? shape)", builder, StringComparison.Ordinal);
        Assert.Contains("GetWhereClause(BulkCudQueryShape? shape)", builder, StringComparison.Ordinal);
        Assert.DoesNotContain("ValidateCudPlan(string sql)", builder, StringComparison.Ordinal);
        Assert.DoesNotContain("ExtractWhereClause(string sql", builder, StringComparison.Ordinal);
        Assert.DoesNotContain("IndexOf(\" WHERE\"", builder, StringComparison.Ordinal);
        Assert.Contains("ValidateCudPlan(plan.BulkCudShape)", provider, StringComparison.Ordinal);
        Assert.Contains("GetWhereClause(plan.BulkCudShape)", provider, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_optimistic_concurrency_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "optimistic-concurrency.md"));

        Assert.Contains("docs/optimistic-concurrency.md", readme, StringComparison.Ordinal);
        Assert.Contains("UseAffectedRows=false", contract, StringComparison.Ordinal);
        Assert.Contains("new MySqlProvider(useAffectedRowsSemantics: false)", contract, StringComparison.Ordinal);
        Assert.Contains("RequireMatchedRowOccSemantics", contract, StringComparison.Ordinal);
        Assert.Contains("default v1 behavior", contract, StringComparison.Ordinal);
        Assert.Contains("Strict gate by default", contract, StringComparison.Ordinal);
        Assert.Contains("refuses timestamp-tracked MySQL updates by default", readme, StringComparison.Ordinal);
        Assert.Contains("same-value token", contract, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("subclass", readme, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Readme_links_to_exception_taxonomy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "exception-taxonomy.md"));

        Assert.Contains("docs/exception-taxonomy.md", readme, StringComparison.Ordinal);
        Assert.Contains("NormException", contract, StringComparison.Ordinal);
        Assert.Contains("NormUnsupportedFeatureException", contract, StringComparison.Ordinal);
        Assert.Contains("DbConcurrencyException", contract, StringComparison.Ordinal);
        Assert.Contains("OperationCanceledException", contract, StringComparison.Ordinal);
        Assert.Contains("composite-key dependent", contract, StringComparison.Ordinal);
        Assert.Contains("Include/GroupJoin", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Repository_hygiene_policy_keeps_release_scripts_visible()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var gitignore = File.ReadAllText(Path.Combine(root, ".gitignore"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "repository-hygiene.md"));
        var ci = File.ReadAllText(Path.Combine(root, ".github", "workflows", "ci.yml"));

        Assert.Contains("docs/repository-hygiene.md", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("*.ps1", gitignore.Replace("*.local.ps1", "", StringComparison.Ordinal).Replace("*.scratch.ps1", "", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("*.local.ps1", gitignore, StringComparison.Ordinal);
        Assert.Contains("*.scratch.ps1", gitignore, StringComparison.Ordinal);
        Assert.True(File.Exists(Path.Combine(root, "eng", "v1-release-gate.ps1")));
        Assert.Contains("PowerShell scripts are not globally ignored", policy, StringComparison.Ordinal);
        Assert.DoesNotContain("â", ci, StringComparison.Ordinal);
        Assert.DoesNotContain("─", ci, StringComparison.Ordinal);
    }

    [Fact]
    public void Source_generation_boundaries_are_documented_and_single_path()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "source-generation.md"));
        var runtimeAttributes = Path.Combine(root, "src", "SourceGeneration");
        var analyzerProject = Path.Combine(root, "src", "nORM.SourceGenerators", "nORM.SourceGenerators.csproj");
        var legacyPath = Path.Combine(root, "src", "nORM", "SourceGeneration");

        Assert.Contains("docs/source-generation.md", readme, StringComparison.Ordinal);
        Assert.True(Directory.Exists(runtimeAttributes));
        Assert.True(File.Exists(analyzerProject));
        Assert.False(Directory.Exists(legacyPath));
        Assert.Contains("analyzers/dotnet/cs/nORM.SourceGenerators.dll", contract, StringComparison.Ordinal);
        Assert.Contains("must not be recreated", contract, StringComparison.Ordinal);
    }

    [Fact]
    public void Benchmark_governance_defines_fair_raw_ado_categories()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "benchmark-governance.md"));
        var providerMatrix = File.ReadAllText(Path.Combine(root, "benchmarks", "ProviderMatrixBenchmarks.cs"));
        var sqliteMatrix = File.ReadAllText(Path.Combine(root, "benchmarks", "OrmBenchmarks.cs"));

        Assert.Contains("docs/benchmark-governance.md", readme, StringComparison.Ordinal);
        Assert.Contains("RawAdo_Convenience", contract, StringComparison.Ordinal);
        Assert.Contains("RawAdo_Optimized", contract, StringComparison.Ordinal);
        Assert.Contains("RawAdo_PreparedOptimized", contract, StringComparison.Ordinal);
        Assert.Contains("Dapper_Prepared", contract, StringComparison.Ordinal);
        Assert.Contains("warmupCount: 3, iterationCount: 10", providerMatrix, StringComparison.Ordinal);
        Assert.Contains("Query_Join_RawAdo_Convenience", providerMatrix, StringComparison.Ordinal);
        Assert.Contains("Query_Join_RawAdo_Optimized", providerMatrix, StringComparison.Ordinal);
        Assert.Contains("Query_Join_RawAdo_PreparedOptimized", providerMatrix, StringComparison.Ordinal);
        Assert.Contains("Query_Join_RawAdo_Convenience", sqliteMatrix, StringComparison.Ordinal);
        Assert.Contains("Query_Join_RawAdo_PreparedOptimized", sqliteMatrix, StringComparison.Ordinal);
    }

    [Fact]
    public void Readme_links_to_production_operations_runbook()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var runbook = File.ReadAllText(Path.Combine(root, "docs", "production-operations.md"));

        Assert.Contains("docs/production-operations.md", readme, StringComparison.Ordinal);
        Assert.Contains("Provider Setup", runbook, StringComparison.Ordinal);
        Assert.Contains("Connection Pooling", runbook, StringComparison.Ordinal);
        Assert.Contains("Timeouts and Retries", runbook, StringComparison.Ordinal);
        Assert.Contains("Transactions", runbook, StringComparison.Ordinal);
        Assert.Contains("Migrations", runbook, StringComparison.Ordinal);
        Assert.Contains("Multi-Tenancy", runbook, StringComparison.Ordinal);
        Assert.Contains("Raw SQL", runbook, StringComparison.Ordinal);
        Assert.Contains("Performance Tuning", runbook, StringComparison.Ordinal);
        Assert.Contains("Troubleshooting", runbook, StringComparison.Ordinal);
    }

    [Fact]
    public void Public_project_governance_files_exist()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));

        var requiredFiles = new[]
        {
            "SECURITY.md",
            "CHANGELOG.md",
            "CONTRIBUTING.md",
            "SUPPORT.md",
            Path.Combine("docs", "release-checklist.md")
        };

        foreach (var relativePath in requiredFiles)
        {
            var path = Path.Combine(root, relativePath);
            Assert.True(File.Exists(path), $"{relativePath} should exist.");
            Assert.True(new FileInfo(path).Length > 200, $"{relativePath} should not be a placeholder.");
        }

        Assert.Contains("SECURITY.md", readme, StringComparison.Ordinal);
        Assert.Contains("CHANGELOG.md", readme, StringComparison.Ordinal);
        Assert.Contains("CONTRIBUTING.md", readme, StringComparison.Ordinal);
        Assert.Contains("SUPPORT.md", readme, StringComparison.Ordinal);
        Assert.Contains("docs/release-checklist.md", readme, StringComparison.Ordinal);
    }

    [Fact]
    public void V1_issue_map_tracks_current_forty_blockers()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var map = File.ReadAllText(Path.Combine(root, "docs", "v1-issue-map.md"));

        Assert.Contains("docs/v1-issue-map.md", readme, StringComparison.Ordinal);
        for (var i = 1; i <= 40; i++)
            Assert.Contains($"| {i} |", map, StringComparison.Ordinal);

        Assert.DoesNotContain("| 41 |", map, StringComparison.Ordinal);
        Assert.Contains("exactly 40 blockers", map, StringComparison.Ordinal);
        Assert.Contains("Rebuild benchmark evidence", map, StringComparison.Ordinal);
        Assert.Contains("Closure Rule", map, StringComparison.Ordinal);
        Assert.Contains("Open", map, StringComparison.Ordinal);
        Assert.Contains("In Progress", map, StringComparison.Ordinal);
        Assert.Contains("Verified", map, StringComparison.Ordinal);
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }
}
