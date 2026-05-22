using System;
using System.IO;
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
    public void Readme_links_to_aot_and_trimming_policy()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var policy = File.ReadAllText(Path.Combine(root, "docs", "aot-trimming.md"));

        Assert.Contains("docs/aot-trimming.md", readme, StringComparison.Ordinal);
        Assert.Contains("NativeAOT", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresDynamicCode", policy, StringComparison.Ordinal);
        Assert.Contains("RequiresUnreferencedCode", policy, StringComparison.Ordinal);
        Assert.Contains("not supported", policy, StringComparison.OrdinalIgnoreCase);
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
    public void Readme_links_to_multi_tenancy_security_contract()
    {
        var root = FindRepositoryRoot();
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var contract = File.ReadAllText(Path.Combine(root, "docs", "multi-tenancy-security.md"));

        Assert.Contains("docs/multi-tenancy-security.md", readme, StringComparison.Ordinal);
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
