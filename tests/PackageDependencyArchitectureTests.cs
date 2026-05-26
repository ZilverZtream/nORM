using System;
using System.IO;
using System.Xml.Linq;
using System.Linq;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies the v1 runtime package dependency architecture contract against the
/// live project file so the contract is enforced at test time, not only when
/// <c>dotnet pack</c> is run as part of the release gate.
///
/// v1 dependency contract (mirrors docs/provider-packages.md):
///   Bundled (always transitive for consumers):
///     Microsoft.Data.SqlClient — SQL Server provider uses typed SQL Server APIs.
///     Microsoft.Data.Sqlite    — SQLite is the default local/test provider.
///   Reflection-loaded (NOT bundled; user installs separately):
///     Npgsql                   — PostgresProvider loads types by reflection.
///     MySqlConnector / MySql.Data — MySqlProvider loads types by reflection.
///
/// These tests lock the dependency graph so it cannot drift without a code
/// review that updates both the csproj and this test class.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public sealed class PackageDependencyArchitectureTests
{
    private static XDocument LoadNormCsproj()
    {
        var path = FindNormCsproj();
        return XDocument.Load(path);
    }

    [Fact]
    public void SqlClient_is_a_direct_bundled_dependency()
    {
        var deps = GetPackageReferenceIds(LoadNormCsproj());
        Assert.Contains("Microsoft.Data.SqlClient", deps, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Sqlite_is_a_direct_bundled_dependency()
    {
        var deps = GetPackageReferenceIds(LoadNormCsproj());
        Assert.Contains("Microsoft.Data.Sqlite", deps, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Npgsql_is_not_a_bundled_dependency_reflection_loaded_only()
    {
        var deps = GetPackageReferenceIds(LoadNormCsproj());
        Assert.DoesNotContain("Npgsql", deps, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySqlConnector_is_not_a_bundled_dependency_reflection_loaded_only()
    {
        var deps = GetPackageReferenceIds(LoadNormCsproj());
        Assert.DoesNotContain("MySqlConnector", deps, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySql_Data_is_not_a_bundled_dependency_reflection_loaded_only()
    {
        var deps = GetPackageReferenceIds(LoadNormCsproj());
        Assert.DoesNotContain("MySql.Data", deps, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Provider_packages_doc_exists_and_documents_v1_single_package_decision()
    {
        var root = FindRepositoryRoot();
        var docPath = Path.Combine(root, "docs", "provider-packages.md");
        Assert.True(File.Exists(docPath), $"docs/provider-packages.md not found at {docPath}.");

        var content = File.ReadAllText(docPath);

        // Doc must describe the v1 single-package decision with all four providers.
        Assert.Contains("SqlServerProvider", content, StringComparison.Ordinal);
        Assert.Contains("SqliteProvider", content, StringComparison.Ordinal);
        Assert.Contains("PostgresProvider", content, StringComparison.Ordinal);
        Assert.Contains("MySqlProvider", content, StringComparison.Ordinal);

        // Doc must call out the reflection-load strategy for Postgres and MySQL.
        Assert.Contains("reflection", content, StringComparison.OrdinalIgnoreCase);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string[] GetPackageReferenceIds(XDocument csproj)
    {
        return csproj.Descendants("PackageReference")
            .Select(static el => el.Attribute("Include")?.Value ?? string.Empty)
            .Where(static id => !string.IsNullOrEmpty(id))
            .ToArray();
    }

    private static string FindNormCsproj()
    {
        var root = FindRepositoryRoot();
        var path = Path.Combine(root, "src", "nORM.csproj");
        Assert.True(File.Exists(path), $"nORM.csproj not found at {path}.");
        return path;
    }

    private static string FindRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "nORM.sln")))
                return dir.FullName;
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }
}
