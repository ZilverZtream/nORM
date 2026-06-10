using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace nORM.Tests;

/// <summary>
/// Every public test class in the nORM.Tests assembly must declare an explicit
/// <c>[Trait("Category", ...)]</c>. The category-driven release gate (Fast / LiveProvider /
/// Stress / ProviderParity / RC routing categories / PackageConsumer) cannot route work to the right step if classes opt out, so the
/// suite refuses to grow uncategorized classes.
///
/// To auto-categorize a freshly added test file, run
/// <c>eng/scripts/add-test-categories.ps1</c>. The script picks Fast by default and promotes
/// by filename pattern (Stress / LiveProvider / PackageConsumer).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TestCategoryHygieneTests
{
    [Fact]
    public void Every_public_test_class_declares_a_category_trait()
    {
        var assembly = typeof(TestCategoryHygieneTests).Assembly;
        var validCategories = new HashSet<string>(StringComparer.Ordinal)
        {
            TestCategory.Fast,
            TestCategory.LiveProvider,
            TestCategory.Stress,
            TestCategory.ProviderParity,
            TestCategory.NavigationStress,
            TestCategory.TransactionStress,
            TestCategory.CompiledQueryStress,
            TestCategory.ProviderSourceGenParity,
            TestCategory.BulkProviderParity,
            TestCategory.MigrationParity,
            TestCategory.CacheMemory,
            TestCategory.AdversarialConcurrency,
            TestCategory.PackageConsumer,
        };

        var uncategorized = new List<string>();
        var invalidCategory = new List<string>();

        foreach (var type in assembly.GetTypes())
        {
            if (!type.IsPublic && !type.IsNestedPublic) continue;
            if (type.IsAbstract && type.IsSealed) continue; // static class
            if (type.IsInterface || type.IsEnum) continue;

            // Only require categories on classes that declare xUnit test methods (Fact/Theory).
            var hasTestMethod = type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Any(m => m.GetCustomAttributes(typeof(FactAttribute), inherit: true).Any()
                       || m.GetCustomAttributes(typeof(TheoryAttribute), inherit: true).Any());
            if (!hasTestMethod) continue;

            // TraitAttribute on a class is just metadata; its constructor args are not exposed as
            // properties on the runtime instance. Read them via CustomAttributeData instead.
            var traitData = type.GetCustomAttributesData()
                .Where(d => d.AttributeType == typeof(TraitAttribute) && d.ConstructorArguments.Count == 2)
                .Select(d => new
                {
                    Name = d.ConstructorArguments[0].Value as string,
                    Value = d.ConstructorArguments[1].Value as string
                })
                .Where(t => string.Equals(t.Name, "Category", StringComparison.Ordinal))
                .ToList();

            if (traitData.Count == 0)
            {
                uncategorized.Add(type.FullName ?? type.Name);
                continue;
            }

            foreach (var trait in traitData)
            {
                if (trait.Value is null || !validCategories.Contains(trait.Value))
                {
                    invalidCategory.Add($"{type.FullName}: Category={trait.Value ?? "<null>"}");
                }
            }
        }

        Assert.True(
            uncategorized.Count == 0,
            "The following test classes are missing a [Trait(\"Category\", ...)] attribute. " +
            "Run eng/scripts/add-test-categories.ps1 to add them, or add manually:\n" +
            string.Join("\n", uncategorized));

        Assert.True(
            invalidCategory.Count == 0,
            "The following test classes use a category name outside the v1 set " +
            "(Fast / LiveProvider / Stress / ProviderParity / NavigationStress / TransactionStress / " +
            "CompiledQueryStress / ProviderSourceGenParity / BulkProviderParity / MigrationParity / " +
            "CacheMemory / AdversarialConcurrency / PackageConsumer):\n" +
            string.Join("\n", invalidCategory));
    }

    [Fact]
    public void Runtime_live_provider_skip_policy_uses_early_return_not_runtime_exceptions()
    {
        var forbidden = "Skip" + "Exception";
        var root = FindRepositoryRoot();
        var offenders = Directory.EnumerateFiles(Path.Combine(root, "tests"), "*.cs", SearchOption.AllDirectories)
            .Concat(Directory.EnumerateFiles(Path.Combine(root, "src"), "*.cs", SearchOption.AllDirectories))
            .Where(path => !path.EndsWith(nameof(TestCategoryHygieneTests) + ".cs", StringComparison.Ordinal))
            .Where(path => File.ReadAllText(path).Contains(forbidden, StringComparison.Ordinal))
            .Select(path => Path.GetRelativePath(root, path))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            offenders.Count == 0,
            "Runtime skip exceptions are not part of the no-provider local test contract. " +
            "Use the shared Skip.If early-return helper in live-provider tests and keep " +
            "live/RC provider minimums enforced by eng/v1-release-gate.ps1:\n" +
            string.Join("\n", offenders));
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }
}
