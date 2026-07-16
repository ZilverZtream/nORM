using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for the provider hook override matrix (provider-mobility matrix cell).
///
/// nORM's provider mobility rests on the DatabaseProvider virtual-hook system: every
/// provider-specific SQL divergence flows through a hook, and each provider's override SET is the
/// executable statement of where it diverges from the portable default. This test snapshots that
/// entire surface - the full list of virtual hooks declared by DatabaseProvider AND the declared
/// override set of each provider - against a checked-in matrix file. Adding a new virtual hook,
/// or adding/removing an override on any provider, fails this test until the matrix file is
/// deliberately regenerated and reviewed (set NORM_UPDATE_HOOK_MATRIX=1 to regenerate), exactly
/// like the public-API snapshot. The behavioural correctness of the overrides themselves is pinned
/// by the per-cell matrices (NH-01xx/02xx/03xx/04xx contract tests); this pins the SHAPE.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProviderHookOverrideMatrixTests
{
    private static string[] BaseHooks()
        => typeof(DatabaseProvider)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(m => m.IsVirtual && !m.IsFinal && !m.IsSpecialName)
            .Select(m => m.Name)
            .Concat(typeof(DatabaseProvider)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
                .Where(p => (p.GetMethod?.IsVirtual ?? false) && !(p.GetMethod?.IsFinal ?? true))
                .Select(p => p.Name))
            .Distinct().OrderBy(x => x, StringComparer.Ordinal).ToArray();

    private static string[] Overrides(Type provider)
        => provider
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(m => m.IsVirtual && !m.IsSpecialName && m.GetBaseDefinition().DeclaringType == typeof(DatabaseProvider))
            .Select(m => m.Name)
            .Concat(provider
                .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
                .Where(p => (p.GetMethod?.IsVirtual ?? false) && p.GetMethod!.GetBaseDefinition().DeclaringType == typeof(DatabaseProvider))
                .Select(p => p.Name))
            .Distinct().OrderBy(x => x, StringComparer.Ordinal).ToArray();

    private static string BuildMatrix()
    {
        var sb = new StringBuilder();
        void Section(string name, string[] hooks)
        {
            sb.Append('[').Append(name).AppendLine("]");
            foreach (var h in hooks) sb.AppendLine(h);
            sb.AppendLine();
        }
        Section("DatabaseProvider", BaseHooks());
        Section("SqliteProvider", Overrides(typeof(SqliteProvider)));
        Section("SqlServerProvider", Overrides(typeof(SqlServerProvider)));
        Section("PostgresProvider", Overrides(typeof(PostgresProvider)));
        Section("MySqlProvider", Overrides(typeof(MySqlProvider)));
        return sb.ToString().Replace("\r\n", "\n");
    }

    private static string MatrixFilePath()
    {
        // Walk up from the test assembly to the repo's tests directory (same technique as the
        // public-API snapshot: the file lives beside the test sources, not in bin/).
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "ProviderHookMatrix.Shipped.txt")))
            dir = Path.GetDirectoryName(dir);
        if (dir == null)
            throw new InvalidOperationException(
                "ProviderHookMatrix.Shipped.txt not found in any parent of the test output directory. " +
                "Run once with NORM_UPDATE_HOOK_MATRIX=1 from the repo to create it in tests/.");
        return Path.Combine(dir, "ProviderHookMatrix.Shipped.txt");
    }

    [Fact]
    public void Provider_override_sets_match_the_shipped_hook_matrix()
    {
        var current = BuildMatrix();

        if (Environment.GetEnvironmentVariable("NORM_UPDATE_HOOK_MATRIX") == "1")
        {
            // Regeneration mode: write beside the test sources (tests/ is 3 levels above bin output).
            var testsDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
            File.WriteAllText(Path.Combine(testsDir, "ProviderHookMatrix.Shipped.txt"), current);
            return;
        }

        var shipped = File.ReadAllText(MatrixFilePath()).Replace("\r\n", "\n");
        if (!string.Equals(shipped, current, StringComparison.Ordinal))
        {
            // Produce an actionable diff instead of a wall of text.
            var shippedLines = shipped.Split('\n');
            var currentLines = current.Split('\n');
            var added = currentLines.Except(shippedLines).ToArray();
            var removed = shippedLines.Except(currentLines).ToArray();
            Assert.Fail(
                "The DatabaseProvider hook surface or a provider's override set changed. If deliberate, " +
                "review the divergence, extend the relevant matrix contract tests, and regenerate with " +
                "NORM_UPDATE_HOOK_MATRIX=1.\n" +
                $"Added lines ({added.Length}): {string.Join(", ", added.Take(20))}\n" +
                $"Removed lines ({removed.Length}): {string.Join(", ", removed.Take(20))}");
        }
    }
}
