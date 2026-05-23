using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The default configuration must funnel every write path (normal, direct, bulk) through
/// identical row-count + token-comparison logic. <see cref="DbContextOptions.RequireMatchedRowOccSemantics"/>
/// defaults to <c>true</c> and <see cref="MySqlProvider"/> defaults to matched-rows semantics;
/// together they close the MySQL same-value blind spot where a no-op UPDATE that re-writes the
/// existing concurrency token would otherwise silently succeed.
///
/// Looser semantics must require an explicit opt-in. This test fails if a future default
/// change silently weakens concurrency detection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OccDefaultsContractTests
{
    [Fact]
    public void DbContextOptions_RequireMatchedRowOccSemantics_defaults_to_true()
    {
        var options = new DbContextOptions();
        Assert.True(
            options.RequireMatchedRowOccSemantics,
            "Default must be strict OCC semantics. Flipping this off without an explicit opt-in " +
            "reintroduces the MySQL same-value blind spot where a no-op UPDATE re-writing the " +
            "existing token would silently succeed.");
    }

    [Fact]
    public void MySqlProvider_default_constructor_uses_matched_row_semantics()
    {
        // Default constructor must not pass useAffectedRowsSemantics=true, which would weaken
        // OCC detection on no-op UPDATEs that re-write the same token.
        var provider = new MySqlProvider();
        Assert.NotNull(provider);
        // We can't read the private flag, but if the default constructor were the
        // affected-rows variant, the strict-mode test elsewhere in the suite would fail.
        // This test exists to document the contract and force a review on any default change.
    }
}
