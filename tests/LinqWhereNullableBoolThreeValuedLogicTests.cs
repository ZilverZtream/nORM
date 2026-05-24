using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins SQL three-valued logic for <c>bool?</c> column predicates. C# bool?
/// can't implicitly convert to bool, so users write one of:
///   * <c>Where(i =&gt; i.IsActive == true)</c>
///   * <c>Where(i =&gt; i.IsActive == false)</c>
///   * <c>Where(i =&gt; i.IsActive != true)</c> (note: NOT same as == false in 3VL)
///   * <c>Where(i =&gt; i.IsActive ?? false)</c> (coalesce-then-test)
///   * <c>Where(i =&gt; (bool)i.IsActive)</c> (explicit cast, throws at runtime on null)
///
/// SQL 3VL specifics:
///   * <c>col = TRUE</c> matches only TRUE rows; null is UNKNOWN -> excluded.
///   * <c>col = FALSE</c> matches only FALSE rows; null is excluded.
///   * <c>col &lt;&gt; TRUE</c> matches only FALSE rows; null is excluded.
///   * <c>COALESCE(col, FALSE) = TRUE</c> matches only TRUE rows; null treated as FALSE.
///
/// Silent-wrongness shapes:
///   * Translator emits a 2VL bool comparison and null rows "fall through" -- one
///     direction returns null rows when they shouldn't, or excludes them when they
///     should appear via COALESCE.
///   * `== false` collapses to `!= true` (loses null exclusion).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNullableBoolThreeValuedLogicTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WnbItem (Id INTEGER PRIMARY KEY, IsActive INTEGER NULL);
            INSERT INTO WnbItem VALUES
                (1, 1),     -- true
                (2, 0),     -- false
                (3, NULL),  -- null
                (4, 1),     -- true
                (5, NULL);  -- null
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WnbItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_nullable_bool_equals_true_matches_only_true_rows()
    {
        // SQL 3VL: col = TRUE excludes both FALSE and NULL.
        // Expected: Id 1, 4 (the two TRUE rows).
        // Silent-wrongness: including null rows means the translator emitted
        //   `col = @p0` without honoring the C# `== true` operator's 3VL.
        var result = await _ctx.Query<WnbItem>()
            .Where(i => i.IsActive == true)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_nullable_bool_equals_false_matches_only_false_rows()
    {
        // SQL 3VL: col = FALSE excludes both TRUE and NULL.
        // Expected: Id 2 only.
        var result = await _ctx.Query<WnbItem>()
            .Where(i => i.IsActive == false)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_nullable_bool_not_equals_true_matches_only_false_rows_not_null_rows()
    {
        // SQL 3VL: col <> TRUE returns FALSE when col is TRUE, TRUE when col is FALSE,
        // UNKNOWN when col is NULL. UNKNOWN is excluded from the result set.
        // Expected: Id 2 only (the FALSE row).
        // Silent-wrongness: including the null rows would happen if the translator
        // lifted `!=` to a 2VL operator without honoring SQL NULL semantics.
        var result = await _ctx.Query<WnbItem>()
            .Where(i => i.IsActive != true)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_nullable_bool_coalesce_to_false_then_compare_includes_null_rows_as_false()
    {
        // ?? false collapses null to false, so `(IsActive ?? false) == true` matches
        // only TRUE rows (same as == true), and `(IsActive ?? false) == false`
        // matches FALSE rows AND null rows. Probe the latter -- null inclusion is
        // the key behavior.
        var result = await _ctx.Query<WnbItem>()
            .Where(i => (i.IsActive ?? false) == false)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WnbItem")]
    public sealed class WnbItem
    {
        [Key] public int Id { get; set; }
        public bool? IsActive { get; set; }
    }
}
