using System;
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
/// Probes <c>string.IsNullOrEmpty(col)</c> and <c>string.IsNullOrWhiteSpace(col)</c>
/// as Where predicates. These are extremely common everyday idioms.
///
/// LINQ-to-SQL convention (EF Core, Linq2Db):
///   * <c>IsNullOrEmpty(col)</c> -> <c>(col IS NULL OR col = '')</c>
///   * <c>IsNullOrWhiteSpace(col)</c> -> <c>(col IS NULL OR TRIM(col) = '')</c>
///
/// Silent-wrongness shapes if mis-translated:
///   * dropped IS NULL branch: NULL rows excluded (3VL stripped the condition)
///   * dropped empty-string branch: only-empty rows excluded
///   * fall-through to client-eval: full table scan
///
/// Pins both shapes. Asserts either correct translation OR a nORM-typed
/// error pointing at the manual disjunction workaround.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringIsNullOrEmptyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsneItem (Id INTEGER PRIMARY KEY, Name TEXT NULL);
            INSERT INTO WsneItem VALUES
                (1, 'alpha'),
                (2, NULL),
                (3, ''),
                (4, '   '),
                (5, 'beta');
            -- Rows: 1=alpha, 2=NULL, 3=empty, 4=whitespace, 5=beta
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WsneItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_IsNullOrEmpty_matches_null_rows_and_empty_string_rows()
    {
        // Strict: must translate to (col IS NULL OR col = '') and return Ids 2 & 3.
        // Silent-wrongness probes:
        //   * matched={2} -> dropped empty-string branch
        //   * matched={3} -> dropped IS NULL branch
        //   * matched={1,2,3,4,5} -> predicate dropped entirely
        //   * matched={2,3,4} -> conflated with IsNullOrWhiteSpace
        var result = await _ctx.Query<WsneItem>()
            .Where(i => string.IsNullOrEmpty(i.Name))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_negated_IsNullOrEmpty_excludes_null_and_empty()
    {
        // Strict: !IsNullOrEmpty must exclude both NULL rows and empty-string
        // rows. The silent-wrongness shape to probe: NULL rows leak through
        // when the translator inverts only the equality branch and leaves
        // `NOT (col IS NULL)` evaluating to UNKNOWN for NULL (treated as
        // false correctly in SQL, but a buggy emit could swap to NOT NULL
        // which is also false for NULL -- so the failure mode is more subtle
        // here than for the positive case).
        var result = await _ctx.Query<WsneItem>()
            .Where(i => !string.IsNullOrEmpty(i.Name))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_IsNullOrWhiteSpace_matches_null_empty_and_whitespace_rows()
    {
        // Strict: must translate to (col IS NULL OR TRIM(col) = '') and return
        // Ids 2, 3, 4. Common implementation pitfall: leaving out TRIM and
        // collapsing to IsNullOrEmpty semantics (would return {2,3}).
        var result = await _ctx.Query<WsneItem>()
            .Where(i => string.IsNullOrWhiteSpace(i.Name))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 4 }, result.Select(r => r.Id).ToArray());
    }

    // ── SQL Server shape: DATALENGTH instead of = '' ─────────────────────────
    // SQL Server ignores trailing spaces in equality comparisons, so
    // '   ' = '' is TRUE. The fix uses DATALENGTH(col) = 0 which counts raw
    // bytes and treats '   ' as non-empty.

    [Fact]
    public void IsNullOrEmpty_SqlServer_generates_DATALENGTH_not_equality()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var q = ctx.Query<WsneItem>().Where(i => string.IsNullOrEmpty(i.Name));
        var plan = ctx.GetQueryProvider().GetPlan(q.Expression, out _, out _);

        Assert.NotNull(plan.Sql);
        // Must use DATALENGTH so '   ' is NOT matched.
        Assert.Contains("DATALENGTH", plan.Sql, StringComparison.OrdinalIgnoreCase);
        // Must NOT use the naive equality that SQL Server elides trailing spaces for.
        Assert.DoesNotContain("= ''", plan.Sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IsNullOrEmpty_SqlServer_negated_generates_DATALENGTH()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var q = ctx.Query<WsneItem>().Where(i => !string.IsNullOrEmpty(i.Name));
        var plan = ctx.GetQueryProvider().GetPlan(q.Expression, out _, out _);

        Assert.NotNull(plan.Sql);
        Assert.Contains("DATALENGTH", plan.Sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IsNullOrWhiteSpace_SqlServer_uses_LTRIM_RTRIM_not_DATALENGTH()
    {
        // IsNullOrWhiteSpace intentionally does NOT use DATALENGTH: it trims
        // first, so '   ' becomes '' before the comparison, which is correct.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqlServerProvider());

        var q = ctx.Query<WsneItem>().Where(i => string.IsNullOrWhiteSpace(i.Name));
        var plan = ctx.GetQueryProvider().GetPlan(q.Expression, out _, out _);

        Assert.NotNull(plan.Sql);
        Assert.Contains("LTRIM", plan.Sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RTRIM", plan.Sql, StringComparison.OrdinalIgnoreCase);
    }

    [Table("WsneItem")]
    public sealed class WsneItem
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
    }
}
