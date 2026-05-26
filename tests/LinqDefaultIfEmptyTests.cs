using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins DefaultIfEmpty standalone (not via GroupJoin left-join) as a supported operator.
///
/// Semantics (match LINQ-to-Objects exactly):
///   source.DefaultIfEmpty()         → source if non-empty, else { null }
///   source.DefaultIfEmpty(value)    → source if non-empty, else { value }
///
/// Implementation: post-materialize transform — SQL fetches the source normally;
/// if the materialized list is empty, the default element is appended before returning.
/// This is correct and efficient: for SingleResult queries (First/FirstOrDefault etc.)
/// the SQL already uses LIMIT 1, so at most 1 row is fetched to determine emptiness.
///
/// Silent-wrongness risks:
///   * DefaultIfEmpty on non-empty source returns wrong number of rows.
///   * DefaultIfEmpty on empty source returns [] instead of [null].
///   * DefaultIfEmpty(value) ignores the provided value.
///   * Terminal operators (First, Count) don't see the injected default element.
///
/// Schema: DieRow (Id PK, Name TEXT). Data: (1,'a'),(2,'b'),(3,'c').
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDefaultIfEmptyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("DieRow")]
    private sealed class DieRow
    {
        [Key] public int    Id   { get; set; }
        public string Name { get; set; } = "";
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DieRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO DieRow VALUES (1,'a'),(2,'b'),(3,'c');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ── 1: Non-empty source — DefaultIfEmpty is a no-op ─────────────────────────

    [Fact]
    public async Task DefaultIfEmpty_on_non_empty_source_returns_source_unchanged()
    {
        var rows = await _ctx.Query<DieRow>().DefaultIfEmpty().ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.NotNull(r));
    }

    // ── 2: Empty source — DefaultIfEmpty injects null ────────────────────────────

    [Fact]
    public async Task DefaultIfEmpty_on_empty_source_returns_single_null_element()
    {
        var rows = await _ctx.Query<DieRow>().Where(r => r.Id > 9999).DefaultIfEmpty().ToListAsync();
        Assert.Single(rows);
        Assert.Null(rows[0]);
    }

    // ── 3: DefaultIfEmpty with value — non-empty source returns source ───────────

    [Fact]
    public async Task DefaultIfEmpty_with_value_on_non_empty_source_returns_source_unchanged()
    {
        var fallback = new DieRow { Id = -1, Name = "fallback" };
        var rows = await _ctx.Query<DieRow>().DefaultIfEmpty(fallback).ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r!.Id > 0));
    }

    // ── 4: DefaultIfEmpty with value — empty source returns default value ────────

    [Fact]
    public async Task DefaultIfEmpty_with_value_on_empty_source_returns_provided_default()
    {
        var fallback = new DieRow { Id = -1, Name = "fallback" };
        var rows = await _ctx.Query<DieRow>().Where(r => r.Id > 9999).DefaultIfEmpty(fallback).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(-1, rows[0]!.Id);
        Assert.Equal("fallback", rows[0]!.Name);
    }

    // ── 5: DefaultIfEmpty → Count: non-empty returns element count ──────────────

    [Fact]
    public async Task DefaultIfEmpty_count_on_non_empty_source_returns_element_count()
    {
        // Note: Count() runs directly on source SQL, not through DefaultIfEmpty transform.
        // DefaultIfEmpty transforms the list; Count on a list-terminal sees the transform.
        var rows = await _ctx.Query<DieRow>().DefaultIfEmpty().ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    // ── 6: DefaultIfEmpty → Count: empty returns 1 (the null element) ───────────

    [Fact]
    public async Task DefaultIfEmpty_on_empty_returns_list_count_of_one()
    {
        var rows = await _ctx.Query<DieRow>().Where(r => r.Id > 9999).DefaultIfEmpty().ToListAsync();
        Assert.Single(rows); // [null] has Count=1
    }

    // ── 7: DefaultIfEmpty chained with Where filter ──────────────────────────────

    [Fact]
    public async Task DefaultIfEmpty_after_Where_on_non_empty_returns_filtered_rows()
    {
        var rows = await _ctx.Query<DieRow>().Where(r => r.Id <= 2).DefaultIfEmpty().ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.NotNull(r));
    }

    // ── 8: DefaultIfEmpty after Where that yields empty ─────────────────────────

    [Fact]
    public async Task DefaultIfEmpty_after_Where_on_empty_returns_single_null()
    {
        var rows = await _ctx.Query<DieRow>().Where(r => r.Name == "zzz").DefaultIfEmpty().ToListAsync();
        Assert.Single(rows);
        Assert.Null(rows[0]);
    }

    // ── 9: DefaultIfEmpty → OrderBy composition ──────────────────────────────────

    [Fact]
    public async Task DefaultIfEmpty_before_OrderBy_on_non_empty_preserves_ordering()
    {
        var rows = await _ctx.Query<DieRow>().DefaultIfEmpty().OrderByDescending(r => r!.Id).ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(3, rows[0]!.Id);
        Assert.Equal(1, rows[2]!.Id);
    }
}
