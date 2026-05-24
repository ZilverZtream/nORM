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
/// Exercises two common LINQ shapes that need explicit coverage:
///  - Take(n).Skip(m) and Skip(m).Take(n) over an OrderBy'd source — both should produce the
///    same window when ordering is stable, but the order they appear in the call chain controls
///    which OFFSET / LIMIT semantics the provider emits.
///  - Nullable boolean short-circuiting: `Where(r => r.Flag == true)` must include only rows
///    where Flag is non-null AND equal to true; null Flag rows must NOT match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqPagingAndNullableBoolTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PgRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Flag INTEGER NULL);
            INSERT INTO PgRow VALUES
                (1, 'alpha',   1),
                (2, 'bravo',   0),
                (3, 'charlie', NULL),
                (4, 'delta',   1),
                (5, 'echo',    0),
                (6, 'foxtrot', NULL),
                (7, 'golf',    1);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Skip_then_Take_returns_window_after_offset()
    {
        var ids = (await _ctx.Query<PgRow>().OrderBy(r => r.Id).Skip(2).Take(3).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 4, 5 }, ids);
    }

    [Fact]
    public async Task Take_then_Skip_returns_inner_window_via_algebraic_rewrite()
    {
        // Take(5).Skip(2) ≡ Skip(2).Take(3) (rows [2, 5)) — both yield rows 3, 4, 5.
        var ids = (await _ctx.Query<PgRow>().OrderBy(r => r.Id).Take(5).Skip(2).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 4, 5 }, ids);
    }

    [Fact]
    public async Task Take_then_Skip_with_skip_at_or_beyond_take_returns_empty()
    {
        // Take(3).Skip(3): the Take window ends exactly where Skip begins → 0 rows.
        var ids = (await _ctx.Query<PgRow>().OrderBy(r => r.Id).Take(3).Skip(3).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Empty(ids);
    }

    [Fact]
    public async Task Nullable_bool_equality_true_excludes_nulls_and_false()
    {
        var ids = (await _ctx.Query<PgRow>().Where(r => r.Flag == true).OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 4, 7 }, ids);
    }

    [Fact]
    public async Task Nullable_bool_equality_false_excludes_nulls_and_true()
    {
        var ids = (await _ctx.Query<PgRow>().Where(r => r.Flag == false).OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 5 }, ids);
    }

    [Fact]
    public async Task Nullable_bool_HasValue_filter_excludes_only_null_rows()
    {
        var ids = (await _ctx.Query<PgRow>().Where(r => r.Flag.HasValue).OrderBy(r => r.Id).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 4, 5, 7 }, ids);
    }

    [Table("PgRow")]
    public sealed class PgRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool? Flag { get; set; }
    }
}
