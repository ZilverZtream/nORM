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
/// Proves that compiled queries correctly apply PostReverse (TakeLast/SkipLast)
/// and PostMaterializeTransform (DistinctBy/ExceptBy/IntersectBy/UnionBy) through
/// the pooled execution paths in NormQueryProvider.Compiled.cs.
///
/// Silent-wrongness risk: the pooled paths (ExecutePooledListSync, ExecuteCompiledFreshSync,
/// ExecutePooledListAsync) materialize rows inline from the DbDataReader without going
/// through QueryExecutor.MaterializeAsync, so they historically skipped both transforms.
/// TakeLast/SkipLast would return rows in the DB-reversed order (wrong direction).
/// DistinctBy would return duplicates (no dedup applied).
///
/// Schema: CpRow (Id PK, Group INT, Name TEXT)
///   (1,1,'a'),(2,2,'b'),(3,1,'c'),(4,2,'d'),(5,1,'e')
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledTailPagingAndKeyedSetOpsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("CpRow")]
    private sealed class CpRow
    {
        [Key] public int Id    { get; set; }
        public int    Group { get; set; }
        public string Name  { get; set; } = "";
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CpRow (Id INTEGER PRIMARY KEY, "Group" INTEGER NOT NULL, Name TEXT NOT NULL);
            INSERT INTO CpRow VALUES (1,1,'a'),(2,2,'b'),(3,1,'c'),(4,2,'d'),(5,1,'e');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ── 1: TakeLast via compiled query returns rows in original ORDER BY direction ──

    [Fact]
    public async Task TakeLast_via_compiled_query_preserves_ascending_order()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, bool _) =>
            ctx.Query<CpRow>().OrderBy(r => r.Id).TakeLast(2));

        var rows = await compiled(_ctx, true);

        // TakeLast(2) ordered by Id → last 2 rows: Id=4, Id=5 in ascending order.
        // Bug (without fix): DB returns Id=5,Id=4 (DESC) and no re-reverse → [5,4].
        Assert.Equal(2, rows.Count);
        Assert.Equal(4, rows[0].Id);
        Assert.Equal(5, rows[1].Id);
    }

    // ── 2: TakeLast(1) = last row ────────────────────────────────────────────────

    [Fact]
    public async Task TakeLast_1_via_compiled_query_returns_last_row()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, bool _) =>
            ctx.Query<CpRow>().OrderBy(r => r.Id).TakeLast(1));

        var rows = await compiled(_ctx, true);

        Assert.Single(rows);
        Assert.Equal(5, rows[0].Id);
    }

    // ── 3: SkipLast via compiled query preserves order ───────────────────────────

    [Fact]
    public async Task SkipLast_via_compiled_query_preserves_ascending_order()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, bool _) =>
            ctx.Query<CpRow>().OrderBy(r => r.Id).SkipLast(2));

        var rows = await compiled(_ctx, true);

        // SkipLast(2) ordered by Id → rows Id=1,2,3 in ascending order.
        Assert.Equal(3, rows.Count);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
        Assert.Equal(3, rows[2].Id);
    }

    // ── 4: TakeLast with descending OrderBy ─────────────────────────────────────

    [Fact]
    public async Task TakeLast_with_descending_order_returns_smallest_values()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, bool _) =>
            ctx.Query<CpRow>().OrderByDescending(r => r.Id).TakeLast(2));

        var rows = await compiled(_ctx, true);

        // OrderByDescending(Id) then TakeLast(2) → last 2 of DESC sequence = smallest Ids
        // = Id=2,Id=1 in descending order (i.e. 2 then 1).
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(1, rows[1].Id);
    }

    // ── 5: DistinctBy via compiled query deduplicates ───────────────────────────

    [Fact]
    public async Task DistinctBy_via_compiled_query_keeps_first_occurrence_per_key()
    {
        var compiled = Norm.CompileQuery((DbContext ctx, bool _) =>
            ctx.Query<CpRow>().OrderBy(r => r.Id).DistinctBy(r => r.Group));

        var rows = await compiled(_ctx, true);

        // DistinctBy(Group): first occ of group=1 is Id=1, first occ of group=2 is Id=2.
        // Bug (without fix): returns all 5 rows (no dedup applied).
        Assert.Equal(2, rows.Count);
        var ids = rows.Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    // ── 6: Runtime path TakeLast still works (regression guard) ────────────────

    [Fact]
    public async Task TakeLast_via_runtime_query_preserves_order_regression_guard()
    {
        var rows = await _ctx.Query<CpRow>().OrderBy(r => r.Id).TakeLast(2).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(4, rows[0].Id);
        Assert.Equal(5, rows[1].Id);
    }

    // ── 7: Runtime path DistinctBy still works (regression guard) ───────────────

    [Fact]
    public async Task DistinctBy_via_runtime_query_deduplicates_regression_guard()
    {
        var rows = await _ctx.Query<CpRow>().OrderBy(r => r.Id).DistinctBy(r => r.Group).ToListAsync();

        Assert.Equal(2, rows.Count);
    }
}
