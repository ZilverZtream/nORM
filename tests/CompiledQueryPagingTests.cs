using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

namespace nORM.Tests;

/// <summary>
/// COMPILED-query (Norm.CompileQuery) Skip/Take composition + computed paging args. Compiled queries bind
/// paging counts as genuine SQL parameters (_skipParam/_takeParam) instead of constant-folding to literals, so
/// these paths are not reachable from ordinary queries. Each result is checked against a LINQ-to-Objects oracle.
/// Covers: parameterized-first-Skip composition, a computed count (pageIndex*PageSize) as LIMIT/OFFSET (the
/// canonical compiled paging idiom), and Take(param)/Skip composites — none of which may silently drop the
/// LIMIT/OFFSET or discard an earlier offset.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompiledQueryPagingTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    private SqliteConnection _cn = null!;
    private CqpContext _ctx = null!;
    private List<CqpRow> _oracle = null!;

    public CompiledQueryPagingTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CqpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "INSERT INTO CqpRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j'),(11,'k'),(12,'l');";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new CqpContext(_cn);
        _oracle = Enumerable.Range(1, 12).Select(i => new CqpRow { Id = i, Name = ((char)('a' + i - 1)).ToString() }).ToList();
    }

    public async Task DisposeAsync() { _ctx.Dispose(); await _cn.DisposeAsync(); }

    // ===================== SILENT-WRONG =====================

    // Skip(param).Take(lit).Skip(lit): the parameterized FIRST offset is discarded.
    // OrderBy(Id).Skip(2).Take(6).Skip(2) should be OFFSET 4 LIMIT 4 => [5,6,7,8];
    // nORM emits OFFSET 2 LIMIT 4 => [3,4,5,6]. Two wrong rows returned, two dropped.
    [Fact]
    public async Task SILENT_Skip_param_Take_lit_Skip_lit()
    {
        var q = Norm.CompileQuery((CqpContext c, int a) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Skip(a).Take(6).Skip(2));
        var norm = (await q(_ctx, 2)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2).Select(r => r.Id).ToArray();
        _out.WriteLine($"Skip(param 2).Take(6).Skip(2)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // Take(a*3): a computed paging arg binds neither as a bare parameter nor a literal,
    // so TakeTranslator silently sets no limit and ALL rows are returned.
    [Fact]
    public async Task SILENT_Take_computed_expression_drops_limit()
    {
        var q = Norm.CompileQuery((CqpContext c, int a) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Take(a * 3));
        var norm = (await q(_ctx, 2)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Take(6).Select(r => r.Id).ToArray();
        _out.WriteLine($"Take(a*3) a=2  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // Skip(a*3): same class on the Skip side — offset silently dropped, nothing skipped.
    [Fact]
    public async Task SILENT_Skip_computed_expression_drops_offset()
    {
        var q = Norm.CompileQuery((CqpContext c, int a) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Skip(a * 3).Take(4));
        var norm = (await q(_ctx, 2)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Skip(6).Take(4).Select(r => r.Id).ToArray();
        _out.WriteLine($"Skip(a*3).Take(4) a=2  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // The canonical pagination idiom in a compiled query: Skip(pageIndex * PageSize).
    // pageIndex is the compiled parameter, PageSize a constant. pageIndex*PageSize is a
    // computed expression -> OFFSET silently dropped -> EVERY page returns page 0.
    [Fact]
    public async Task SILENT_realistic_compiled_paging_skip_pageIndex_times_size()
    {
        const int PageSize = 4;
        var page = Norm.CompileQuery((CqpContext c, int pageIndex) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Skip(pageIndex * PageSize).Take(PageSize));
        var page2 = (await page(_ctx, 2)).Select(r => r.Id).ToArray();     // should be rows [9,10,11,12]
        var oracle = _oracle.OrderBy(r => r.Id).Skip(2 * PageSize).Take(PageSize).Select(r => r.Id).ToArray();
        _out.WriteLine($"Skip(pageIndex*4).Take(4) pageIndex=2  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", page2)}]");
        Assert.Equal(oracle, page2);
    }

    // ===================== FAIL-LOUD (still bugs, but not silent) =====================

    // Skip(lit).Take(param).Skip(lit): the composite offset must combine both skips (2+2) and the LIMIT
    // becomes take-skip; earlier this threw (clamp param name) / dropped the first offset.
    [Fact]
    public async Task Skip_lit_Take_param_Skip_lit()
    {
        var q = Norm.CompileQuery((CqpContext c, int n) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Skip(2).Take(n).Skip(2));
        var norm = (await q(_ctx, 6)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2).Select(r => r.Id).ToArray();
        _out.WriteLine($"Skip(2).Take(param 6).Skip(2)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // Canonical Take(param).Skip(lit) compiled shape: OFFSET 2, LIMIT take-2.
    [Fact]
    public async Task Take_param_Skip_lit_canonical()
    {
        var q = Norm.CompileQuery((CqpContext c, int n) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Take(n).Skip(2));
        var norm = (await q(_ctx, 5)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Take(5).Skip(2).Select(r => r.Id).ToArray();
        _out.WriteLine($"Take(param 5).Skip(2)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    // ===================== CONTROLS (known good) =====================

    // Canonical compiled paging Skip(param).Take(param) — Skip-then-Take, no composite.
    [Fact]
    public async Task CONTROL_Skip_param_Take_param()
    {
        var q = Norm.CompileQuery((CqpContext c, int a) =>
            c.Query<CqpRow>().OrderBy(r => r.Id).Skip(a).Take(4));
        var norm = (await q(_ctx, 3)).Select(r => r.Id).ToArray();
        var oracle = _oracle.OrderBy(r => r.Id).Skip(3).Take(4).Select(r => r.Id).ToArray();
        _out.WriteLine($"Skip(param 3).Take(4)  oracle=[{string.Join(",", oracle)}] norm=[{string.Join(",", norm)}]");
        Assert.Equal(oracle, norm);
    }

    [Table("CqpRow")]
    public sealed class CqpRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CqpContext : DbContext
    {
        public CqpContext(SqliteConnection cn) : base(cn, new SqliteProvider()) { }
    }
}
