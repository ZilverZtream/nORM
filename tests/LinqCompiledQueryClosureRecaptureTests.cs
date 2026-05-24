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
/// Pins behaviour of a compiled query that captures an OUTER local variable
/// (not via TParam). EF Core bakes the captured value at compile-time —
/// later mutations don't affect subsequent invocations. nORM's compiled
/// query re-evaluates the closure reference per invocation, so the latest
/// mutated value flows through. This is more flexible than EF Core's
/// behaviour but means callers who EXPECT compile-time baking would be
/// surprised. Both surfaces are valid; pin nORM's contract here so a future
/// refactor toward EF Core's behaviour is caught.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledQueryClosureRecaptureTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private CqcContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CqcRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO CqcRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new CqcContext(_cn);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Compiled_query_with_outer_closure_capture_reflects_latest_value_each_invocation()
    {
        int minId = 1;
        var compiled = Norm.CompileQuery((CqcContext c, int unused) =>
            c.Query<CqcRow>().Where(r => r.Id >= minId).OrderBy(r => r.Id));

        // First invocation: minId is 1 → all 5 rows.
        var first = await compiled(_ctx, 0);
        Assert.Equal(5, first.Count);

        // Mutate the captured local.
        minId = 3;

        // nORM re-evaluates the closure reference each invocation, so the second call
        // picks up minId=3 (3 rows: Ids 3, 4, 5). EF Core would still return 5 here
        // because it bakes the compile-time value.
        var second = await compiled(_ctx, 0);
        Assert.Equal(3, second.Count);
        Assert.Equal(3, second[0].Id);

        // Reset to confirm it's not a one-shot recapture — the closure is consulted
        // fresh on every call.
        minId = 5;
        var third = await compiled(_ctx, 0);
        Assert.Single(third);
        Assert.Equal(5, third[0].Id);
    }

    [Fact]
    public async Task Compiled_query_with_tparam_picks_up_per_invocation_value()
    {
        // Use TParam (the lambda's second parameter) — the value-supplying contract.
        var compiled = Norm.CompileQuery((CqcContext c, int minId) =>
            c.Query<CqcRow>().Where(r => r.Id >= minId).OrderBy(r => r.Id));

        Assert.Equal(5, (await compiled(_ctx, 1)).Count);
        Assert.Equal(3, (await compiled(_ctx, 3)).Count);
        Assert.Single(await compiled(_ctx, 5));
        Assert.Empty(await compiled(_ctx, 99));
    }

    [Table("CqcRow")]
    public sealed class CqcRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CqcContext : DbContext
    {
        public CqcContext(SqliteConnection cn) : base(cn, new SqliteProvider()) { }
    }
}
