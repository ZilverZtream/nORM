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
/// Pins <c>Norm.CompileQuery((ctx, n) => ctx.Query&lt;T&gt;().Take(n))</c> —
/// the Take count is a runtime parameter (TParam), not a baked literal. Two
/// invocations with different counts must return different result sets.
/// Silent-wrongness risk if the compiled query caches the SQL with the FIRST
/// invocation's count baked in (or worse, with a constant captured from the
/// expression tree) — every subsequent call would silently return the same
/// rows regardless of the parameter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledQueryTakeParameterTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private CqtContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CqtRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO CqtRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new CqtContext(_cn);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Compiled_query_with_parameterised_take_returns_correct_count_per_invocation()
    {
        var query = Norm.CompileQuery((CqtContext c, int n) =>
            c.Query<CqtRow>().OrderBy(r => r.Id).Take(n));

        // First invocation: Take(2) → 2 rows.
        var first = await query(_ctx, 2);
        Assert.Equal(2, first.Count);
        Assert.Equal(1, first[0].Id);
        Assert.Equal(2, first[1].Id);

        // Second invocation with a DIFFERENT count: Take(3) → 3 rows.
        // If the compiled query baked the first call's `2` into the SQL template,
        // this would still return 2 rows.
        var second = await query(_ctx, 3);
        Assert.Equal(3, second.Count);
        Assert.Equal(1, second[0].Id);
        Assert.Equal(3, second[2].Id);

        // Third invocation with yet another value to defeat any 2-element LRU cache
        // that might happen to alternate between the two values.
        var third = await query(_ctx, 5);
        Assert.Equal(5, third.Count);
    }

    [Table("CqtRow")]
    public sealed class CqtRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CqtContext : DbContext
    {
        public CqtContext(SqliteConnection cn) : base(cn, new SqliteProvider()) { }
    }
}
