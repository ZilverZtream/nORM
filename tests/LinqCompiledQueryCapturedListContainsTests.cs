using System.Collections.Generic;
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
/// Pins the fix for <c>List&lt;T&gt;.Contains</c> (and other generic-collection
/// instance Contains methods) in Where predicates, including the
/// closure-captured-collection shape that compiled queries use.
///
/// Before the fix, <c>Where(i =&gt; list.Contains(i.Id))</c> threw
/// <c>NormQueryException("Method 'Contains' cannot be translated to SQL")</c>
/// while the array equivalent <c>Where(i =&gt; arr.Contains(i.Id))</c> worked,
/// because <c>int[].Contains</c> binds to <c>Enumerable.Contains</c> (declared
/// in the translator's safe-types set) but <c>List&lt;int&gt;.Contains</c> is an
/// instance method on <c>List&lt;T&gt;</c> which wasn't in the safe set. The
/// dedicated Contains handler that rewrites to a SQL IN clause was unreachable
/// because <c>IsTranslatableMethod</c> threw first.
///
/// Tests pin behavior + the mutation-vs-stale-plan probe for CompileQuery.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledQueryCapturedListContainsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CqlcItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO CqlcItem VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CqlcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_List_Contains_two_elements_translates()
    {
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<CqlcItem>()
            .Where(i => ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_List_Contains_three_elements_translates()
    {
        var ids = new List<int> { 3, 4, 5 };
        var result = await _ctx.Query<CqlcItem>()
            .Where(i => ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_HashSet_Contains_translates()
    {
        // HashSet<T>.Contains is also an instance method on a generic collection;
        // verify the IsTranslatableContainsReceiver helper recognises it too.
        var ids = new HashSet<int> { 1, 3, 5 };
        var result = await _ctx.Query<CqlcItem>()
            .Where(i => ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_int_array_Contains_still_works()
    {
        // Regression guard: array Contains was already working (via
        // Enumerable.Contains extension). The fix must not break it.
        var ids = new int[] { 2, 4 };
        var result = await _ctx.Query<CqlcItem>()
            .Where(i => ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Compiled_query_capturing_list_returns_correct_first_call_rows()
    {
        var ids = new List<int> { 1, 2 };
        var compiled = Norm.CompileQuery((DbContext ctx, int _) =>
            ctx.Query<CqlcItem>()
                .Where(i => ids.Contains(i.Id))
                .OrderBy(i => i.Id));

        var rows = await compiled(_ctx, 0);
        Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Compiled_query_capturing_list_picks_up_mutation_between_calls()
    {
        // Silent-wrongness probe: if the SQL plan baked the first-call list
        // values into an IN(...) literal, mutating the list and calling again
        // would return the original Ids. The fix's per-call closure read +
        // cache key derived from list contents must re-translate.
        var ids = new List<int> { 1, 2 };
        var compiled = Norm.CompileQuery((DbContext ctx, int _) =>
            ctx.Query<CqlcItem>()
                .Where(i => ids.Contains(i.Id))
                .OrderBy(i => i.Id));

        var first = await compiled(_ctx, 0);
        Assert.Equal(new[] { 1, 2 }, first.Select(r => r.Id).ToArray());

        ids.Clear();
        ids.AddRange(new[] { 3, 4, 5 });

        var second = await compiled(_ctx, 0);
        Assert.Equal(new[] { 3, 4, 5 }, second.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Compiled_query_capturing_list_two_calls_no_mutation_returns_consistent_results()
    {
        var ids = new List<int> { 2, 4 };
        var compiled = Norm.CompileQuery((DbContext ctx, int _) =>
            ctx.Query<CqlcItem>()
                .Where(i => ids.Contains(i.Id))
                .OrderBy(i => i.Id));

        var a = await compiled(_ctx, 0);
        var b = await compiled(_ctx, 0);
        Assert.Equal(a.Select(r => r.Id).ToArray(), b.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 2, 4 }, a.Select(r => r.Id).ToArray());
    }

    [Table("CqlcItem")]
    public sealed class CqlcItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
