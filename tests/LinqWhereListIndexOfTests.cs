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
/// Probes <c>List&lt;T&gt;.IndexOf</c> used as a set-membership predicate:
/// <c>Where(p =&gt; list.IndexOf(p.Id) &gt;= 0)</c>. This is a real idiom
/// some users reach for (especially those coming from collection APIs that
/// lack a Contains method) -- LINQ semantics:
///   * <c>IndexOf(x) &gt;= 0</c> is equivalent to <c>list.Contains(x)</c>
///   * <c>IndexOf(x) == -1</c> is equivalent to <c>!list.Contains(x)</c>
///
/// Since the SQL equivalent of "index of x in collection" doesn't exist
/// (SQL has no positional concept on a values list), the only translation
/// that makes sense is to rewrite the predicate to its Contains form OR
/// fail with an actionable NormUnsupportedFeatureException. The worst
/// outcome would be silent fall-through to a full materialization +
/// client-side IndexOf.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereListIndexOfTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WlioItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO WlioItem VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WlioItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_List_IndexOf_fails_fast_with_nORM_typed_error()
    {
        // IndexOf has no SQL equivalent; the translator must fail fast with
        // a nORM-typed error rather than fall through to client-eval (which
        // would silently materialize the whole table and run IndexOf in C#).
        var ids = new List<int> { 2, 4 };
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
            await _ctx.Query<WlioItem>()
                .Where(i => ids.IndexOf(i.Id) >= 0)
                .OrderBy(i => i.Id)
                .ToListAsync());

        Assert.True(
            ex is NormException || ex is System.InvalidOperationException || ex is System.NotSupportedException,
            $"List.IndexOf threw an unfriendly error: {ex.GetType().FullName}: {ex.Message}");
        // Actionability: message should mention the unsupported method name
        // or point at the translatable alternative (Contains).
        var msgLower = ex.Message.ToLowerInvariant();
        Assert.True(
            msgLower.Contains("indexof") || msgLower.Contains("contains") || msgLower.Contains("not supported") || msgLower.Contains("translat"),
            $"List.IndexOf error message lacks actionable hint: {ex.Message}");
    }

    [Fact]
    public async Task Where_with_Contains_equivalent_of_IndexOf_idiom_is_supported_workaround()
    {
        // Positive guard: the documented workaround `list.Contains(p.Id)` works
        // via the d97c5f0 fix. Users hitting the IndexOf error should be able
        // to switch to Contains and get the expected rows.
        var ids = new List<int> { 2, 4 };
        var result = await _ctx.Query<WlioItem>()
            .Where(i => ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());

        // And the negation: !Contains is the IndexOf == -1 equivalent.
        var inverse = await _ctx.Query<WlioItem>()
            .Where(i => !ids.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, inverse.Select(r => r.Id).ToArray());
    }

    [Table("WlioItem")]
    public sealed class WlioItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
