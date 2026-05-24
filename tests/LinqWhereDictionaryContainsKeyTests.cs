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
/// Direct sister of <c>d97c5f0</c> (List&lt;T&gt;.Contains fix).
/// <c>Dictionary&lt;K,V&gt;.ContainsKey</c> has the same instance-on-generic-
/// collection shape: a one-arg method on a closed generic type that is not
/// in the translator's safe-types set. A common EF idiom is:
///
///   <c>Where(p =&gt; lookup.ContainsKey(p.Id))</c>
///
/// where <c>lookup</c> is a closure-captured <c>Dictionary&lt;int, T&gt;</c>.
/// LINQ semantics: keep rows whose Id is a key in the dictionary -- the same
/// shape as <c>Where(p =&gt; lookup.Keys.Contains(p.Id))</c> but without
/// allocating an explicit Keys snapshot.
///
/// The probe expects either (a) the IN-list translation that matches
/// LINQ semantics, or (b) an actionable nORM error pointing at the
/// <c>lookup.Keys.ToList()</c> workaround.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDictionaryContainsKeyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DkItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO DkItem VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DkItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_Dictionary_ContainsKey_translates_to_in_clause_over_keys()
    {
        // Dictionary<K,V>.ContainsKey(k) is semantically equivalent to
        // dictionary.Keys.Contains(k). Both should yield the same SQL IN
        // clause. The d97c5f0 fix admits any one-arg `Contains` method on a
        // generic-collection receiver; Dictionary<K,V> implements
        // ICollection<KeyValuePair<K,V>> so the same gate admits it. The
        // dedicated Contains handler at the visitor uses TryGetConstantValue
        // on the receiver and walks the dictionary as IEnumerable -- which
        // yields KeyValuePair<K,V> items, NOT the keys. So this test verifies
        // we don't get a silent-wrongness where the IN clause contains
        // KeyValuePair.ToString() values or similar.
        var lookup = new Dictionary<int, string>
        {
            { 2, "two" },
            { 4, "four" }
        };

        System.Exception? ex = null;
        int[]? ids = null;
        try
        {
            var result = await _ctx.Query<DkItem>()
                .Where(i => lookup.ContainsKey(i.Id))
                .OrderBy(i => i.Id)
                .ToListAsync();
            ids = result.Select(r => r.Id).ToArray();
        }
        catch (System.Exception caught)
        {
            ex = caught;
        }

        Assert.True(ex == null, $"Expected ContainsKey to translate, but got {ex?.GetType().FullName}: {ex?.Message}");
        // Silent-wrongness:
        //   * returned all 4 rows (predicate dropped)
        //   * returned 0 rows (dictionary walked as KeyValuePair items, none match int Id)
        //   * returned wrong subset
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Fact]
    public async Task Where_with_Dictionary_Keys_Contains_works_as_documented_workaround()
    {
        // Workaround pattern: snapshot the Keys to a List or array first; the
        // existing List/array Contains translation handles it.
        var lookup = new Dictionary<int, string>
        {
            { 1, "one" },
            { 3, "three" }
        };
        var keys = lookup.Keys.ToList();
        var result = await _ctx.Query<DkItem>()
            .Where(i => keys.Contains(i.Id))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("DkItem")]
    public sealed class DkItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
