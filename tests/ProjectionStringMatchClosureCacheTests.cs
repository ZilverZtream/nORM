using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (F1/F2 — plan-cache poisoning + injection surface): a runtime (closure) string in a
/// PROJECTION string-match — <c>Select(a => a.Name.Contains(term))</c> and StartsWith/EndsWith — was folded
/// and INLINED into the SQL text. Because the plan cache keys on expression structure (closure values are
/// excluded by design), the first caller's value was frozen into the cached plan and replayed for every later
/// caller with a different value — a silent cross-caller data-corruption / boolean-oracle leak. (The same
/// inlining is a MySQL injection surface, since '-doubling is not a safe MySQL escape.) The projection path
/// must not bake a closure value into a reusable plan — mirroring the WHERE path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ProjectionStringMatchClosureCacheTests
{
    [Table("PsmAcct")]
    public sealed class Acct
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Grp { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PsmAcct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Grp INTEGER NOT NULL DEFAULT 1);" +
                              "INSERT INTO PsmAcct (Id,Name,Grp) VALUES (1,'alice',1),(2,'bob',1);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void GroupConcat_projection_with_runtime_separator_is_not_cache_poisoned()
    {
        using var ctx = NewCtx();
        // string.Join(sep, g.Select(..)) lowers to GROUP_CONCAT(.. SEPARATOR '<sep>'); the separator MUST be a
        // literal (not a bound parameter), so it is inlined and the plan must be fold-no-cache for a runtime sep.
        string Joined(string sep) => ctx.Query<Acct>().GroupBy(a => a.Grp)
            .Select(g => string.Join(sep, g.Select(x => x.Name))).First();

        var comma = Joined(",");
        Assert.Contains("alice", comma);
        Assert.Contains(",", comma);       // separator is ','
        var pipe = Joined("|");
        Assert.Contains("|", pipe);        // POISONED plan would still use ',' → no '|'
        Assert.DoesNotContain(",", pipe);
    }

    private static bool HitFor(DbContext ctx, string name, string term)
        => ctx.Query<Acct>().Where(a => a.Name == name)
              .Select(a => a.Name.Contains(term)).First();

    [Fact]
    public void Contains_projection_with_runtime_term_is_not_cache_poisoned()
    {
        using var ctx = NewCtx();
        // First call caches the plan; subsequent calls with a DIFFERENT term must NOT reuse the first term.
        Assert.True(HitFor(ctx, "alice", "ali"));   // 'alice' contains 'ali'
        Assert.False(HitFor(ctx, "bob", "ali"));    // 'bob' does not contain 'ali'
        Assert.True(HitFor(ctx, "bob", "bob"));     // 'bob' contains 'bob' — POISONED plan would still test 'ali' → false
        Assert.False(HitFor(ctx, "alice", "zzz"));  // neither contains 'zzz'
    }

    [Fact]
    public void StartsWith_projection_with_runtime_term_is_not_cache_poisoned()
    {
        using var ctx = NewCtx();
        Assert.True(HitStarts(ctx, "alice", "ali"));
        Assert.False(HitStarts(ctx, "bob", "ali"));
        Assert.True(HitStarts(ctx, "bob", "bo"));
    }

    private static bool HitStarts(DbContext ctx, string name, string term)
        => ctx.Query<Acct>().Where(a => a.Name == name)
              .Select(a => a.Name.StartsWith(term)).First();
}
