using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that _simpleSqlCache uses a stable per-fingerprint string key.
//
//Root bug: ExpressionFingerprint did not override ToString(). The default struct
//ToString() returns the type name ("nORM.Query.ExpressionFingerprint") for every
//instance, collapsing all distinct fingerprints to the same dictionary key.
//
//Effect: the second distinct Where predicate on the same entity type hits the
//cache key for the first and reuses the wrong SQL shape. The parameter value is
//still extracted from the new expression, so a query like WHERE Id = 'Alice'
//executes — returning zero rows instead of the expected match.
//</summary>
public class SimpleSqlCacheCollisionTests
{
    [Table("CacheItem")]
    private class CacheItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CacheItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "INSERT INTO CacheItem VALUES (1, 'Alice');" +
            "INSERT INTO CacheItem VALUES (2, 'Bob');";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

 // ── Unit test: the fix itself ────────────────────────────────────────────

 //<summary>
 //ExpressionFingerprint.ToString() must return a stable hex string (32 lower-case
 //hex chars encoding _low and _high), NOT the CLR type name.
 //Before the fix: every instance returned "nORM.Query.ExpressionFingerprint".
 //</summary>
    [Fact]
    public void ExpressionFingerprint_ToString_ReturnsHexNotTypeName()
    {
        var fpType = typeof(DbContext).Assembly
            .GetType("nORM.Query.ExpressionFingerprint", throwOnError: true)!;
        var compute = fpType.GetMethod("Compute", BindingFlags.Public | BindingFlags.Static)!;

 // Two expressions that differ only in the literal value
        System.Linq.Expressions.Expression<System.Func<int, bool>> e1 = x => x == 1;
        System.Linq.Expressions.Expression<System.Func<int, bool>> e2 = x => x == 2;

        var fp1 = compute.Invoke(null, new object[] { e1 })!;
        var fp2 = compute.Invoke(null, new object[] { e2 })!;

        var s1 = fp1.ToString()!;
        var s2 = fp2.ToString()!;

 // Must be 32 lowercase hex chars (16 bytes: 8 for _low + 8 for _high)
        Assert.Equal(32, s1.Length);
        Assert.All(s1, c => Assert.True("0123456789abcdef".Contains(c),
            $"Non-hex character '{c}' in fingerprint string '{s1}'"));

 // Must NOT be the CLR type name (which was the bug)
        Assert.DoesNotContain("ExpressionFingerprint", s1);

 // Two different expressions must produce different strings
        Assert.NotEqual(s1, s2);
    }

 // ── Integration tests: end-to-end cache key correctness ─────────────────

 //<summary>
 //regression: two Where predicates on different columns of the same entity type
 //must each produce correct results. The same NormQueryProvider instance is used so
 //both queries hit the same _simpleSqlCache.
 //
 //Before the fix: the second query (Name == "Alice") re-used the SQL template cached
 //by the first (WHERE "Id" = @p0) but bound "Alice" to @p0, executing
 //WHERE "Id" = 'Alice' on an INTEGER column — returning 0 rows.
 //</summary>
    [Fact]
    public async Task TwoDifferentPredicates_SameProvider_BothReturnCorrectResults()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // First query populates the fast-path SQL cache for the Id predicate shape.
            var byId = await ctx.Query<CacheItem>().Where(x => x.Id == 2).ToListAsync();
            Assert.Single(byId);
            Assert.Equal("Bob", byId[0].Name);

 // Second query must NOT reuse the Id-predicate SQL cached above.
 // Before the fix this returned 0 rows (WHERE "Id" = 'Alice').
            var byName = await ctx.Query<CacheItem>().Where(x => x.Name == "Alice").ToListAsync();
            Assert.Single(byName);
            Assert.Equal("Alice", byName[0].Name);
        }
    }

 //<summary>
 //regression: Count queries with structurally different predicates must
 //each count correctly and not share cached SQL.
 //</summary>
    [Fact]
    public async Task CountWithDifferentPredicates_ProduceCorrectCounts()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // Count by Id — populates the count SQL cache entry.
            var countById = await ctx.Query<CacheItem>().Where(x => x.Id == 1).CountAsync();
            Assert.Equal(1, countById);

 // Count by Name — must produce its own SQL, not reuse the Id cache entry.
            var countByName = await ctx.Query<CacheItem>().Where(x => x.Name == "Bob").CountAsync();
            Assert.Equal(1, countByName);

 // Unfiltered count — separate entry from the predicated counts.
            var countAll = await ctx.Query<CacheItem>().CountAsync();
            Assert.Equal(2, countAll);
        }
    }

 //<summary>
 //Same predicate shape but different literal values must not collide.
 //Verifies literal-variance fingerprinting is independent of the column-variance fix.
 //</summary>
    [Fact]
    public async Task SamePredicateShape_DifferentLiterals_ReturnDistinctRows()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // Both queries use the same shape (Name == literal) — exercise the cache
 // for the same column with two different string values on the same provider.
            var alice = await ctx.Query<CacheItem>().Where(x => x.Name == "Alice").ToListAsync();
            var bob = await ctx.Query<CacheItem>().Where(x => x.Name == "Bob").ToListAsync();

            Assert.Single(alice);
            Assert.Single(bob);
            Assert.NotEqual(alice[0].Id, bob[0].Id);
        }
    }

 //<summary>
 //Ordering matters: predicate A then predicate B, AND predicate B then predicate A,
 //must both return the correct entity — the cache must not be order-dependent.
 //</summary>
    [Fact]
    public async Task PredicateOrder_DoesNotAffectCacheCorrectness()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
 // Name first
            var byName = await ctx.Query<CacheItem>().Where(x => x.Name == "Alice").ToListAsync();
            Assert.Single(byName);
            Assert.Equal(1, byName[0].Id);

 // Then Id — if the cache key for Id had been poisoned by the Name entry, wrong SQL.
            var byId = await ctx.Query<CacheItem>().Where(x => x.Id == 2).ToListAsync();
            Assert.Single(byId);
            Assert.Equal("Bob", byId[0].Name);
        }
    }
}
