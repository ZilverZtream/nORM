using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An uncorrelated ctx.Query&lt;T&gt;() terminal in a predicate (no outer-row
/// reference, only a captured closure) is technically constant-evaluable. If the
/// translator FOLDS it, it executes a database query during translation, bakes
/// the boolean/scalar result as a literal, and caches that — so the next
/// execution with a different closure replays the first result. These must
/// translate as server-side subqueries whose closures re-bind per execution.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class UncorrelatedQuerySubqueryFoldTests
{
    [Table("UqsRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:uqs_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE UqsRow_Test (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
                INSERT INTO UqsRow_Test VALUES (1, 10), (2, 20), (3, 30);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Uncorrelated_any_rebinds_closure_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // ONE shared call site → shared cached plan. needle=25 has no row (Any
        // false → 0 rows survive); needle=20 has a row (Any true → 3 rows). A
        // fold-and-cache would replay the first needle's result for the second.
        static async Task<int> CountWhereExists(DbContext c, int needle)
            => (await c.Query<Row>().Where(r => c.Query<Row>().Any(x => x.V == needle)).ToListAsync()).Count;

        Assert.Equal(0, await CountWhereExists(ctx, 25));
        Assert.Equal(3, await CountWhereExists(ctx, 20));
    }

    [Fact]
    public async Task Uncorrelated_contains_rebinds_closure_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        static async Task<int> CountWhereContains(DbContext c, int needle)
            => (await c.Query<Row>().Where(r => c.Query<Row>().Select(x => x.V).Contains(needle)).ToListAsync()).Count;

        Assert.Equal(0, await CountWhereContains(ctx, 25));
        Assert.Equal(3, await CountWhereContains(ctx, 30));
    }

    [Fact]
    public async Task Uncorrelated_all_rebinds_closure_across_cached_plans()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        // All rows have V <= 30. needle=5: not all V <= 5 → false → 0 rows.
        // needle=100: all V <= 100 → true → 3 rows.
        static async Task<int> CountWhereAll(DbContext c, int needle)
            => (await c.Query<Row>().Where(r => c.Query<Row>().All(x => x.V <= needle)).ToListAsync()).Count;

        Assert.Equal(0, await CountWhereAll(ctx, 5));
        Assert.Equal(3, await CountWhereAll(ctx, 100));
    }
}
