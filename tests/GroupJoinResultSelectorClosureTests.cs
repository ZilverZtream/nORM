using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The GroupJoin result selector runs client-side as a compiled delegate cached
/// with the plan. A closure captured inside it (here the bound of a filtered
/// per-group aggregate) must bind the CURRENT execution's value on every plan
/// cache hit — the original defect replayed the first execution's value forever.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinResultSelectorClosureTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GjcParent")]
    public sealed class GjcParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GjcChild")]
    public sealed class GjcChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task Filtered_aggregate_closure_rebinds_on_each_execution()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GjcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE GjcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO GjcParent VALUES (1,'a'),(2,'b');
                INSERT INTO GjcChild  VALUES (1,1,1),(2,1,5),(3,1,9),(4,2,3);
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        async Task<int[]> CountsAboveAsync(int bound)
        {
            var rows = await ctx.Query<GjcParent>()
                .GroupJoin(ctx.Query<GjcChild>(), p => p.Id, c => c.ParentId,
                    (p, cs) => new { p.Id, N = cs.Where(c => c.Val > bound).Count() })
                .ToListAsync();
            return rows.OrderBy(x => x.Id).Select(x => x.N).ToArray();
        }

        // First execution builds and caches the plan with bound = 0.
        Assert.Equal(new[] { 3, 1 }, await CountsAboveAsync(0));
        // Cache hits must bind the fresh closure value, not replay bound = 0.
        Assert.Equal(new[] { 2, 0 }, await CountsAboveAsync(4));
        Assert.Equal(new[] { 1, 0 }, await CountsAboveAsync(5));
        Assert.Equal(new[] { 0, 0 }, await CountsAboveAsync(9));
    }
}
