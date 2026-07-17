using System.Collections;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that disposing a DbContext disposes its cached NormQueryProvider — which releases the
/// pooled DbCommands and decrements the static active-provider count. Without the fix the
/// provider was created and cached but never disposed, so the pooled commands (and SQLite
/// prepared-statement handles) leaked and the background timers never stopped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class QueryProviderDisposalContractTests
{
    [Table("QpdRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }

    [Fact]
    public async Task Disposing_the_context_disposes_the_cached_query_provider()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE QpdRow (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL); INSERT INTO QpdRow VALUES (1, 1), (2, 0);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Exercise the paths that pool commands: a Count and a couple of plan queries.
        await ctx.Query<Row>().CountAsync(r => r.Active);
        await ctx.Query<Row>().Where(r => r.Active).ToListAsync();
        await ctx.Query<Row>().Where(r => r.Id == 1).ToListAsync();

        var provider = typeof(DbContext)
            .GetField("_cachedQueryProvider", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(ctx);
        Assert.NotNull(provider);

        var pooledBefore = TotalPooledCommands(provider!);
        Assert.True(pooledBefore > 0,
            $"Expected the query provider to have pooled at least one command; got {pooledBefore}. " +
            "The test can't prove disposal if nothing was pooled.");

        ctx.Dispose();

        // Disposal must clear the pools (NormQueryProvider.Dispose clears them). Without the fix
        // the provider is never disposed and the pools stay populated.
        Assert.Equal(0, TotalPooledCommands(provider!));
    }

    private static int TotalPooledCommands(object provider)
    {
        var total = 0;
        foreach (var name in new[] { "_pooledCountCommands", "_pooledPlanCommands" })
        {
            var field = provider.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
            if (field?.GetValue(provider) is ICollection collection)
                total += collection.Count;
        }
        return total;
    }
}
