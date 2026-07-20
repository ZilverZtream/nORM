using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// AddNormPool context pooling: a bounded pool of warm contexts is reused across DI scopes (avoiding
/// per-context cache warm-up), and each lease is reset so no state leaks between requests. The critical
/// invariants: the change tracker/identity map is cleared, the native tenant session key is cleared (a
/// cross-tenant leak hazard), and a context holding a live transaction is disposed rather than pooled.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AddNormPoolTests
{
    [Table("PoolWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PoolWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static ServiceProvider BuildPool(SqliteConnection cn, int poolSize = 32)
    {
        var services = new ServiceCollection();
        services.AddNormPool(_ => new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        }, ownsConnection: false), poolSize);
        return services.BuildServiceProvider();
    }

    [Fact]
    public void Pooled_context_is_reused_and_change_tracker_cleared_across_scopes()
    {
        using var cn = NewDb();
        using var sp = BuildPool(cn);

        DbContext ctx1;
        using (var scope1 = sp.CreateScope())
        {
            ctx1 = scope1.ServiceProvider.GetRequiredService<DbContext>();
            ctx1.Add(new Widget { Id = 1, Name = "a" });   // track (no SaveChanges)
            Assert.Single(ctx1.ChangeTracker.Entries);
        } // scope end → ctx1 reset and returned to the pool (NOT disposed)

        using (var scope2 = sp.CreateScope())
        {
            var ctx2 = scope2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);                        // same warm instance reused
            Assert.Empty(ctx2.ChangeTracker.Entries);       // change tracker cleared between leases
        }
    }

    [Fact]
    public async System.Threading.Tasks.Task Query_round_trips_through_a_pooled_context()
    {
        using var cn = NewDb();
        using var sp = BuildPool(cn);

        using (var scope = sp.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<DbContext>();
            ctx.Add(new Widget { Id = 1, Name = "x" });
            await ctx.SaveChangesAsync();
        }
        using (var scope = sp.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<DbContext>();
            var w = ctx.Query<Widget>().First(x => x.Id == 1);
            Assert.Equal("x", w.Name);
        }
    }

    [Fact]
    public void Context_holding_a_live_transaction_is_not_pooled()
    {
        using var cn = NewDb();
        using var sp = BuildPool(cn);

        DbContext ctx1;
        using (var scope1 = sp.CreateScope())
        {
            ctx1 = scope1.ServiceProvider.GetRequiredService<DbContext>();
            ctx1.Database.BeginTransaction();   // open, never committed/disposed
        } // scope end: reset refuses a live-transaction context → it is disposed, not pooled

        using (var scope2 = sp.CreateScope())
        {
            var ctx2 = scope2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.NotSame(ctx1, ctx2);   // a fresh context, not the transaction-holding one
        }
    }

    [Fact]
    public void Reset_clears_the_native_tenant_session_key()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
        }, ownsConnection: false);

        var keyField = typeof(DbContext).GetField("_nativeTenantSessionAppliedKey", BindingFlags.NonPublic | BindingFlags.Instance)!;
        keyField.SetValue(ctx, "tenant-A-session");   // simulate a prior tenant's applied native session

        var reset = typeof(DbContext).GetMethod("TryResetForPooling", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var pooled = (bool)reset.Invoke(ctx, null)!;

        Assert.True(pooled);                        // poolable (no live tx)
        Assert.Null(keyField.GetValue(ctx));        // tenant key cleared → next lease re-applies its own tenant
    }
}
