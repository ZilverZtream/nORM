using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that state from one rental of a pooled DbContext (AddNormPool) does NOT leak into the next rental
/// of the SAME pooled instance. Each test rents, DIRTIES the context (tracked entities, tenant, cached query),
/// returns it (scope end resets and re-pools), then rents again — asserting Assert.Same to prove the same warm
/// instance is reused — and verifies the second rental is clean (no leaked tracker entries, tenant read live,
/// warm caches tenant-keyed). Uses a shared SQLite connection so both rentals see the same data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PooledContextResetLeakageTests
{
    [Table("Pr57Widget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("Pr57Item")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string TenantId { get; set; } = "";
    }

    /// <summary>Ambient tenant provider: the returned tenant is mutable, simulating an AsyncLocal/scoped tenant.</summary>
    private sealed class AmbientTenantProvider : ITenantProvider
    {
        public string Current = "A";
        public object GetCurrentTenantId() => Current;
    }

    private static SqliteConnection NewWidgetDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Pr57Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static SqliteConnection NewItemDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE Pr57Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);" +
            "INSERT INTO Pr57Item VALUES (1,'a-one','A'),(2,'b-two','B');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static ServiceProvider BuildWidgetPool(SqliteConnection cn, DbContextOptions opts, int poolSize = 4)
    {
        var services = new ServiceCollection();
        services.AddNormPool(_ => new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false), poolSize);
        return services.BuildServiceProvider();
    }

    private static long CountWidgets(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Pr57Widget;";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ---- Surface 1: ChangeTracker leak (phantom Add persisted by the NEXT rental's SaveChanges) ----
    [Fact]
    public async Task Added_but_unsaved_entity_does_not_persist_on_the_next_leases_SaveChanges()
    {
        using var cn = NewWidgetDb();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        using var sp = BuildWidgetPool(cn, opts);

        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            ctx1.Add(new Widget { Id = 42, Name = "ghost" });   // tracked Added, NO SaveChanges
            Assert.Single(ctx1.ChangeTracker.Entries);
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);                       // SAME warm instance reused
            Assert.Empty(ctx2.ChangeTracker.Entries);      // tracker cleared
            await ctx2.SaveChangesAsync();                 // must NOT flush the previous rental's Add
        }

        Assert.Equal(0, CountWidgets(cn));   // the ghost was never persisted
    }

    // ---- Surface 1b: a tracked MODIFY from a prior rental must not persist on the next SaveChanges ----
    [Fact]
    public async Task Modified_but_unsaved_entity_does_not_persist_on_the_next_leases_SaveChanges()
    {
        using var cn = NewWidgetDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Pr57Widget VALUES (1,'orig');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        using var sp = BuildWidgetPool(cn, opts);

        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            var w = ctx1.Query<Widget>().First(x => x.Id == 1);
            w.Name = "dirty";   // tracked Modified, NO SaveChanges
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);
            await ctx2.SaveChangesAsync();   // must be a no-op
        }

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Name FROM Pr57Widget WHERE Id=1;";
        Assert.Equal("orig", (string)check.ExecuteScalar()!);   // the pending edit did NOT leak into the DB
    }

    // ---- Surface 6: identity-map / first-level cache leak — a fresh load must reflect the DB ----
    [Fact]
    public void Reload_after_reset_yields_a_fresh_instance_not_the_prior_leases_stale_tracked_one()
    {
        using var cn = NewWidgetDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Pr57Widget VALUES (1,'orig');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        using var sp = BuildWidgetPool(cn, opts);

        Widget w1;
        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            w1 = ctx1.Query<Widget>().First(x => x.Id == 1);
            w1.Name = "stale-in-memory";   // mutate the tracked instance, do NOT save
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);
            var w2 = ctx2.Query<Widget>().First(x => x.Id == 1);
            Assert.NotSame(w1, w2);                  // not the prior rental's cached instance
            Assert.Equal("orig", w2.Name);           // reflects the DB, not the stale in-memory edit
        }
    }

    // ---- Surface 2/5: cross-tenant query correctness across leases (ambient tenant + result cache) ----
    [Fact]
    public async Task Second_lease_with_a_different_tenant_sees_only_its_own_rows_not_the_first_leases()
    {
        using var cn = NewItemDb();
        var tenant = new AmbientTenantProvider { Current = "A" };
        var opts = new DbContextOptions
        {
            TenantProvider = tenant,
            OnModelCreating = mb => mb.Entity<Item>().HasKey(i => i.Id)
        };
        var services = new ServiceCollection();
        services.AddNormPool(_ => new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false), 4);
        using var sp = services.BuildServiceProvider();

        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            tenant.Current = "A";
            var aRows = await ctx1.Query<Item>().ToListAsync();
            Assert.Equal(new[] { 1 }, aRows.Select(i => i.Id).OrderBy(x => x).ToArray());
            Assert.All(aRows, i => Assert.Equal("A", i.TenantId));
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);                    // SAME pooled instance
            tenant.Current = "B";
            var bRows = await ctx2.Query<Item>().ToListAsync();
            Assert.All(bRows, i => Assert.Equal("B", i.TenantId));   // NO tenant-A leak
            Assert.Equal(new[] { 2 }, bRows.Select(i => i.Id).OrderBy(x => x).ToArray());
        }
    }

    // ---- Surface 5b: a COMPILED query reused across pooled leases must respect the CURRENT tenant ----
    // The SQLite compiled-query sync path pools a prepared command per context and bakes the tenant as a
    // FIXED parameter. If the per-context command-pool key failed to segregate by tenant, the second
    // lease (tenant B) would reuse tenant A's baked command and read A's rows.
    [Fact]
    public async Task Compiled_query_reused_across_pooled_leases_respects_the_current_tenant()
    {
        using var cn = NewItemDb();
        var tenant = new AmbientTenantProvider { Current = "A" };
        var opts = new DbContextOptions
        {
            TenantProvider = tenant,
            OnModelCreating = mb => mb.Entity<Item>().HasKey(i => i.Id)
        };
        var services = new ServiceCollection();
        services.AddNormPool(_ => new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false), 4);
        using var sp = services.BuildServiceProvider();

        // Compiled ONCE; reused across both leases. The tenant filter is auto-injected by nORM.
        var compiled = Norm.CompileQuery<DbContext, int, Item>((c, minId) => c.Query<Item>().Where(i => i.Id >= minId));

        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            tenant.Current = "A";
            var aRows = await compiled(ctx1, 0);
            Assert.All(aRows, i => Assert.Equal("A", i.TenantId));
            Assert.Equal(new[] { 1 }, aRows.Select(i => i.Id).OrderBy(x => x).ToArray());
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);
            tenant.Current = "B";
            var bRows = await compiled(ctx2, 0);
            Assert.All(bRows, i => Assert.Equal("B", i.TenantId));   // must NOT reuse A's baked command
            Assert.Equal(new[] { 2 }, bRows.Select(i => i.Id).OrderBy(x => x).ToArray());
        }
    }

    // ---- Surface 5: warm RESULT cache must not serve tenant-A's cached rows to a tenant-B rental ----
    [Fact]
    public async Task Result_cache_does_not_serve_tenant_A_rows_to_a_tenant_B_lease()
    {
        using var cn = NewItemDb();
        var tenant = new AmbientTenantProvider { Current = "A" };
        var opts = new DbContextOptions
        {
            TenantProvider = tenant,
            OnModelCreating = mb => mb.Entity<Item>().HasKey(i => i.Id)
        }.UseInMemoryCache();
        var services = new ServiceCollection();
        services.AddNormPool(_ => new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false), 4);
        using var sp = services.BuildServiceProvider();

        var cache = TimeSpan.FromMinutes(5);

        DbContext ctx1;
        using (var s1 = sp.CreateScope())
        {
            ctx1 = s1.ServiceProvider.GetRequiredService<DbContext>();
            tenant.Current = "A";
            var aRows = await ctx1.Query<Item>().Cacheable(cache).ToListAsync();  // populates cache under tenant A
            Assert.All(aRows, i => Assert.Equal("A", i.TenantId));
            Assert.Equal(new[] { 1 }, aRows.Select(i => i.Id).OrderBy(x => x).ToArray());
        } // reset + return

        using (var s2 = sp.CreateScope())
        {
            var ctx2 = s2.ServiceProvider.GetRequiredService<DbContext>();
            Assert.Same(ctx1, ctx2);
            tenant.Current = "B";
            var bRows = await ctx2.Query<Item>().Cacheable(cache).ToListAsync();  // SAME SQL, different tenant
            Assert.All(bRows, i => Assert.Equal("B", i.TenantId));   // must NOT serve A's cached rows
            Assert.Equal(new[] { 2 }, bRows.Select(i => i.Id).OrderBy(x => x).ToArray());
        }
    }
}
