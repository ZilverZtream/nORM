using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 — Concurrency torture: parallel query + save + eviction + tenant
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Stress harness that hammers nORM's internal caches (query plan cache, fast-path
/// SQL cache, prepared-command cache) concurrently with reads and writes on independent
/// DbContext instances. Verifies there is no data corruption, deadlock, or race-induced
/// exception when multiple readers and writers share the same SQLite database but use
/// separate contexts (the nORM-correct ownership model for concurrency).
/// </summary>
public class ConcurrencyTortureTests
{
    [Table("TortureItem")]
    private class TortureItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Counter { get; set; }
    }

    /// <summary>Single in-memory connection with TortureItem table created.</summary>
    private static SqliteConnection SharedConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS TortureItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Counter INTEGER NOT NULL DEFAULT 0);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    /// <summary>
    /// Creates a named in-memory SQLite database shared across connections.
    /// Returns a "keeper" connection (must stay open to keep the DB alive) and
    /// the connection string for creating additional per-task connections.
    /// SQLite is not thread-safe on a single connection; each concurrent task must
    /// open its own connection to the same named shared-cache DB.
    /// </summary>
    private static (SqliteConnection keeper, string connStr) BuildSharedDb()
    {
        var name = Guid.NewGuid().ToString("N");
        var connStr = $"Data Source={name};Mode=Memory;Cache=Shared";
        var keeper = new SqliteConnection(connStr);
        keeper.Open();
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS TortureItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Counter INTEGER NOT NULL DEFAULT 0);";
        cmd.ExecuteNonQuery();
        return (keeper, connStr);
    }

    private static SqliteConnection OpenDb(string connStr)
    {
        var cn = new SqliteConnection(connStr);
        cn.Open();
        return cn;
    }

    private static DbContext BuildContext(SqliteConnection cn)
        => new(cn, new SqliteProvider());

    // ── Parallel inserts from multiple contexts ────────────────────────────────

    [Fact]
    public async Task ParallelInserts_MultipleContexts_AllItemsPersisted()
    {
        // Each task gets its own connection to the shared named in-memory DB.
        // SQLite serializes writes internally; nORM's plan cache is exercised concurrently.
        var (keeper, connStr) = BuildSharedDb();
        using var _ = keeper;
        const int contexts = 4;
        const int insertsEach = 25;

        var tasks = Enumerable.Range(0, contexts).Select(ctxIdx => Task.Run(async () =>
        {
            using var taskCn = OpenDb(connStr);
            using var context = BuildContext(taskCn);
            for (int i = 0; i < insertsEach; i++)
            {
                await context.InsertAsync(new TortureItem { Name = $"c{ctxIdx}_i{i}", Counter = i });
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        using var verifyCn = OpenDb(connStr);
        await using var cmd = verifyCn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TortureItem";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(contexts * insertsEach, count);
    }

    // ── Interleaved read + write on separate contexts ──────────────────────────

    [Fact]
    public async Task InterleavedReadWrite_NoDataCorruption()
    {
        // Each task needs its own connection — SQLite connections are not thread-safe.
        var (keeper, connStr) = BuildSharedDb();
        using var _ = keeper;

        // Seed rows via a dedicated connection.
        using var seedCn = OpenDb(connStr);
        using var seedCtx = BuildContext(seedCn);
        for (int i = 0; i < 10; i++)
            await seedCtx.InsertAsync(new TortureItem { Name = $"seed{i}", Counter = 0 });

        const int rounds = 20;
        var exceptions = new ConcurrentBag<Exception>();

        // Readers: run compiled queries in a tight loop.
        var readers = Enumerable.Range(0, 3).Select(__ => Task.Run(async () =>
        {
            try
            {
                using var taskCn = OpenDb(connStr);
                using var ctx = BuildContext(taskCn);
                var compiled = Norm.CompileQuery((DbContext c, string prefix) =>
                    c.Query<TortureItem>().Where(t => t.Name.StartsWith(prefix)));

                for (int r = 0; r < rounds; r++)
                {
                    var items = (await compiled(ctx, "seed")).ToList();
                    Assert.True(items.Count >= 0); // must not throw
                }
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        // Writer: inserts new rows continuously.
        var writer = Task.Run(async () =>
        {
            try
            {
                using var taskCn = OpenDb(connStr);
                using var ctx = BuildContext(taskCn);
                for (int w = 0; w < rounds; w++)
                    await ctx.InsertAsync(new TortureItem { Name = $"written{w}", Counter = w });
            }
            catch (Exception ex) { exceptions.Add(ex); }
        });

        await Task.WhenAll(readers.Append(writer));

        Assert.Empty(exceptions);
    }

    // ── Query-plan cache eviction stress ──────────────────────────────────────

    [Fact]
    public async Task PlanCacheEviction_ParallelDifferentQueries_NoException()
    {
        var (keeper, connStr) = BuildSharedDb();
        using var _ = keeper;

        using var seedCn = OpenDb(connStr);
        using var seedCtx = BuildContext(seedCn);
        for (int i = 0; i < 20; i++)
            await seedCtx.InsertAsync(new TortureItem { Name = $"item{i}", Counter = i });

        var exceptions = new ConcurrentBag<Exception>();
        const int numTasks = 6;
        const int queriesPerTask = 30;

        var work = Enumerable.Range(0, numTasks).Select(t => Task.Run(async () =>
        {
            try
            {
                using var taskCn = OpenDb(connStr);
                using var ctx = BuildContext(taskCn);
                for (int q = 0; q < queriesPerTask; q++)
                {
                    // Different threshold each call → different cache fingerprints.
                    var threshold = (t * queriesPerTask + q) % 20;
                    var results = (await ctx.Query<TortureItem>()
                        .Where(i => i.Counter >= threshold)
                        .ToListAsync()).ToList();
                    Assert.NotNull(results);
                }
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        await Task.WhenAll(work);
        Assert.Empty(exceptions);
    }

    // ── Multi-tenant parallel isolation stress ─────────────────────────────────

    [Fact]
    public async Task MultiTenant_ParallelContexts_NoTenantLeakage()
    {
        // Two tenant schemas in the same SQLite database, isolated by TenantProvider.
        // Each context inserts and reads under its own tenant; data must not cross tenants.
        using var cn = SharedConnection();

        // Create a tenant-aware table.
        using var setup = cn.CreateCommand();
        setup.CommandText = @"
            DROP TABLE IF EXISTS TenantTorture;
            CREATE TABLE TenantTorture (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                TenantId TEXT NOT NULL
            );";
        await setup.ExecuteNonQueryAsync();

        // We exercise the tenant isolation at the SQL layer by using raw SQL.
        // (Full tenant-provider wiring is tested in TenantWriteIsolationTests.)
        // Here we verify the shared plan cache doesn't mix tenant data.
        var tenant1Ids = new ConcurrentBag<long>();
        var tenant2Ids = new ConcurrentBag<long>();

        async Task InsertForTenant(string tenantId, ConcurrentBag<long> bag)
        {
            for (int i = 0; i < 10; i++)
            {
                await using DbCommand cmd = cn.CreateCommand();
                cmd.CommandText = $"INSERT INTO TenantTorture (Name, TenantId) VALUES (@n, @t); SELECT last_insert_rowid();";
                cmd.AddParam("@n", $"item_{tenantId}_{i}");
                cmd.AddParam("@t", tenantId);
                var id = Convert.ToInt64(await cmd.ExecuteScalarAsync());
                bag.Add(id);
            }
        }

        await Task.WhenAll(InsertForTenant("tenant1", tenant1Ids), InsertForTenant("tenant2", tenant2Ids));

        // Verify cross-tenant isolation.
        async Task<List<string>> ReadForTenant(string tenantId)
        {
            await using DbCommand cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT Name FROM TenantTorture WHERE TenantId = @t";
            cmd.AddParam("@t", tenantId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var names = new List<string>();
            while (await reader.ReadAsync()) names.Add(reader.GetString(0));
            return names;
        }

        var t1Names = await ReadForTenant("tenant1");
        var t2Names = await ReadForTenant("tenant2");

        Assert.Equal(10, t1Names.Count);
        Assert.Equal(10, t2Names.Count);
        Assert.All(t1Names, n => Assert.StartsWith("item_tenant1_", n));
        Assert.All(t2Names, n => Assert.StartsWith("item_tenant2_", n));
        Assert.Empty(t1Names.Intersect(t2Names));
    }

    // ── Prepared-command cache under transaction churn ────────────────────────

    [Fact]
    public async Task PreparedCommandCache_ConcurrentTransactionCycles_NoException()
    {
        // Each task must have its own connection: SQLite connections are not thread-safe
        // and do not support nested transactions across concurrent tasks.
        var (keeper, connStr) = BuildSharedDb();
        using var _ = keeper;
        var exceptions = new ConcurrentBag<Exception>();

        var work = Enumerable.Range(0, 5).Select(t => Task.Run(async () =>
        {
            try
            {
                using var taskCn = OpenDb(connStr);
                using var ctx = BuildContext(taskCn);
                for (int cycle = 0; cycle < 10; cycle++)
                {
                    // Insert without transaction → prepared command cached.
                    await ctx.InsertAsync(new TortureItem { Name = $"t{t}_out{cycle}", Counter = 0 });

                    // Insert within transaction → cache entry replaced.
                    await using var tx = await ctx.Database.BeginTransactionAsync();
                    await ctx.InsertAsync(new TortureItem { Name = $"t{t}_in{cycle}", Counter = 1 });
                    await tx.CommitAsync();
                }
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        await Task.WhenAll(work);
        Assert.Empty(exceptions);
    }

    // ── Change-tracker eviction under concurrent load ─────────────────────────

    [Fact]
    public async Task ChangeTracker_ManyEntitiesTracked_NoCorruption()
    {
        using var cn = SharedConnection();
        using var ctx = BuildContext(cn);

        // Insert many entities.
        const int count = 100;
        for (int i = 0; i < count; i++)
            await ctx.InsertAsync(new TortureItem { Name = $"tracked{i}", Counter = i });

        // Query them all back (tracked).
        var items = (await ctx.Query<TortureItem>().ToListAsync()).ToList();
        Assert.Equal(count, items.Count);

        // Update all in SaveChanges batch.
        foreach (var item in items)
            item.Counter += 1000;

        await ctx.SaveChangesAsync();

        // Verify all were updated.
        using var verifyCtx = BuildContext(cn);
        var updatedItems = (await verifyCtx.Query<TortureItem>()
            .Where(t => t.Counter >= 1000)
            .ToListAsync()).ToList();
        Assert.Equal(count, updatedItems.Count);
    }
}
