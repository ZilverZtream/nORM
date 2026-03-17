using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// C1 — GlobalFilters concurrent mutation race (Gate 4.0→4.5)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that concurrent calls to AddGlobalFilter while queries are being
/// translated/executed do not throw InvalidOperationException.
///
/// C1 root cause: GlobalFilters was backed by a plain Dictionary. AddGlobalFilter
/// mutated the dictionary and its inner List without synchronization; concurrent
/// enumeration in ApplyGlobalFilters threw "collection was modified".
///
/// Fix: GlobalFilters is now backed by a ConcurrentDictionary. AddGlobalFilter
/// uses AddOrUpdate with copy-on-write inner lists, so the query pipeline always
/// sees a stable snapshot and never races against in-place mutation.
/// </summary>
public class GlobalFilterConcurrencyTests
{
    [Table("GfcEntity")]
    private class GfcEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id    { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx, DbContextOptions Opts)
        BuildContext(DbContextOptions opts)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE GfcEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider(), opts), opts);
    }

    // ── C1-1: AddGlobalFilter concurrent with Count() translation ─────────────

    /// <summary>
    /// Many threads call AddGlobalFilter while many other threads execute Count()
    /// (which triggers ApplyGlobalFilters). Must not throw.
    /// </summary>
    [Fact]
    public async Task AddGlobalFilter_ConcurrentWithQueryTranslation_NoException()
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GfcEntity>()
        };
        var (cn, ctx, _) = BuildContext(opts);
        using var _cn = cn;
        await using var _ctx = ctx;

        // Seed a couple rows.
        ctx.Add(new GfcEntity { Value = 1 });
        ctx.Add(new GfcEntity { Value = 2 });
        await ctx.SaveChangesAsync();

        var errors = new ConcurrentBag<Exception>();
        const int threads = 8;
        const int opsPerThread = 200;

        // Writer threads: repeatedly add global filters.
        var writers = Enumerable.Range(0, threads / 2).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < opsPerThread; i++)
            {
                try
                {
                    int threshold = i; // capture
                    opts.AddGlobalFilter<GfcEntity>(e => e.Value >= threshold);
                }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        // Reader threads: repeatedly execute Count() which enumerates GlobalFilters.
        var readers = Enumerable.Range(0, threads / 2).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < opsPerThread; i++)
            {
                try { _ = await ctx.Query<GfcEntity>().CountAsync(); }
                catch (OperationCanceledException) { /* acceptable */ }
                catch (Exception ex) { errors.Add(ex); }
            }
        })).ToList();

        await Task.WhenAll(writers.Concat(readers));

        Assert.Empty(errors);
    }

    // ── C1-2: AddGlobalFilter is idempotent under concurrent writers ───────────

    [Fact]
    public void AddGlobalFilter_ConcurrentWriters_AllFiltersRegistered()
    {
        var opts = new DbContextOptions();
        const int numFilters = 100;

        // All threads add filters for the same entity type simultaneously.
        Parallel.For(0, numFilters, i =>
        {
            int threshold = i;
            opts.AddGlobalFilter<GfcEntity>(e => e.Value > threshold);
        });

        // All filters must be registered (no lost updates).
        Assert.True(opts.GlobalFilters.TryGetValue(typeof(GfcEntity), out var list));
        Assert.Equal(numFilters, list!.Count);
    }

    // ── C1-3: GlobalFilters.Count is stable under concurrent mutation ──────────

    [Fact]
    public void GlobalFilters_Count_NeverNegativeUnderConcurrentMutation()
    {
        var opts = new DbContextOptions();
        var errors = new ConcurrentBag<string>();

        Parallel.For(0, 500, i =>
        {
            int threshold = i;
            opts.AddGlobalFilter<GfcEntity>(e => e.Value > threshold);
            var count = opts.GlobalFilters.Count;
            if (count < 0)
                errors.Add($"Negative count {count} at iteration {i}");
        });

        Assert.Empty(errors);
    }

    // ── C1-4: AddGlobalFilter for multiple entity types is race-free ───────────

    [Table("GfcOther")]
    private class GfcOther
    {
        [Key]
        public int Id   { get; set; }
        public int Code { get; set; }
    }

    [Fact]
    public void AddGlobalFilter_MultipleEntityTypes_Concurrent_NoLostFilters()
    {
        var opts = new DbContextOptions();
        const int n = 50;

        Parallel.For(0, n * 2, i =>
        {
            if (i % 2 == 0)
            {
                int v = i;
                opts.AddGlobalFilter<GfcEntity>(e => e.Value > v);
            }
            else
            {
                int c = i;
                opts.AddGlobalFilter<GfcOther>(e => e.Code < c);
            }
        });

        Assert.True(opts.GlobalFilters.ContainsKey(typeof(GfcEntity)));
        Assert.True(opts.GlobalFilters.ContainsKey(typeof(GfcOther)));
        Assert.Equal(n, opts.GlobalFilters[typeof(GfcEntity)].Count);
        Assert.Equal(n, opts.GlobalFilters[typeof(GfcOther)].Count);
    }
}
