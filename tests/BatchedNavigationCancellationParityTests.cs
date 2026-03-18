using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

// A1 regression — BatchedNavigationLoader must not link all caller tokens into a
// single shared CancellationTokenSource for the batch DB query.
//
// Bug (before fix):
//   var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(allCallerTokens);
//   var ct = linkedCts.Token;
//   // One caller cancels → linked token fires → DB query canceled → ALL callers get OCE
//
// Fix:
//   ct = CancellationToken.None  (DB query always runs to completion)
//   After DB query: per-caller check callerCt.IsCancellationRequested → SetCanceled or SetResult
//
// Key invariant: canceling caller A's token must not prevent caller B (same batch) from
// receiving its result.

#nullable enable

namespace nORM.Tests;

public class BatchedNavigationCancellationParityTests
{
    // ── Domain model ──────────────────────────────────────────────────────────

    [Table("NavCancel_Author")]
    private sealed class NavCancelAuthor
    {
        [Key]
        public string AuthorId { get; set; } = string.Empty;
        public ICollection<NavCancelBook> Books { get; set; } = new List<NavCancelBook>();
    }

    [Table("NavCancel_Book")]
    private sealed class NavCancelBook
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string AuthorId { get; set; } = string.Empty;
        public string Title { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static DbContextOptions BuildOptions() => new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<NavCancelAuthor>()
              .HasKey(a => a.AuthorId)
              .HasMany(a => a.Books)
              .WithOne()
              .HasForeignKey(b => b.AuthorId);
        }
    };

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void CreateSchema(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS NavCancel_Author (AuthorId TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS NavCancel_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId TEXT NOT NULL, Title TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
    }

    // ── A1-1: mixed-batch — canceled A does not poison uncanceled B ───────────

    /// <summary>
    /// Enqueue A (with a cancellable token) and B (with None) into the same batch window.
    /// Cancel A's token AFTER enqueue (so A is in the batch list).
    /// The batch DB query must still run and deliver results to B.
    /// </summary>
    [Fact]
    public async Task CanceledCallerA_DoesNotBlock_UncanceledCallerB()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);
        await using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());
        var loader = new BatchedNavigationLoader(ctx);

        var authorA = new NavCancelAuthor { AuthorId = "a1" };
        var authorB = new NavCancelAuthor { AuthorId = "a2" };

        using var ctsA = new CancellationTokenSource();

        // Enqueue A first (token NOT yet canceled)
        var taskA = loader.LoadNavigationAsync(authorA, nameof(NavCancelAuthor.Books), ctsA.Token);

        // Cancel A's token synchronously — A is already in the batch list
        ctsA.Cancel();

        // Enqueue B with a non-canceled token
        var taskB = loader.LoadNavigationAsync(authorB, nameof(NavCancelAuthor.Books), CancellationToken.None);

        // B must complete successfully
        var resultB = await taskB.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(resultB); // empty list is fine — NavCancel_Book has no rows

        // A must complete with OCE (canceled or WaitAsync abandoned)
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => taskA.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    /// <summary>
    /// Both A and B use None tokens — both should receive their results (control case).
    /// </summary>
    [Fact]
    public async Task NeitherCanceled_BothReceiveResults()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);
        await using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());
        var loader = new BatchedNavigationLoader(ctx);

        var a1 = new NavCancelAuthor { AuthorId = "x1" };
        var a2 = new NavCancelAuthor { AuthorId = "x2" };

        var t1 = loader.LoadNavigationAsync(a1, nameof(NavCancelAuthor.Books), CancellationToken.None);
        var t2 = loader.LoadNavigationAsync(a2, nameof(NavCancelAuthor.Books), CancellationToken.None);

        var results = await Task.WhenAll(t1, t2).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(results[0]);
        Assert.NotNull(results[1]);
    }

    /// <summary>
    /// Both A and B use canceled tokens — both should get OCE (not hang).
    /// </summary>
    [Fact]
    public async Task BothCanceled_BothGetOce_NeitherHangs()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);
        await using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());
        var loader = new BatchedNavigationLoader(ctx);

        using var ctsA = new CancellationTokenSource();
        using var ctsB = new CancellationTokenSource();

        var a1 = new NavCancelAuthor { AuthorId = "y1" };
        var a2 = new NavCancelAuthor { AuthorId = "y2" };

        var t1 = loader.LoadNavigationAsync(a1, nameof(NavCancelAuthor.Books), ctsA.Token);
        var t2 = loader.LoadNavigationAsync(a2, nameof(NavCancelAuthor.Books), ctsB.Token);

        ctsA.Cancel();
        ctsB.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => t1.WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => t2.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    // ── A1-2: many callers in the batch ───────────────────────────────────────

    /// <summary>
    /// N callers in a single batch window; every other one is canceled after enqueue.
    /// All uncanceled callers must receive results; all canceled callers get OCE.
    /// </summary>
    [Fact]
    public async Task ManyCallers_HalfCanceled_HalfReceiveResults()
    {
        const int n = 6;
        using var cn = OpenMemory();
        CreateSchema(cn);
        await using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());
        var loader = new BatchedNavigationLoader(ctx);

        var ctsList = new List<CancellationTokenSource>();
        var tasks   = new List<Task<List<object>>>();
        var authors = new List<NavCancelAuthor>();

        for (int i = 0; i < n; i++)
        {
            var cts = new CancellationTokenSource();
            ctsList.Add(cts);
            var author = new NavCancelAuthor { AuthorId = $"mc{i}" };
            authors.Add(author);
            tasks.Add(loader.LoadNavigationAsync(author, nameof(NavCancelAuthor.Books), cts.Token));
        }

        // Cancel every even-indexed caller after all are enqueued
        for (int i = 0; i < n; i += 2)
            ctsList[i].Cancel();

        int succeeded = 0, canceled = 0;
        for (int i = 0; i < n; i++)
        {
            try
            {
                await tasks[i].WaitAsync(TimeSpan.FromSeconds(5));
                succeeded++;
            }
            catch (OperationCanceledException)
            {
                canceled++;
            }
        }

        // At least the uncanceled callers must succeed
        Assert.True(succeeded >= n / 2,
            $"Expected at least {n / 2} callers to succeed; got {succeeded}.");
        // Canceled callers must get OCE
        Assert.True(canceled >= n / 2,
            $"Expected at least {n / 2} callers to get OCE; got {canceled}.");
    }

    // ── A1-3: pre-canceled token does not enter batch ─────────────────────────

    /// <summary>
    /// A pre-canceled token must throw at semaphore acquire before entering the batch list,
    /// and must not corrupt subsequent requests.
    /// </summary>
    [Fact]
    public async Task PreCanceled_DoesNotEnterBatch_SubsequentSucceeds()
    {
        using var cn = OpenMemory();
        CreateSchema(cn);
        await using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());
        var loader = new BatchedNavigationLoader(ctx);

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-canceled

        var author = new NavCancelAuthor { AuthorId = "pre1" };
        var author2 = new NavCancelAuthor { AuthorId = "pre2" };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => loader.LoadNavigationAsync(author, nameof(NavCancelAuthor.Books), cts.Token));

        // Subsequent request with valid token must succeed
        var result = await loader.LoadNavigationAsync(author2, nameof(NavCancelAuthor.Books), CancellationToken.None)
                                 .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(result);
    }

    // ── A1-4: missing-relation path honours per-caller cancellation ───────────

    /// <summary>
    /// The early-exit path (missing relation → DeliverEmpty) must also respect
    /// per-caller cancellation: canceled caller gets OCE, uncanceled gets [].
    /// </summary>
    [Fact]
    public async Task MissingRelation_CanceledCallerA_UncanceledCallerB_GetsEmpty()
    {
        using var cn = OpenMemory();
        await using var ctx = new DbContext(cn, new SqliteProvider()); // no model config → no relations
        var loader = new BatchedNavigationLoader(ctx);

        var e1 = new NavCancelBook { Id = 1, AuthorId = "a", Title = "T1" };
        var e2 = new NavCancelBook { Id = 2, AuthorId = "a", Title = "T2" };

        using var ctsA = new CancellationTokenSource();

        // Enqueue A (not yet canceled)
        var taskA = loader.LoadNavigationAsync(e1, "NonExistentProp", ctsA.Token);

        // Cancel after enqueue
        ctsA.Cancel();

        // Enqueue B (uncanceled)
        var taskB = loader.LoadNavigationAsync(e2, "NonExistentProp", CancellationToken.None);

        // B gets empty list (missing relation → empty)
        var resultB = await taskB.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Empty(resultB);

        // A gets OCE
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => taskA.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    // ── A1-5: batch-level exception does not poison uncanceled callers ─────────

    /// <summary>
    /// Regression: when the batch query throws (e.g., mapping error), uncanceled callers
    /// should receive the exception, and a separately-canceled caller should get OCE
    /// (not the batch exception).
    /// </summary>
    [Fact]
    public async Task BatchException_CanceledCallerGetsOce_UncanceledGetsException()
    {
        // Use a context with no model and request a navigation that will fail at the batch level
        using var cn = OpenMemory();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        // Do NOT configure NavCancelAuthor → mapping will exist but table won't → DB exception
        // Actually with no model, GetMapping throws. Use a relation-bearing model that points to
        // a non-existent table to force a DB-level exception.

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NavCancelAuthor>()
                  .HasKey(a => a.AuthorId)
                  .HasMany(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };

        // Use an in-memory DB without creating the tables → the SELECT will fail
        await using var ctx2 = new DbContext(cn, new SqliteProvider(), opts);
        var loader = new BatchedNavigationLoader(ctx2);

        var a1 = new NavCancelAuthor { AuthorId = "ex1" };
        var a2 = new NavCancelAuthor { AuthorId = "ex2" };

        using var ctsA = new CancellationTokenSource();

        var taskA = loader.LoadNavigationAsync(a1, nameof(NavCancelAuthor.Books), ctsA.Token);
        ctsA.Cancel(); // A canceled after enqueue

        var taskB = loader.LoadNavigationAsync(a2, nameof(NavCancelAuthor.Books), CancellationToken.None);

        // A must get OCE (its own token), not the DB exception
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => taskA.WaitAsync(TimeSpan.FromSeconds(5)));

        // B must get some exception (DB error because table doesn't exist) — not hang
        var bEx = await Assert.ThrowsAnyAsync<Exception>(() => taskB.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.IsNotType<OperationCanceledException>(bEx);
    }
}
