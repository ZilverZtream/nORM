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

namespace nORM.Tests;

/// <summary>
/// Verifies that BatchedNavigationLoader resolves all queued TaskCompletionSources
/// on every exit path and that cancellation tokens are propagated to async DB operations.
/// </summary>
public class BatchedNavigationBatchTests
{
    [Table("NavBatch_Item")]
    private sealed class NavBatchItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("NavBatch_Author")]
    private sealed class NavBatchAuthor
    {
        public string? AuthorId { get; set; }
        public ICollection<NavBatchBook> Books { get; set; } = new List<NavBatchBook>();
    }

    [Table("NavBatch_Book")]
    private sealed class NavBatchBook
    {
        [Key]
        public int Id { get; set; }
        public string? AuthorId { get; set; }
    }

    // ── A1/X1: TCS resolution on early-return paths ───────────────────────

    /// <summary>
    /// When the requested navigation property name has no corresponding relation
    /// in the entity's mapping, the batch must resolve every pending TCS with an
    /// empty result list instead of leaving them permanently unresolved.
    /// </summary>
    [Fact]
    public async Task MissingRelation_BatchCompletes_ReturnsEmptyList()
    {
        using var cn = CreateOpenConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var loader = new BatchedNavigationLoader(ctx);

        var entity = new NavBatchItem { Id = 1, Name = "Test" };

        // "NonExistentNav" has no registered relation on NavBatchItem
        var resultTask = loader.LoadNavigationAsync(entity, "NonExistentNav");

        var result = await resultTask.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Empty(result);
    }

    /// <summary>
    /// When every entity in the batch has a null principal key value, no keys
    /// survive the null filter. The batch must resolve all pending TCSs with
    /// empty result lists instead of leaving them unresolved.
    /// </summary>
    [Fact]
    public async Task AllNullPrincipalKeys_BatchCompletes_ReturnsEmptyList()
    {
        using var cn = CreateOpenConnection();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NavBatchAuthor>()
                    .HasKey(a => a.AuthorId!)
                    .HasMany(a => a.Books)
                    .WithOne()
                    .HasForeignKey(b => b.AuthorId!);
            }
        };

        await using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var loader = new BatchedNavigationLoader(ctx);

        // AuthorId is null → PrincipalKey.Getter returns null → all keys filtered
        var entity = new NavBatchAuthor { AuthorId = null };

        var resultTask = loader.LoadNavigationAsync(entity, "Books");

        var result = await resultTask.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Empty(result);
    }

    /// <summary>
    /// Multiple entities with null principal keys in the same batch must all
    /// have their TCSs resolved with empty lists.
    /// </summary>
    [Fact]
    public async Task MultipleEntities_AllNullKeys_AllTcsResolved()
    {
        using var cn = CreateOpenConnection();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NavBatchAuthor>()
                    .HasKey(a => a.AuthorId!)
                    .HasMany(a => a.Books)
                    .WithOne()
                    .HasForeignKey(b => b.AuthorId!);
            }
        };

        await using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var loader = new BatchedNavigationLoader(ctx);

        var entities = new[]
        {
            new NavBatchAuthor { AuthorId = null },
            new NavBatchAuthor { AuthorId = null },
            new NavBatchAuthor { AuthorId = null }
        };

        var tasks = new Task<List<object>>[entities.Length];
        for (int i = 0; i < entities.Length; i++)
            tasks[i] = loader.LoadNavigationAsync(entities[i], "Books");

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(3));

        Assert.All(results, r => Assert.Empty(r));
    }

    // ── A2: cancellation token propagation ────────────────────────────────

    /// <summary>
    /// A pre-canceled token must cause LoadNavigationAsync to throw
    /// OperationCanceledException without hanging or corrupting the loader state.
    /// </summary>
    [Fact]
    public async Task PreCanceled_ThrowsOperationCanceledException()
    {
        using var cn = CreateOpenConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var loader = new BatchedNavigationLoader(ctx);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var entity = new NavBatchItem { Id = 1 };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => loader.LoadNavigationAsync(entity, "AnyProp", cts.Token));
    }

    /// <summary>
    /// A token that cancels shortly after enqueueing must unblock the awaiting caller
    /// via WaitAsync(ct). The task must complete without hanging, either by throwing
    /// OperationCanceledException or by returning before cancellation fires.
    /// </summary>
    [Fact]
    public async Task TokenCanceledAfterEnqueue_TaskCompletesWithoutHanging()
    {
        using var cn = CreateOpenConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var loader = new BatchedNavigationLoader(ctx);

        // Cancel at 1 ms — fires well before the batch timer (10 ms)
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

        var entity = new NavBatchItem { Id = 1 };
        var loadTask = loader.LoadNavigationAsync(entity, "NonExistentNav", cts.Token);

        var completed = await Task.WhenAny(loadTask, Task.Delay(3000));
        Assert.Same(loadTask, completed);
    }

    /// <summary>
    /// After a pre-canceled request throws, subsequent requests on the same loader
    /// with a valid token must still complete successfully.
    /// </summary>
    [Fact]
    public async Task CanceledRequest_DoesNotCorruptLoader_SubsequentRequestSucceeds()
    {
        using var cn = CreateOpenConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var loader = new BatchedNavigationLoader(ctx);

        // Canceled request
        using var canceledCts = new CancellationTokenSource();
        canceledCts.Cancel();
        var entity = new NavBatchItem { Id = 1 };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => loader.LoadNavigationAsync(entity, "AnyProp", canceledCts.Token));

        // Subsequent uncanceled request must succeed
        var result = await loader.LoadNavigationAsync(entity, "NonExistentNav")
            .WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Empty(result);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateOpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }
}
