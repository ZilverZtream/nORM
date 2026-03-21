using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that <see cref="BatchedNavigationLoader"/> chunks navigation key lists
/// by the provider's <c>MaxParameters</c> limit, preventing "too many variables"
/// errors on SQLite (≤ 999) and equivalent limits on other providers (X2 fix).
/// </summary>
public class BatchedNavigationProviderCapTests
{
    // ── Entity types ──────────────────────────────────────────────────────────

    [Table("bnpc_author")]
    private sealed class BnpcAuthor
    {
        [Key]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public ICollection<BnpcBook> Books { get; set; } = new List<BnpcBook>();
    }

    [Table("bnpc_book")]
    private sealed class BnpcBook
    {
        [Key]
        public int Id { get; set; }

        public int AuthorId { get; set; }

        public string Title { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateAndPopulateDb(int authorCount)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE bnpc_author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT ''); " +
                "CREATE TABLE bnpc_book   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL DEFAULT '')";
            cmd.ExecuteNonQuery();
        }

        // Bulk-insert authors
        var sb = new StringBuilder("INSERT INTO bnpc_author (Id, Name) VALUES ");
        for (int i = 1; i <= authorCount; i++)
        {
            if (i > 1) sb.Append(',');
            sb.Append($"({i},'Author{i}')");
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        // Bulk-insert one book per author
        sb.Clear();
        sb.Append("INSERT INTO bnpc_book (Id, AuthorId, Title) VALUES ");
        for (int i = 1; i <= authorCount; i++)
        {
            if (i > 1) sb.Append(',');
            sb.Append($"({i},{i},'Book{i}')");
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BnpcAuthor>()
                  .HasKey(a => a.Id)
                  .HasMany(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ── X2-1: Below SQLite limit (999) — single query ─────────────────────────

    /// <summary>
    /// With 998 authors (< SQLite MaxParameters=999), all keys fit in a single
    /// WHERE IN query. All books must be loaded correctly.
    /// </summary>
    [Fact]
    public async Task BelowSqliteLimit_998Authors_AllBooksLoaded()
    {
        const int count = 998;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        // Build author entities in memory with correct Ids
        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnpcAuthor { Id = i, Name = $"Author{i}" })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnpcAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(count, results.Length);
        Assert.All(results, r => Assert.Single(r));
    }

    // ── X2-2: At SQLite limit (999) ───────────────────────────────────────────

    /// <summary>
    /// Exactly 999 authors equals SQLite's MaxParameters. Should fit in one query.
    /// </summary>
    [Fact]
    public async Task AtSqliteLimit_999Authors_AllBooksLoaded()
    {
        const int count = 999;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnpcAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnpcAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-3: Exceeds SQLite limit (1000) — chunks into two queries ───────────

    /// <summary>
    /// With 1000 authors (> SQLite MaxParameters=999), the loader must chunk the
    /// key list across multiple queries. Previously, a single WHERE IN (1000 params)
    /// would crash with SQLite error 1 ("too many SQL variables"). After the X2 fix,
    /// all 1000 books must be returned without error.
    /// </summary>
    [Fact]
    public async Task ExceedsSqliteLimit_1000Authors_AllBooksLoaded()
    {
        const int count = 1000;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnpcAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnpcAuthor.Books)))
                           .ToArray();

        // This would previously throw "SqliteException: too many SQL variables" on SQLite.
        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(count, results.Length);
        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-4: Large batch (1050) — two chunks ────────────────────────────────

    /// <summary>
    /// 1050 authors → 2 chunks (989 + 61 = 1050, since reserve=10 reduces max per
    /// chunk to 999-10=989). All 1050 books must be loaded.
    /// </summary>
    [Fact]
    public async Task LargeBatch_1050Authors_AllBooksLoaded()
    {
        const int count = 1050;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnpcAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnpcAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-5: Each author gets its correct book ───────────────────────────────

    /// <summary>
    /// With 1001 authors and one book per author, each LoadNavigationAsync call must
    /// return the correct book for that specific author (not just any book).
    /// </summary>
    [Fact]
    public async Task ExceedsLimit_EachAuthorGetsCorrectBook()
    {
        const int count = 1001;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnpcAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnpcAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30));

        for (int i = 0; i < count; i++)
        {
            Assert.Single(results[i]);
            var book = (BnpcBook)results[i][0];
            Assert.Equal(authors[i].Id, book.AuthorId);
        }
    }

    // ── X2-6: Author with no matching books returns empty list ───────────────

    /// <summary>
    /// An author whose Id has no matching book in the DB must receive an empty
    /// result list (not an exception). Verifies the chunk path handles zero-result
    /// queries gracefully.
    /// </summary>
    [Fact]
    public async Task AuthorWithNoBooks_ReturnsEmptyList()
    {
        // Create DB with schema but only 1 author who has no books
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE bnpc_author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT ''); " +
                "CREATE TABLE bnpc_book   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL DEFAULT ''); " +
                "INSERT INTO bnpc_author (Id, Name) VALUES (99, 'Lonely Author')";
            cmd.ExecuteNonQuery();
        }

        await using var ctx = CreateCtx(cn);
        var loader = new BatchedNavigationLoader(ctx);

        var author = new BnpcAuthor { Id = 99 };

        var result = await loader.LoadNavigationAsync(author, nameof(BnpcAuthor.Books))
                                 .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(result);
    }
}

/// <summary>
/// Verifies that BatchedNavigationLoader correctly chunks navigation key lists when
/// the provider's MaxParameters limit is reached. Uses a fake provider with
/// MaxParameters=100 (chunk size=90) to reproduce the same boundary conditions as
/// SQL Server's 2100 limit without spawning thousands of parallel tasks.
/// </summary>
public class NavigationLoaderSqlServerCapTests
{
    // ── Fake provider: MaxParameters=100 → chunkSize=90 ─────────────────────

    private sealed class FakeSqlServerCapProvider : SqliteProvider
    {
        public override int MaxParameters => 100;
    }

    // ── Entity types ────────────────────────────────────────────────────────

    [Table("bnss_author")]
    private sealed class BnssAuthor
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ICollection<BnssBook> Books { get; set; } = new List<BnssBook>();
    }

    [Table("bnss_book")]
    private sealed class BnssBook
    {
        [Key]
        public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static SqliteConnection CreateAndPopulateDb(int authorCount)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE bnss_author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT ''); " +
                "CREATE TABLE bnss_book   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL DEFAULT '')";
            cmd.ExecuteNonQuery();
        }

        var sb = new StringBuilder("INSERT INTO bnss_author (Id, Name) VALUES ");
        for (int i = 1; i <= authorCount; i++)
        {
            if (i > 1) sb.Append(',');
            sb.Append($"({i},'Author{i}')");
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        sb.Clear();
        sb.Append("INSERT INTO bnss_book (Id, AuthorId, Title) VALUES ");
        for (int i = 1; i <= authorCount; i++)
        {
            if (i > 1) sb.Append(',');
            sb.Append($"({i},{i},'Book{i}')");
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BnssAuthor>()
                  .HasKey(a => a.Id)
                  .HasMany(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };
        return new DbContext(cn, new FakeSqlServerCapProvider(), opts);
    }

    // ── X2-SS-1: Below chunk size (89 < chunkSize=90) — single query ─────────

    [Fact]
    public async Task BelowChunkSize_89Authors_SingleChunkFetchesAll()
    {
        const int count = 89;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnssAuthor { Id = i, Name = $"Author{i}" })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnssAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(count, results.Length);
        Assert.All(results, r => Assert.Single(r));
    }

    // ── X2-SS-2: At MaxParameters (100) — exactly two chunks ────────────────

    [Fact]
    public async Task AtMaxParameters_100Authors_AllBooksLoaded()
    {
        const int count = 100;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnssAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnssAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-SS-3: Exceeds MaxParameters (101) — chunks into two queries ───────

    [Fact]
    public async Task ExceedsMaxParameters_101Authors_AllBooksLoaded()
    {
        const int count = 101;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnssAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnssAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(count, results.Length);
        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-SS-4: Large batch (200) — three chunks ────────────────────────────

    [Fact]
    public async Task LargeBatch_200Authors_AllBooksLoaded()
    {
        const int count = 200;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnssAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnssAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(count, results.Sum(r => r.Count));
    }

    // ── X2-SS-5: Each author gets its correct book ───────────────────────────

    [Fact]
    public async Task ProviderCap_EachAuthorGetsCorrectBook()
    {
        const int count = 101;
        using var cn = CreateAndPopulateDb(count);
        await using var ctx = CreateCtx(cn);

        var authors = Enumerable.Range(1, count)
                                .Select(i => new BnssAuthor { Id = i })
                                .ToList();

        var loader = new BatchedNavigationLoader(ctx);

        var tasks = authors.Select(a => loader.LoadNavigationAsync(a, nameof(BnssAuthor.Books)))
                           .ToArray();

        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        for (int i = 0; i < count; i++)
        {
            Assert.Single(results[i]);
            var book = (BnssBook)results[i][0];
            Assert.Equal(authors[i].Id, book.AuthorId);
        }
    }
}
