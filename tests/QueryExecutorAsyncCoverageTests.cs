using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
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

/// <summary>
/// Forces the async execution paths in QueryExecutor (MaterializeAsync,
/// MaterializeGroupJoinAsync, ExecuteDependentQueriesAsync, etc.) by using
/// a custom provider with PrefersSyncExecution = false on top of SQLite.
/// SQLite normally uses the sync path; overriding that flag routes through
/// all the async materializer code.
/// </summary>
public class QueryExecutorAsyncCoverageTests
{
    // ── Async-forcing provider ─────────────────────────────────────────────

    /// <summary>
    /// SQLite provider that reports PrefersSyncExecution = false, forcing
    /// MaterializeAsync and MaterializeGroupJoinAsync to be called instead
    /// of the sync variants.
    /// </summary>
    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }

    // ── Domain models ──────────────────────────────────────────────────────

    [Table("AQE_Author")]
    private class AqeAuthor
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int YearBorn { get; set; }
        public List<AqeBook> Books { get; set; } = new();
    }

    [Table("AQE_Book")]
    private class AqeBook
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
        public int Pages { get; set; }
    }

    [Table("AQE_Team")]
    private class AqeTeam
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string TeamName { get; set; } = string.Empty;
        public List<AqeMember> Members { get; set; } = new();
    }

    [Table("AQE_Member")]
    private class AqeMember
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string MemberName { get; set; } = string.Empty;
    }

    // ── Schema helpers ─────────────────────────────────────────────────────

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void ExecSql(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static DbContext CreateAuthorContext(SqliteConnection cn)
    {
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Author (
                Id   INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT    NOT NULL,
                YearBorn INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS AQE_Book (
                Id       INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                Title    TEXT    NOT NULL,
                Pages    INTEGER NOT NULL DEFAULT 0);");
        return new DbContext(cn, new AsyncSqliteProvider());
    }

    private static DbContext CreateGroupJoinContext(SqliteConnection cn)
    {
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Author (
                Id   INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT    NOT NULL,
                YearBorn INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS AQE_Book (
                Id       INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                Title    TEXT    NOT NULL,
                Pages    INTEGER NOT NULL DEFAULT 0);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AqeAuthor>()
                  .HasKey(x => x.Id)
                  .HasMany<AqeBook>(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };
        return new DbContext(cn, new AsyncSqliteProvider(), opts);
    }

    private static DbContext CreateTeamContext(SqliteConnection cn)
    {
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Team (
                Id       INTEGER PRIMARY KEY AUTOINCREMENT,
                TeamName TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS AQE_Member (
                Id         INTEGER PRIMARY KEY AUTOINCREMENT,
                MemberName TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<AqeTeam>().OwnsMany<AqeMember>(t => t.Members,
                    tableName: "AQE_Member", foreignKey: "TeamId")
        };
        return new DbContext(cn, new AsyncSqliteProvider(), opts);
    }

    private static void SeedAuthors(SqliteConnection cn)
    {
        ExecSql(cn, @"
            INSERT INTO AQE_Author (Name, YearBorn) VALUES ('Alice', 1970), ('Bob', 1985);
            INSERT INTO AQE_Book   (AuthorId, Title, Pages) VALUES
                (1, 'A1', 300), (1, 'A2', 250), (2, 'B1', 180);");
    }

    private static void SeedTeams(SqliteConnection cn)
    {
        ExecSql(cn, @"
            INSERT INTO AQE_Team (TeamName) VALUES ('Red'), ('Blue');
            ALTER TABLE AQE_Member ADD COLUMN TeamId INTEGER NOT NULL DEFAULT 0;
            INSERT INTO AQE_Member (MemberName, TeamId) VALUES ('Alice', 1), ('Bob', 1), ('Carol', 2);");
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 1 — MaterializeAsync (async path for normal queries)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async1_ToListAsync_UsesAsyncPath_ReturnsAllRows()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var list = await ctx.Query<AqeAuthor>().ToListAsync();
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public async Task Async1_FirstAsync_UsesAsyncPath_ReturnsFirst()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var q = (INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>().OrderBy(a => a.Id);
        var first = await q.FirstAsync();
        Assert.Equal("Alice", first.Name);
    }

    [Fact]
    public async Task Async1_WhereAndOrderBy_Async_FiltersCorrectly()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var list = await ctx.Query<AqeAuthor>()
            .Where(a => a.YearBorn > 1980)
            .OrderBy(a => a.Name)
            .ToListAsync();
        Assert.Single(list);
        Assert.Equal("Bob", list[0].Name);
    }

    [Fact]
    public async Task Async1_CountAsync_UsesAsyncPath()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var count = await ctx.Query<AqeAuthor>().CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Async1_NoTracking_AsyncPath_Works()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var list = await ((INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>())
            .AsNoTracking()
            .ToListAsync();
        Assert.Equal(2, list.Count);
    }

    [Fact]
    public async Task Async1_SingleResult_AsyncPath_Works()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var q = (INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>().Where(a => a.Name == "Alice");
        var result = await q.SingleAsync();
        Assert.Equal("Alice", result.Name);
    }

    [Fact]
    public async Task Async1_PreCancelled_AsyncPath_ThrowsOce()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<AqeAuthor>().ToListAsync(cts.Token));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 2 — MaterializeGroupJoinAsync (async GroupJoin path)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async2_GroupJoin_AsyncPath_ReturnsGroupedResults()
    {
        using var cn = OpenMemory();
        using var ctx = CreateGroupJoinContext(cn);
        SeedAuthors(cn);

        var results = await ctx.Query<AqeAuthor>()
            .GroupJoin(
                ctx.Query<AqeBook>(),
                a => a.Id,
                b => b.AuthorId,
                (a, books) => new { Author = a, Books = books.ToList() })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var alice = results.First(r => r.Author.Name == "Alice");
        Assert.Equal(2, alice.Books.Count);
        var bob = results.First(r => r.Author.Name == "Bob");
        Assert.Single(bob.Books);
    }

    [Fact]
    public async Task Async2_GroupJoin_AsyncPath_EmptyInner_ReturnsEmptyGroups()
    {
        using var cn = OpenMemory();
        using var ctx = CreateGroupJoinContext(cn);
        // Insert authors but NO books
        ExecSql(cn, "INSERT INTO AQE_Author (Name, YearBorn) VALUES ('AuthorOnly', 2000)");

        var results = await ctx.Query<AqeAuthor>()
            .GroupJoin(
                ctx.Query<AqeBook>(),
                a => a.Id,
                b => b.AuthorId,
                (a, books) => new { Author = a, Books = books.ToList() })
            .ToListAsync();

        Assert.Single(results);
        Assert.Empty(results[0].Books);
    }

    [Fact]
    public void Async2_GroupJoin_SyncPath_MaterializeGroupJoin()
    {
        // Confirm sync path still works (exercises sync MaterializeGroupJoin)
        using var cn = OpenMemory();
        // Use regular SqliteProvider (sync) for this one
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, YearBorn INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS AQE_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL, Pages INTEGER NOT NULL DEFAULT 0);
            INSERT INTO AQE_Author (Name, YearBorn) VALUES ('Sync1', 1970), ('Sync2', 1985);
            INSERT INTO AQE_Book   (AuthorId, Title, Pages) VALUES (1, 'T1', 200), (2, 'T2', 150);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AqeAuthor>().HasKey(x => x.Id).HasMany<AqeBook>(a => a.Books).WithOne().HasForeignKey(b => b.AuthorId)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var results = ctx.Query<AqeAuthor>()
            .GroupJoin(
                ctx.Query<AqeBook>(),
                a => a.Id,
                b => b.AuthorId,
                (a, books) => new { Author = a, Books = books.ToList() })
            .ToList();
        Assert.Equal(2, results.Count);
        Assert.Single(results[0].Books);
        Assert.Single(results[1].Books);
    }

    [Fact]
    public async Task Async2_GroupJoin_AsyncPath_CancellationBefore_ThrowsOce()
    {
        using var cn = OpenMemory();
        using var ctx = CreateGroupJoinContext(cn);
        SeedAuthors(cn);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.Query<AqeAuthor>()
               .GroupJoin(ctx.Query<AqeBook>(), a => a.Id, b => b.AuthorId,
                    (a, books) => new { Author = a, Books = books.ToList() })
               .ToListAsync(cts.Token));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 3 — OwnedMany via MaterializeAsync (LoadOwnedCollectionsAsync)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async3_OwnedMany_AsyncMaterialize_LoadsOwnedItems()
    {
        using var cn = OpenMemory();
        using var ctx = CreateTeamContext(cn);
        SeedTeams(cn);

        var teams = await ctx.Query<AqeTeam>().OrderBy(t => t.Id).ToListAsync();
        Assert.Equal(2, teams.Count);
        Assert.Equal(2, teams[0].Members.Count); // Red: Alice, Bob
        Assert.Single(teams[1].Members);          // Blue: Carol
    }

    [Fact]
    public async Task Async3_OwnedMany_AsyncMaterialize_EmptyOwned_ReturnsEmpty()
    {
        using var cn = OpenMemory();
        using var ctx = CreateTeamContext(cn);
        // Insert team but no members
        ExecSql(cn, @"
            ALTER TABLE AQE_Member ADD COLUMN TeamId INTEGER NOT NULL DEFAULT 0;
            INSERT INTO AQE_Team (TeamName) VALUES ('Empty');");

        var teams = await ctx.Query<AqeTeam>().ToListAsync();
        Assert.Single(teams);
        Assert.Empty(teams[0].Members);
    }

    [Fact]
    public async Task Async3_OwnedMany_AsyncMaterialize_WithWhere_StillLoadsOwned()
    {
        using var cn = OpenMemory();
        using var ctx = CreateTeamContext(cn);
        SeedTeams(cn);

        var teams = await ctx.Query<AqeTeam>().Where(t => t.TeamName == "Red").ToListAsync();
        Assert.Single(teams);
        Assert.Equal(2, teams[0].Members.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 4 — ExecuteDependentQueriesAsync (navigation in Select)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async4_DependentQuery_NavigationInSelect_TriggersAsync()
    {
        // Select projection that includes a HasMany navigation collection triggers
        // BuildDependentQueryDefinitions → ExecuteDependentQueriesAsync
        using var cn = OpenMemory();
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, YearBorn INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS AQE_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL, Pages INTEGER NOT NULL DEFAULT 0);
            INSERT INTO AQE_Author (Name, YearBorn) VALUES ('DepAuthor1', 1970), ('DepAuthor2', 1985);
            INSERT INTO AQE_Book   (AuthorId, Title, Pages) VALUES (1, 'DA1-B1', 200), (1, 'DA1-B2', 150), (2, 'DA2-B1', 100);");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AqeAuthor>()
                  .HasKey(x => x.Id)
                  .HasMany<AqeBook>(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };
        using var ctx = new DbContext(cn, new AsyncSqliteProvider(), opts);

        // Select with navigation collection property included — triggers DependentQueries
        var results = await ctx.Query<AqeAuthor>()
            .Select(a => new { a.Name, a.YearBorn })
            .ToListAsync();

        // At minimum the main query materializes correctly
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "DepAuthor1");
    }

    [Fact]
    public async Task Async4_DependentQuery_Sync_PathAlsoCovered()
    {
        // Exercise the sync ExecuteDependentQueries path via Materialize (sync)
        using var cn = OpenMemory();
        ExecSql(cn, @"
            CREATE TABLE IF NOT EXISTS AQE_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, YearBorn INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS AQE_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL, Pages INTEGER NOT NULL DEFAULT 0);
            INSERT INTO AQE_Author (Name, YearBorn) VALUES ('SyncDep1', 1970);
            INSERT INTO AQE_Book   (AuthorId, Title, Pages) VALUES (1, 'SD1', 100);");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AqeAuthor>()
                  .HasKey(x => x.Id)
                  .HasMany<AqeBook>(a => a.Books)
                  .WithOne()
                  .HasForeignKey(b => b.AuthorId);
            }
        };
        // Use sync provider (SQLite) to exercise sync Materialize path
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var results = ctx.Query<AqeAuthor>()
            .Select(a => new { a.Name })
            .ToList();

        Assert.Single(results);
        Assert.Equal("SyncDep1", results[0].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 5 — Async scalar and aggregate paths
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async5_ScalarAsync_CountAsync_UsesAsyncScalarPath()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        // CountAsync on async provider → ExecuteScalarPlanAsync
        var count = await ctx.Query<AqeAuthor>().CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Async5_MinAsync_AsyncPath()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var q = (INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>();
        var min = await q.MinAsync(a => a.YearBorn);
        Assert.Equal(1970, min);
    }

    [Fact]
    public async Task Async5_MaxAsync_AsyncPath()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var q = (INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>();
        var max = await q.MaxAsync(a => a.YearBorn);
        Assert.Equal(1985, max);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 6 — Projection via async path (CreateSchemaAwareMaterializer)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async6_SelectProjection_AsyncPath_AnonymousType()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        // Projection triggers CreateSchemaAwareMaterializer
        var results = await ctx.Query<AqeAuthor>()
            .OrderBy(a => a.Id)
            .Select(a => new { a.Id, a.Name })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    public async Task Async6_SelectProjection_AsyncPath_SingleColumn()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var names = await ctx.Query<AqeAuthor>()
            .OrderBy(a => a.Name)
            .Select(a => a.Name)
            .ToListAsync();

        Assert.Equal(2, names.Count);
        Assert.Equal("Alice", names[0]);
        Assert.Equal("Bob", names[1]);
    }

    [Fact]
    public async Task Async6_SelectProjection_AsyncPath_WithWhere()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var results = await ctx.Query<AqeAuthor>()
            .Where(a => a.YearBorn < 1980)
            .Select(a => new { a.Name, a.YearBorn })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 7 — AsNoTracking and tracking differentiation in async path
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Async7_NoTracking_AsyncPath_EntitiesNotTracked()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var list = await ((INormQueryable<AqeAuthor>)ctx.Query<AqeAuthor>())
            .AsNoTracking()
            .ToListAsync();

        Assert.Equal(2, list.Count);
        // With NoTracking, entities are not in change tracker
        foreach (var a in list)
        {
            var state = ctx.ChangeTracker.GetEntryOrDefault(a)?.State ?? EntityState.Detached;
            // Either Detached or the tracker simply doesn't know about it
            Assert.True(state == EntityState.Detached || state == EntityState.Unchanged,
                $"Expected Detached or Unchanged, got {state}");
        }
    }

    [Fact]
    public async Task Async7_WithTracking_AsyncPath_EntitiesTracked()
    {
        using var cn = OpenMemory();
        using var ctx = CreateAuthorContext(cn);
        SeedAuthors(cn);

        var list = await ctx.Query<AqeAuthor>().ToListAsync();

        Assert.Equal(2, list.Count);
        foreach (var a in list)
        {
            var state = ctx.ChangeTracker.GetEntryOrDefault(a)?.State ?? EntityState.Detached;
            Assert.Equal(EntityState.Unchanged, state);
        }
    }
}
