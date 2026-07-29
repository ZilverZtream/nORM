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
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Relationship-fixup collection reconcile (severance / reparent) mutates in-memory FKs, entity states,
/// AND the load-time collection snapshot BEFORE the transaction opens. If a save that carries such a
/// reconcile then fails and rolls back, the snapshot mutation is not restored. This verifies the pending
/// disassociation/reparent still persists correctly on the next save rather than being silently dropped
/// (leaving the DB link intact = silent data loss).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesReconcileRollbackPersistenceTests
{
    [Table("RcParent")]
    public sealed class RcParent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<RcChild> Children { get; set; } = new();
    }

    [Table("RcChild")]
    public sealed class RcChild
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? ParentId { get; set; }   // nullable => optional relationship (severance nulls the FK)
        public string Tag { get; set; } = "";
    }

    private static SqliteConnection Open()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE RcParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE RcChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NULL, Tag TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContextOptions Opts(IDbCommandInterceptor? icept = null)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<RcParent>().HasKey(p => p.Id);
                mb.Entity<RcChild>().HasKey(c => c.Id);
                mb.Entity<RcParent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => (object)c.ParentId!, p => (object)p.Id);
            }
        };
        if (icept != null) opts.CommandInterceptors.Add(icept);
        return opts;
    }

    private static int? FkOf(SqliteConnection cn, string tag)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT ParentId FROM RcChild WHERE Tag = '{tag}'";
        var v = cmd.ExecuteScalar();
        return v == null || v is DBNull ? (int?)null : Convert.ToInt32(v);
    }

    /// <summary>Throws a transient failure exactly once on the first UPDATE to RcChild.</summary>
    private sealed class ThrowOnceOnChildUpdate : IDbCommandInterceptor
    {
        private int _fired;
        public int FireCount => _fired;
        private bool Trip(DbCommand c) =>
            c.CommandText.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
            && c.CommandText.Contains("RcChild", StringComparison.Ordinal)
            && Interlocked.Exchange(ref _fired, 1) == 0;

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
        {
            if (Trip(c)) throw new SqliteException("Injected transient lock", 6);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }
        public Task NonQueryExecutedAsync(DbCommand c, DbContext ctx, int r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext ctx, object? r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        public Task ReaderExecutedAsync(DbCommand c, DbContext ctx, DbDataReader r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand c, DbContext ctx, Exception ex, CancellationToken ct) => Task.CompletedTask;
    }

    // ============================================================================
    // Severance survives a rolled-back save: remove a child from a loaded collection, the UPDATE that
    // nulls its FK fails once, then a manual re-save must still null the FK (not silently re-link).
    // ============================================================================
    [Fact]
    public async Task Severance_survives_rolled_back_save_and_reapplies_on_resave()
    {
        using var cn = Open();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText =
                "INSERT INTO RcParent (Name) VALUES ('P');" +
                "INSERT INTO RcChild (ParentId, Tag) VALUES (1, 'c1'), (1, 'c2');";
            seed.ExecuteNonQuery();
        }
        var icept = new ThrowOnceOnChildUpdate();
        await using var ctx = new DbContext(cn, new SqliteProvider(), Opts(icept));

        var parent = await ctx.Query<RcParent>().Include(p => p.Children).FirstAsync();
        Assert.Equal(2, parent.Children.Count);

        // Sever c2 from the loaded collection.
        var c2 = parent.Children.First(c => c.Tag == "c2");
        parent.Children.Remove(c2);

        // First save fails on the child UPDATE -> rollback (snapshot mutation NOT restored).
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());
        Assert.Equal(1, icept.FireCount);
        Assert.Equal(1, FkOf(cn, "c2"));   // still linked in the DB after rollback

        // Re-save: the pending severance must still be persisted.
        await ctx.SaveChangesAsync();
        Assert.Null(FkOf(cn, "c2"));       // FK nulled == severance applied (NOT silently dropped)
        Assert.Equal(1, FkOf(cn, "c1"));   // c1 untouched
    }

    // ============================================================================
    // Reparent survives a rolled-back save: move a child from P1's loaded collection into P2's, the
    // UPDATE fails once, then a re-save must still repoint the FK to P2.
    // ============================================================================
    [Fact]
    public async Task Reparent_survives_rolled_back_save_and_reapplies_on_resave()
    {
        using var cn = Open();
        using (var seed = cn.CreateCommand())
        {
            seed.CommandText =
                "INSERT INTO RcParent (Name) VALUES ('P1'), ('P2');" +
                "INSERT INTO RcChild (ParentId, Tag) VALUES (1, 'x');";
            seed.ExecuteNonQuery();
        }
        var icept = new ThrowOnceOnChildUpdate();
        await using var ctx = new DbContext(cn, new SqliteProvider(), Opts(icept));

        var parents = await ctx.Query<RcParent>().Include(p => p.Children).OrderBy(p => p.Id).ToListAsync();
        var p1 = parents[0];
        var p2 = parents[1];
        var child = p1.Children.Single();

        // Move the child from P1 to P2.
        p1.Children.Remove(child);
        p2.Children.Add(child);

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());
        Assert.Equal(1, icept.FireCount);
        Assert.Equal(1, FkOf(cn, "x"));    // still on P1 after rollback

        await ctx.SaveChangesAsync();
        Assert.Equal(p2.Id, FkOf(cn, "x")); // reparented to P2 (NOT silently reverted/dropped)
    }
}
