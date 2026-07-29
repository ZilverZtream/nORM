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
/// Parent-child graph inserts where a mid-graph failure rolls back the principal's DB-generated key. The
/// children's FK values were propagated from that key; on retry the FK correctness relies entirely on
/// re-propagation. Also probes a graph failure with a manual re-save.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesGraphKeyRollbackRepropagationTests
{
    [Table("GParent")]
    public sealed class GParent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<GChild> Children { get; set; } = new();
    }

    [Table("GChild")]
    public sealed class GChild
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = "";
    }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static int Count(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static List<(int Id, int ParentId, string Tag)> ChildRows(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, ParentId, Tag FROM GChild ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var list = new List<(int, int, string)>();
        while (r.Read()) list.Add((r.GetInt32(0), r.GetInt32(1), r.GetString(2)));
        return list;
    }

    private static DbContextOptions GraphOpts(RetryPolicy? retry = null, IDbCommandInterceptor? icept = null)
    {
        var opts = new DbContextOptions
        {
            RetryPolicy = retry,
            OnModelCreating = mb =>
            {
                mb.Entity<GParent>().HasKey(p => p.Id);
                mb.Entity<GChild>().HasKey(c => c.Id);
                mb.Entity<GParent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        if (icept != null) opts.CommandInterceptors.Add(icept);
        return opts;
    }

    private sealed class ThrowOnceOnTable : IDbCommandInterceptor
    {
        private readonly string _needle;
        private int _fired;
        public int FireCount => _fired;
        public ThrowOnceOnTable(string needle) => _needle = needle;
        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
        {
            if (c.CommandText.Contains(_needle, StringComparison.Ordinal) && Interlocked.Exchange(ref _fired, 1) == 0)
                throw new SqliteException("Injected transient lock", 6);
            return Task.FromResult(InterceptionResult<int>.Continue());
        }
        public Task NonQueryExecutedAsync(DbCommand c, DbContext ctx, int r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct) => Task.FromResult(InterceptionResult<object?>.Continue());
        public Task ScalarExecutedAsync(DbCommand c, DbContext ctx, object? r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand c, DbContext ctx, CancellationToken ct)
        {
            if (c.CommandText.Contains(_needle, StringComparison.Ordinal) && Interlocked.Exchange(ref _fired, 1) == 0)
                throw new SqliteException("Injected transient lock", 6);
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }
        public Task ReaderExecutedAsync(DbCommand c, DbContext ctx, DbDataReader r, TimeSpan d, CancellationToken ct) => Task.CompletedTask;
        public Task CommandFailedAsync(DbCommand c, DbContext ctx, Exception ex, CancellationToken ct) => Task.CompletedTask;
    }

    // ============================================================================
    // Parent key rolled back mid-graph, then RETRIED: children FK must re-link to the parent's
    // (re-stamped) key via re-propagation. Fail = orphaned/wrong FK (silent corruption).
    // ============================================================================
    [Fact]
    public async Task Graph_child_insert_fails_once_then_retry_relinks_children_to_parent()
    {
        using var cn = Open(
            "CREATE TABLE GParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE GChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        var retry = new RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1), ShouldRetry = ex => ex is SqliteException };
        var icept = new ThrowOnceOnTable("GChild");
        await using var ctx = new DbContext(cn, new SqliteProvider(), GraphOpts(retry, icept));

        var parent = new GParent { Name = "P" };
        parent.Children.Add(new GChild { Tag = "c1" });
        parent.Children.Add(new GChild { Tag = "c2" });
        parent.Children.Add(new GChild { Tag = "c3" });
        ctx.Add(parent);

        await ctx.SaveChangesAsync();

        Assert.Equal(1, icept.FireCount);
        Assert.Equal(1, Count(cn, "GParent"));
        Assert.Equal(3, Count(cn, "GChild"));
        Assert.True(parent.Id > 0);

        var children = ChildRows(cn);
        Assert.All(children, c => Assert.Equal(parent.Id, c.ParentId));   // every child links to the FINAL parent key
        Assert.Equal(new[] { "c1", "c2", "c3" }, children.Select(c => c.Tag).OrderBy(x => x).ToArray());

        // In-memory graph must also be consistent.
        Assert.All(parent.Children, c => Assert.Equal(parent.Id, c.ParentId));
    }

    // ============================================================================
    // Manual re-save variant (no retry policy): a real constraint failure on a child, caught by the
    // caller, fixed, and re-saved on the same tracker. The graph must persist exactly once, correctly linked.
    // ============================================================================
    [Fact]
    public async Task Graph_child_constraint_failure_then_manual_resave_persists_linked_graph()
    {
        using var cn = Open(
            "CREATE TABLE GParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE GChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL CHECK (Tag <> 'BAD'));");
        await using var ctx = new DbContext(cn, new SqliteProvider(), GraphOpts());

        var parent = new GParent { Name = "P" };
        parent.Children.Add(new GChild { Tag = "ok1" });
        parent.Children.Add(new GChild { Tag = "BAD" });   // violates CHECK
        parent.Children.Add(new GChild { Tag = "ok3" });
        ctx.Add(parent);

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());
        Assert.Equal(0, Count(cn, "GParent"));
        Assert.Equal(0, Count(cn, "GChild"));

        // Fix the offending child and re-save on the same context.
        parent.Children[1].Tag = "ok2";
        await ctx.SaveChangesAsync();

        Assert.Equal(1, Count(cn, "GParent"));
        Assert.Equal(3, Count(cn, "GChild"));
        var children = ChildRows(cn);
        Assert.All(children, c => Assert.Equal(parent.Id, c.ParentId));
        Assert.Equal(new[] { "ok1", "ok2", "ok3" }, children.Select(c => c.Tag).OrderBy(x => x).ToArray());
    }

    // ============================================================================
    // Caller-owned tx: insert the graph (parent + children), ROLL BACK, then re-save.
    // Children FK must re-link after the rollback + re-insert (parent key path).
    // ============================================================================
    [Fact]
    public async Task Graph_callerOwnedTx_rolled_back_then_resaved_relinks_children()
    {
        using var cn = Open(
            "CREATE TABLE GParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE GChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), GraphOpts());

        var parent = new GParent { Name = "P" };
        parent.Children.Add(new GChild { Tag = "c1" });
        parent.Children.Add(new GChild { Tag = "c2" });
        ctx.Add(parent);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            Assert.Equal(1, Count(cn, "GParent"));
            Assert.Equal(2, Count(cn, "GChild"));
            await tx.RollbackAsync();
        }

        Assert.Equal(0, Count(cn, "GParent"));
        Assert.Equal(0, Count(cn, "GChild"));

        await ctx.SaveChangesAsync();   // re-insert whole graph

        Assert.Equal(1, Count(cn, "GParent"));
        Assert.Equal(2, Count(cn, "GChild"));
        var children = ChildRows(cn);
        Assert.All(children, c => Assert.Equal(parent.Id, c.ParentId));
    }
}
