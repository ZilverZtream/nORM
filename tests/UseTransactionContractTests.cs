using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins Database.UseTransaction — enlisting the context in an externally-managed transaction (the raw-ADO /
/// Dapper interop path). nORM's SaveChanges runs inside the caller's transaction, committing/rolling back
/// through the returned wrapper works, and — critically — nORM NEVER disposes the caller's transaction, so
/// the caller can still commit it after disposing the wrapper. Passing null clears; a second call while a
/// transaction is active fails loud; and it is disallowed under strict provider mobility.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class UseTransactionContractTests
{
    [Table("UtWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Make(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE UtWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static int RowCount(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM UtWidget";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task SaveChanges_runs_inside_the_external_transaction_and_rollback_discards_it()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn);

        var conn = ctx.Database.GetDbConnection();
        using var external = conn.BeginTransaction();
        var wrapper = ctx.Database.UseTransaction(external);
        Assert.NotNull(wrapper);

        ctx.Add(new Widget { Id = 1, Name = "a" });
        await ctx.SaveChangesAsync();      // enlisted in the external transaction

        wrapper!.Rollback();               // rolls back the external transaction (does not dispose it)
        Assert.Equal(0, RowCount(conn));   // the nORM write was undone
    }

    [Fact]
    public async Task Wrapper_commit_persists_and_does_not_dispose_the_callers_transaction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn);

        var conn = ctx.Database.GetDbConnection();
        using var external = conn.BeginTransaction();
        var wrapper = ctx.Database.UseTransaction(external);

        ctx.Add(new Widget { Id = 1, Name = "a" });
        await ctx.SaveChangesAsync();

        wrapper!.Commit();
        Assert.Equal(1, RowCount(conn));

        // nORM must not have disposed the caller's transaction — disposing it here is the caller's job and
        // must not throw ObjectDisposedException.
        external.Dispose();
    }

    [Fact]
    public async Task Disposing_the_wrapper_does_not_dispose_the_callers_transaction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn);

        var conn = ctx.Database.GetDbConnection();
        using var external = conn.BeginTransaction();
        var wrapper = ctx.Database.UseTransaction(external);

        ctx.Add(new Widget { Id = 1, Name = "a" });
        await ctx.SaveChangesAsync();

        wrapper!.Dispose();       // non-owning: releases the context reference, does NOT dispose external

        // The caller still owns the transaction and can commit it — this throws if nORM wrongly disposed it.
        external.Commit();
        Assert.Equal(1, RowCount(conn));
    }

    [Fact]
    public void UseTransaction_null_clears_the_current_transaction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn);

        var conn = ctx.Database.GetDbConnection();
        using var external = conn.BeginTransaction();
        ctx.Database.UseTransaction(external);
        Assert.NotNull(ctx.Database.CurrentContextTransaction);

        Assert.Null(ctx.Database.UseTransaction(null));
        Assert.Null(ctx.Database.CurrentContextTransaction);
        external.Rollback();   // caller still owns it
    }

    [Fact]
    public void UseTransaction_throws_when_a_transaction_is_already_active()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn);

        var conn = ctx.Database.GetDbConnection();
        using var external = conn.BeginTransaction();
        ctx.Database.UseTransaction(external);   // now active

        // A second enlist while one is active fails loud (SQLite has no nested transactions anyway).
        Assert.Throws<NormUsageException>(() => ctx.Database.UseTransaction(external));
        external.Rollback();
    }

    [Fact]
    public void UseTransaction_is_blocked_under_strict_provider_mobility()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "CREATE TABLE UtWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);"; cmd.ExecuteNonQuery(); }
        var opts = new DbContextOptions().UseStrictProviderMobility();
        opts.OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        using var external = cn.BeginTransaction();
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Database.UseTransaction(external));
    }
}
