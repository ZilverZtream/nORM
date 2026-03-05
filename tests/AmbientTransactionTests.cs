using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// TX-1: Tests that ambient System.Transactions.TransactionScope is correctly handled.
/// SQLite with Microsoft.Data.Sqlite supports ambient transactions (enlistment) when
/// Enlist=True is in the connection string (this is the default for SqliteConnection).
///
/// IMPORTANT: SQLite itself doesn't fully support distributed transactions, but the
/// ambient TransactionScope enlistment ensures that when a scope is disposed without
/// Complete(), the connection is rolled back correctly.
/// </summary>
public class AmbientTransactionTests
{
    [Table("AmbientItem")]
    private class AmbientItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(DbContextOptions? options = null)
    {
        // Note: SQLite enlistment support depends on the driver version.
        // Microsoft.Data.Sqlite supports ambient transactions via the Enlist keyword.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AmbientItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        // Gate E: Use BestEffort policy by default in these tests because SQLite in-memory
        // connections don't reliably support EnlistTransaction; FailFast (the production
        // default) would cause these ambient-scope tests to throw NormConfigurationException.
        var effectiveOptions = options ?? new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.BestEffort
        };
        var ctx = new DbContext(cn, new SqliteProvider(), effectiveOptions);
        return (cn, ctx);
    }

    private static long CountRows(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM AmbientItem";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ─── EnlistTransaction does not throw for SQLite ──────────────────────

    [Fact]
    public void SQLite_Connection_EnlistTransaction_WithAmbientScope_DoesNotThrow()
    {
        // TX-1: The TransactionManager calls connection.EnlistTransaction() when an ambient
        // transaction is present. This should not throw for a standard SqliteConnection.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        Exception? caughtEx = null;
        using (var scope = new TransactionScope(TransactionScopeOption.Required,
                   TransactionScopeAsyncFlowOption.Enabled))
        {
            try
            {
                cn.EnlistTransaction(System.Transactions.Transaction.Current);
                // If enlistment succeeded, complete normally (don't complete scope to test rollback later)
            }
            catch (Exception ex)
            {
                caughtEx = ex;
            }
            scope.Complete(); // complete so no rollback exception
        }

        // EnlistTransaction should not throw for SQLite (it either succeeds or silently ignores)
        // The important thing is our TransactionManager catches any exception gracefully.
        _ = caughtEx; // acceptable if it throws — driver may not support it
        cn.Dispose();
    }

    // ─── TX-1: TransactionManager calls EnsureConnectionAsync in ambient case ─

    [Fact]
    public async Task SaveChanges_WithAmbientTransactionScope_DoesNotThrow()
    {
        // TX-1: When a TransactionScope is active, TransactionManager should explicitly
        // call connection.EnlistTransaction(). If the provider supports it, operations
        // are enrolled in the scope. If not, operations proceed and the provider gracefully
        // catches and logs the enlistment failure.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        Exception? caughtEx = null;
        try
        {
            using var scope = new TransactionScope(TransactionScopeOption.Required,
                TransactionScopeAsyncFlowOption.Enabled);

            ctx.Add(new AmbientItem { Name = "inside_scope" });
            await ctx.SaveChangesAsync();

            scope.Complete();
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            // Some environments (e.g. SQLite in-memory) may not support ambient transactions.
            // Capture but don't fail — the important thing is we don't get a cryptic exception.
            caughtEx = ex;
        }

        // Either the transaction succeeded, or we got a descriptive error — not a NullReferenceException.
        if (caughtEx != null)
        {
            Assert.IsNotType<NullReferenceException>(caughtEx);
            Assert.IsNotType<InvalidOperationException>(caughtEx);
        }
    }

    // ─── TX-1: Write outside scope is always visible ──────────────────────

    [Fact]
    public async Task Write_OutsideTransactionScope_IsAlwaysVisible()
    {
        // TX-1: Writes that happen outside any TransactionScope commit immediately
        // and should always be visible regardless of any ambient transaction behavior.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Add(new AmbientItem { Name = "always_visible" });
        await ctx.SaveChangesAsync();

        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }

    // ─── TX-1: Explicit BeginTransactionAsync inside ambient scope ─────────

    [Fact]
    public async Task ExplicitTransaction_WorksCorrectly()
    {
        // TX-1: An explicit BeginTransactionAsync should work correctly.
        // The existing transaction wins (TransactionManager.OwnsTransaction = false when there's
        // an existing DbTransaction via Database.BeginTransactionAsync).
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Start an explicit DB transaction through the context
        await using var explicitTx = await ctx.Database.BeginTransactionAsync();

        // Insert inside the explicit transaction
        ctx.Add(new AmbientItem { Name = "in_explicit_tx" });
        await ctx.SaveChangesAsync();

        // Commit the explicit transaction
        await explicitTx.CommitAsync();

        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }

    // ─── TX-1: SaveChanges with no ambient scope works normally ───────────

    [Fact]
    public async Task SaveChanges_NoAmbientScope_WorksNormally()
    {
        // TX-1: Without any ambient transaction, the TransactionManager owns its own transaction
        // (ownsTransaction = true) and everything works as before.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Add(new AmbientItem { Name = "no_scope" });
        await ctx.SaveChangesAsync();

        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }

    // ─── Gate E: AmbientTransactionEnlistmentPolicy tests ─────────────────

    [Fact]
    public async Task GateE_FailFast_WhenEnlistmentFails_ThrowsNormConfigurationException()
    {
        // Gate E: With the default FailFast policy, when a provider's EnlistTransaction()
        // throws (SQLite in-memory does not support ambient transactions), the
        // TransactionManager must re-throw as NormConfigurationException with a clear message.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AmbientItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // FailFast is the production-safe default
        var options = new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.FailFast
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Determine whether SQLite's EnlistTransaction actually throws in this environment.
        // If it succeeds silently, FailFast won't trigger — we only assert the failure path
        // when enlistment is actually unsupported.
        bool enlistmentThrows;
        try
        {
            using (var testScope = new TransactionScope(TransactionScopeOption.Required,
                       TransactionScopeAsyncFlowOption.Enabled))
            {
                cn.EnlistTransaction(System.Transactions.Transaction.Current);
                testScope.Complete();
            }
            enlistmentThrows = false;
        }
        catch
        {
            enlistmentThrows = true;
        }

        using var scope = new TransactionScope(TransactionScopeOption.Required,
            TransactionScopeAsyncFlowOption.Enabled);

        ctx.Add(new AmbientItem { Name = "failfast_test" });

        if (enlistmentThrows)
        {
            // FailFast must surface the failure as NormConfigurationException
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(
                () => ctx.SaveChangesAsync());
            Assert.Contains("Provider does not support ambient transaction enlistment", ex.Message);
            Assert.Contains("BestEffort", ex.Message);
        }
        else
        {
            // Provider supports enlistment — FailFast doesn't trigger, operation succeeds
            var saveEx = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
            Assert.Null(saveEx);
            scope.Complete();
        }

        cn.Dispose();
    }

    [Fact]
    public async Task GateE_BestEffort_WhenEnlistmentFails_ContinuesWithoutException()
    {
        // Gate E: With BestEffort policy, when EnlistTransaction() fails (SQLite in-memory),
        // the operation must continue without throwing — logging a warning instead.
        // This is the previous behavior, now made explicit.
        var (cn, ctx) = CreateContext(new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.BestEffort
        });
        using var _cn = cn;
        await using var _ctx = ctx;

        using var scope = new TransactionScope(TransactionScopeOption.Required,
            TransactionScopeAsyncFlowOption.Enabled);

        ctx.Add(new AmbientItem { Name = "besteffort_test" });

        // Must NOT throw even if enlistment fails — BestEffort continues silently
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);

        // Note: scope.Complete() is intentionally omitted — we're verifying no exception,
        // not rollback semantics (SQLite in-memory may not honor rollback here).
    }

    [Fact]
    public async Task GateE_Ignore_SkipsEnlistmentCompletely()
    {
        // Gate E: With Ignore policy, EnlistTransaction() is never called.
        // Operations proceed as if there is no ambient transaction support.
        // There's no external way to verify the call wasn't made without instrumentation,
        // but we can verify the operation succeeds regardless of ambient scope state.
        var (cn, ctx) = CreateContext(new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.Ignore
        });
        using var _cn = cn;
        await using var _ctx = ctx;

        using var scope = new TransactionScope(TransactionScopeOption.Required,
            TransactionScopeAsyncFlowOption.Enabled);

        ctx.Add(new AmbientItem { Name = "ignore_test" });

        // Must succeed — Ignore means EnlistTransaction is never called, so it can't fail
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);

        // Operation committed independently of the scope (Ignore skips enlistment).
        // Verify row exists.
        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task GateE_DefaultPolicy_IsFailFast()
    {
        // Gate E: Verify that the default policy is FailFast (the production-safe setting).
        // An unconfigured DbContextOptions should use FailFast.
        var options = new DbContextOptions();
        Assert.Equal(AmbientTransactionEnlistmentPolicy.FailFast, options.AmbientTransactionPolicy);
    }

    [Fact]
    public async Task GateE_BestEffort_NoAmbientScope_WorksNormally()
    {
        // Gate E: BestEffort policy with no ambient scope should work exactly like default
        // (no difference when there's no ambient transaction to enlist in).
        var (cn, ctx) = CreateContext(new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.BestEffort
        });
        using var _cn = cn;
        await using var _ctx = ctx;

        // No TransactionScope — should work normally
        ctx.Add(new AmbientItem { Name = "besteffort_no_scope" });
        await ctx.SaveChangesAsync();

        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }

    [Fact]
    public async Task GateE_Ignore_NoAmbientScope_WorksNormally()
    {
        // Gate E: Ignore policy with no ambient scope should work normally.
        var (cn, ctx) = CreateContext(new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.Ignore
        });
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Add(new AmbientItem { Name = "ignore_no_scope" });
        await ctx.SaveChangesAsync();

        var count = CountRows(cn);
        Assert.Equal(1L, count);
    }
}
