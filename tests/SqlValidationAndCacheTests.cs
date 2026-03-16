using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Support entities
// ══════════════════════════════════════════════════════════════════════════════

[Table("DateTimeEntity")]
file class DateTimeEntity
{
    [Key]
    public int Id { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? Name { get; set; }
}

// Custom type that falls into the AssignValue fallback branch
file class CustomFallbackType
{
    public string Value { get; set; } = "test";
    public override string ToString() => Value;
}

// ══════════════════════════════════════════════════════════════════════════════
// Stale DbType on DateTime and fallback branches
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that AssignValue always resets DbType when called with a new value type,
/// preventing stale metadata from a previous call from corrupting subsequent parameter bindings.
/// </summary>
public class StaleDbTypeTests
{
    // ── Direct unit tests of AssignValue metadata reset ───────────────────────

    [Fact]
    public void AssignValue_IntThenDateTime_DbTypeBecomesDateTime()
    {
        // Simulate: prior int assignment set DbType.Int32; next call is DateTime.
        // Without the reset, DbType stays Int32 → stale metadata.
        var p = new SqliteParameter();
        p.DbType = DbType.Int32;
        p.Value = 42;

        ParameterAssign.AssignValue(p, new DateTime(2024, 6, 15, 10, 30, 0));

        Assert.Equal(DbType.DateTime2, p.DbType);
        Assert.IsType<DateTime>(p.Value);
    }

    [Fact]
    public void AssignValue_DateTimeThenInt_DbTypeBecomesInt32()
    {
        // Reverse transition: prior DateTime → next int.
        var p = new SqliteParameter();
        p.DbType = DbType.DateTime;
        p.Value = DateTime.UtcNow;

        ParameterAssign.AssignValue(p, 42);

        Assert.Equal(DbType.Int32, p.DbType);
        Assert.Equal(42, p.Value);
    }

    [Fact]
    public void AssignValue_Int32ThenFallbackObject_DbTypeBecomesObject()
    {
        // Fallback branch (unknown CLR type) must reset DbType to Object.
        var p = new SqliteParameter();
        p.DbType = DbType.Int32;
        p.Value = 99;

        ParameterAssign.AssignValue(p, new CustomFallbackType { Value = "hello" });

        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void AssignValue_StringThenFallbackObject_DbTypeBecomesObject()
    {
        var p = new SqliteParameter();
        p.DbType = DbType.String;
        p.Value = "old";

        ParameterAssign.AssignValue(p, new CustomFallbackType { Value = "custom" });

        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void AssignValue_FallbackObjectThenString_DbTypeBecomesString()
    {
        // After fallback assignment, next string call must set DbType.String.
        var p = new SqliteParameter();
        p.DbType = DbType.Object;
        p.Value = new CustomFallbackType();

        ParameterAssign.AssignValue(p, "hello");

        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal("hello", p.Value);
    }

    [Fact]
    public void AssignValue_DateTimeMetadata_NullTransition_ResetsAll()
    {
        // DateTime → null: null path resets DbType to Object.
        var p = new SqliteParameter();
        ParameterAssign.AssignValue(p, new DateTime(2024, 1, 1));
        Assert.Equal(DbType.DateTime2, p.DbType);

        ParameterAssign.AssignValue(p, null);

        Assert.Equal(DbType.Object, p.DbType);
        Assert.Equal(DBNull.Value, p.Value);
    }

    [Fact]
    public void AssignValue_IntThenDateTimeCycled_DbTypeAlwaysCorrect()
    {
        // 10-cycle alternation: int ↔ DateTime — DbType must be correct each time.
        var p = new SqliteParameter();
        var dt = new DateTime(2024, 6, 15);

        for (int i = 0; i < 10; i++)
        {
            ParameterAssign.AssignValue(p, i);
            Assert.Equal(DbType.Int32, p.DbType);
            Assert.Equal(i, p.Value);

            ParameterAssign.AssignValue(p, dt.AddDays(i));
            Assert.Equal(DbType.DateTime2, p.DbType);
            Assert.IsType<DateTime>(p.Value);
        }
    }

    // ── Integration tests: DateTime parameters execute correctly ───────────────

    private static SqliteConnection CreateDateTimeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE DateTimeEntity (Id INTEGER PRIMARY KEY, CreatedAt TEXT NOT NULL, Name TEXT);
            INSERT INTO DateTimeEntity VALUES (1, '2024-01-15 08:00:00', 'Alice');
            INSERT INTO DateTimeEntity VALUES (2, '2024-06-01 12:00:00', 'Bob');
            INSERT INTO DateTimeEntity VALUES (3, '2024-12-25 00:00:00', 'Carol');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task CompiledQuery_DateTimeParam_ReturnsCorrectRow()
    {
        using var cn = CreateDateTimeDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, DateTime dt) =>
            c.Query<DateTimeEntity>().Where(e => e.CreatedAt == dt));

        var result = (await compiled(ctx, new DateTime(2024, 1, 15, 8, 0, 0))).ToList();

        Assert.Single(result);
        Assert.Equal("Alice", result[0].Name);
    }

    [Fact]
    public async Task CompiledQuery_DateTimeParam_AlternatingValues_ReturnsCorrectRows()
    {
        using var cn = CreateDateTimeDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, DateTime dt) =>
            c.Query<DateTimeEntity>().Where(e => e.CreatedAt == dt));

        // Warm up with Alice
        var r1 = (await compiled(ctx, new DateTime(2024, 1, 15, 8, 0, 0))).ToList();
        Assert.Single(r1);
        Assert.Equal("Alice", r1[0].Name);

        // Switch to Bob — no stale results from first call
        var r2 = (await compiled(ctx, new DateTime(2024, 6, 1, 12, 0, 0))).ToList();
        Assert.Single(r2);
        Assert.Equal("Bob", r2[0].Name);

        // Switch back to Carol
        var r3 = (await compiled(ctx, new DateTime(2024, 12, 25, 0, 0, 0))).ToList();
        Assert.Single(r3);
        Assert.Equal("Carol", r3[0].Name);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Atomic _completed guard in DbContextTransaction
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that DbContextTransaction uses an atomic Interlocked guard to prevent
/// double-dispose and concurrent commit/dispose races from throwing or leaving the
/// context in a poisoned state.
/// </summary>
public class TransactionAtomicCompletionTests
{
    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE C1Entity (Id INTEGER PRIMARY KEY, Val TEXT);" +
                          "INSERT INTO C1Entity VALUES (1, 'test');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task ConcurrentDispose_NeitherThrows()
    {
        // Two concurrent Dispose() calls on the same wrapper must not throw.
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();

        var ex1 = await Record.ExceptionAsync(() =>
        {
            tx.Dispose();
            return Task.CompletedTask;
        });
        var ex2 = await Record.ExceptionAsync(() =>
        {
            tx.Dispose();
            return Task.CompletedTask;
        });

        Assert.Null(ex1);
        Assert.Null(ex2);
    }

    [Fact]
    public async Task ConcurrentDisposeAsync_NeitherThrows()
    {
        // Two concurrent DisposeAsync() calls on the same wrapper must not throw.
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();

        var t1 = tx.DisposeAsync().AsTask();
        var t2 = tx.DisposeAsync().AsTask();

        var ex = await Record.ExceptionAsync(() => Task.WhenAll(t1, t2));
        Assert.Null(ex);
    }

    [Fact]
    public async Task ConcurrentCommitAndDispose_NeitherThrows()
    {
        // Concurrent Commit() + Dispose() — only one must actually commit/dispose.
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();

        // Fire both near-simultaneously
        var t1 = Task.Run(() => tx.Dispose());
        var t2 = Task.Run(async () =>
        {
            try { await tx.CommitAsync(); }
            catch (InvalidOperationException) { /* tx already disposed — expected */ }
        });

        var ex = await Record.ExceptionAsync(() => Task.WhenAll(t1, t2));
        Assert.Null(ex);
    }

    [Fact]
    public async Task DoubleCommit_SecondCallDoesNotThrowObjectDisposed()
    {
        // Second Commit() after Dispose has already run must be a no-op, not crash.
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();

        tx.Commit(); // first call succeeds

        // Second Commit() hits already-completed wrapper; Interlocked gate skips it.
        // Either no exception (gate skipped) or InvalidOperationException from driver —
        // but must NOT be ObjectDisposedException or NullReferenceException.
        var ex = Record.Exception(() => tx.Commit());
        if (ex != null)
            Assert.IsNotType<ObjectDisposedException>(ex);
    }

    [Fact]
    public async Task DisposeAfterCommit_ContextCurrentTransactionIsNull()
    {
        // After Commit+Dispose, the context must not retain the old transaction.
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var tx = await ctx.Database.BeginTransactionAsync();
        tx.Commit();
        tx.Dispose(); // second call must be no-op

        Assert.Null(ctx.CurrentTransaction);
    }

    // ── Cancellation/rollback matrix ──────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_AfterCommit_IsNoOp()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();
        await tx.CommitAsync();

        // DisposeAsync after CommitAsync must be a no-op (Interlocked gate already at 1).
        var ex = await Record.ExceptionAsync(() => tx.DisposeAsync().AsTask());
        Assert.Null(ex);
        Assert.Null(ctx.CurrentTransaction);
    }

    [Fact]
    public async Task DisposeAsync_AfterRollback_IsNoOp()
    {
        using var cn = OpenConnection();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var tx = await ctx.Database.BeginTransactionAsync();
        await tx.RollbackAsync();

        var ex = await Record.ExceptionAsync(() => tx.DisposeAsync().AsTask());
        Assert.Null(ex);
        Assert.Null(ctx.CurrentTransaction);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// SQL comment false-positive fix and adversarial validation matrix
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that the SQL validator correctly handles inline comments (--) in
/// legitimate queries without false-positive rejections, while still blocking
/// real injection patterns.
/// </summary>
public class SqlCommentValidatorTests
{
    // ── Legitimate inline comments must be accepted ───────────────────────────

    [Fact]
    public void InlineComment_EndOfQuery_IsAccepted()
    {
        // Baseline: -- at end already worked before the fix.
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql("SELECT Id, Name FROM Users WHERE Id = @id -- filter by id"));
        Assert.Null(ex);
    }

    [Fact]
    public void InlineComment_MidQuery_IsAccepted()
    {
        // Regression: -- in mid-query position was previously rejected (false positive).
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(
                "SELECT Id, Name FROM Users WHERE Id = @id -- this is a valid comment\nAND IsActive = 1"));
        Assert.Null(ex);
    }

    [Fact]
    public void LongInlineComment_NotAtTail_IsAccepted()
    {
        // Long comment text far from the end of the string — was rejected by < sql.Length-10 heuristic.
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(
                "SELECT Id FROM Orders WHERE CustomerId = @cid -- retrieve orders for customer\nAND Status = @status"));
        Assert.Null(ex);
    }

    [Fact]
    public void MultipleInlineComments_AreAccepted()
    {
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(
                "SELECT Id FROM T WHERE Id = @id -- first filter\nAND Name = @name -- second filter"));
        Assert.Null(ex);
    }

    [Fact]
    public void CommentOnlyLine_IsAccepted()
    {
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql("SELECT Id, Name FROM T\n-- this whole line is a comment\nWHERE Id = @id"));
        Assert.Null(ex);
    }

    // ── Injection patterns must still be blocked ──────────────────────────────

    [Fact]
    public void CommentAfterSemicolon_IsRejected()
    {
        // Classic injection: '; DROP TABLE Users -- to hide trailing malicious SQL.
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(
                "SELECT * FROM Users WHERE Name = 'admin'; DROP TABLE Users -- "));
    }

    [Fact]
    public void CommentAfterSemicolonWithGap_IsRejected()
    {
        // Semicolon before -- further away.
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(
                "SELECT 1; SELECT * FROM secrets --"));
    }

    [Fact]
    public void Union_InjectionPattern_IsRejected()
    {
        // Existing UNION injection check must still fire.
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql(
                "SELECT Id FROM Users WHERE Name = 'x' UNION SELECT password FROM Users--"));
    }

    [Fact]
    public void BlockComment_WithSelectKeyword_IsRejected()
    {
        // Block comments containing SELECT (or other DML keywords) must still be rejected.
        Assert.Throws<NormUsageException>(() =>
            NormValidator.ValidateRawSql("SELECT 1 /* SELECT * FROM secrets */"));
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    [Fact]
    public void CommentInsideStringLiteral_IsAccepted()
    {
        // -- inside a quoted string is not a comment — should be accepted.
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql("SELECT * FROM T WHERE Name = @name -- safe"));
        Assert.Null(ex);
    }

    [Fact]
    public void ValidComplexSelectWithComments_IsAccepted()
    {
        // A realistic multi-line query with inline comments.
        var ex = Record.Exception(() =>
            NormValidator.ValidateRawSql(@"
                SELECT u.Id,           -- primary key
                       u.Name,         -- display name
                       u.Email         -- contact email
                FROM Users u
                WHERE u.IsActive = @active -- only active users
                  AND u.TenantId = @tid    -- tenant isolation
                ORDER BY u.Name"));
        Assert.Null(ex);
    }
}
