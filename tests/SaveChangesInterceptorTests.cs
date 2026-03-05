using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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

public class SaveChangesInterceptorTests
{
    public class User
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime? ModifiedAt { get; set; }
    }

    private class AuditInterceptor : ISaveChangesInterceptor
    {
        public int SavingCalls { get; private set; }
        public int SavedCalls { get; private set; }
        public int LastResult { get; private set; }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
        {
            SavingCalls++;
            foreach (var entry in entries)
            {
                if (entry.Entity is User u)
                {
                    if (entry.State == EntityState.Added)
                        u.CreatedAt = DateTime.UtcNow;
                    if (entry.State == EntityState.Modified)
                        u.ModifiedAt = DateTime.UtcNow;
                }
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
        {
            SavedCalls++;
            LastResult = result;
            return Task.CompletedTask;
        }
    }

    // ── S4-1: AuditLog entity for interceptor expansion tests ─────────────────

    [Table("AuditLog")]
    private class AuditLog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Action { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    // ── S4-1: Interceptor that adds a new AuditLog entity during SavingChanges ─

    private class AddAuditLogInterceptor : ISaveChangesInterceptor
    {
        private readonly string _action;
        private readonly int _count;

        public AddAuditLogInterceptor(string action, int count = 1)
        {
            _action = action;
            _count = count;
        }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
        {
            for (int i = 0; i < _count; i++)
            {
                context.Add(new AuditLog { Action = _action, Timestamp = DateTime.UtcNow });
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    // ── S4-1: Interceptor that modifies an existing tracked entity during SavingChanges ─

    private class ModifyExistingEntityInterceptor : ISaveChangesInterceptor
    {
        private readonly string _newName;

        public ModifyExistingEntityInterceptor(string newName)
        {
            _newName = newName;
        }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
        {
            foreach (var entry in entries)
            {
                if (entry.Entity is User u && entry.State == EntityState.Added)
                {
                    u.Name = _newName;
                }
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    // ── S4-1: No-op interceptor for regression guard ──────────────────────────

    private class NoOpInterceptor : ISaveChangesInterceptor
    {
        public bool WasCalled { get; private set; }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken cancellationToken)
        {
            WasCalled = true;
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    // ── S4-1: Test helper ─────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateContextWithSchema(
        string extraTableDdl = "", ISaveChangesInterceptor? interceptor = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE User(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', CreatedAt TEXT NOT NULL DEFAULT '', ModifiedAt TEXT);" +
                "CREATE TABLE AuditLog(Id INTEGER PRIMARY KEY AUTOINCREMENT, Action TEXT NOT NULL DEFAULT '', Timestamp TEXT NOT NULL DEFAULT '');" +
                extraTableDdl;
            cmd.ExecuteNonQuery();
        }

        var options = new DbContextOptions();
        if (interceptor != null)
            options.SaveChangesInterceptors.Add(interceptor);

        var ctx = new DbContext(cn, new SqliteProvider(), options);
        return (cn, ctx);
    }

    // ── S4-1 Tests ────────────────────────────────────────────────────────────

    /// <summary>
    /// S4-1: An interceptor that calls context.Add() during SavingChangesAsync
    /// must have its new entity included in the same SaveChanges operation.
    /// </summary>
    [Fact]
    public async Task SavingChanges_InterceptorAddsNewEntity_EntityIsPersisted()
    {
        var interceptor = new AddAuditLogInterceptor("save");
        var (cn, ctx) = CreateContextWithSchema(interceptor: interceptor);
        await using var _ = ctx;

        // Add a user — the interceptor will also add an AuditLog
        var user = new User { Name = "Alice", CreatedAt = DateTime.UtcNow };
        ctx.Add(user);
        var result = await ctx.SaveChangesAsync();

        // SaveChanges should have persisted both User and AuditLog
        Assert.True(result >= 2, $"Expected at least 2 rows saved (User + AuditLog), got {result}");

        // Verify AuditLog was persisted to the database
        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM AuditLog WHERE Action = 'save'";
        var count = (long)(checkCmd.ExecuteScalar()!);
        Assert.Equal(1, count);
    }

    /// <summary>
    /// S4-1: An interceptor that modifies a tracked entity's property during SavingChangesAsync
    /// must have that modification included in the persisted row.
    /// </summary>
    [Fact]
    public async Task SavingChanges_InterceptorModifiesEntry_ModificationIsPersisted()
    {
        var interceptor = new ModifyExistingEntityInterceptor("InterceptorModified");
        var (cn, ctx) = CreateContextWithSchema(interceptor: interceptor);
        await using var _ = ctx;

        var user = new User { Name = "Original", CreatedAt = DateTime.UtcNow };
        ctx.Add(user);
        await ctx.SaveChangesAsync();

        // Verify the interceptor's modification was persisted
        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT Name FROM User WHERE Id = 1";
        var name = (string)(checkCmd.ExecuteScalar()!);
        Assert.Equal("InterceptorModified", name);
    }

    /// <summary>
    /// S4-1: An interceptor that does nothing must not break normal save behavior.
    /// Regression guard.
    /// </summary>
    [Fact]
    public async Task SavingChanges_InterceptorDoesNothing_NormalSaveProceeds()
    {
        var interceptor = new NoOpInterceptor();
        var (cn, ctx) = CreateContextWithSchema(interceptor: interceptor);
        await using var _ = ctx;

        var user = new User { Name = "Alice", CreatedAt = DateTime.UtcNow };
        ctx.Add(user);
        var result = await ctx.SaveChangesAsync();

        Assert.Equal(1, result);
        Assert.True(interceptor.WasCalled);

        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM User";
        var count = (long)(checkCmd.ExecuteScalar()!);
        Assert.Equal(1, count);
    }

    /// <summary>
    /// S4-1: An interceptor that adds multiple entities during SavingChangesAsync
    /// must have all of them persisted.
    /// </summary>
    [Fact]
    public async Task SavingChanges_InterceptorAddsMultipleEntities_AllPersisted()
    {
        var interceptor = new AddAuditLogInterceptor("multi-save", count: 3);
        var (cn, ctx) = CreateContextWithSchema(interceptor: interceptor);
        await using var _ = ctx;

        var user = new User { Name = "Bob", CreatedAt = DateTime.UtcNow };
        ctx.Add(user);
        var result = await ctx.SaveChangesAsync();

        // User (1) + 3 AuditLog entries = 4 rows minimum
        Assert.True(result >= 4, $"Expected at least 4 rows saved (User + 3 AuditLog entries), got {result}");

        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM AuditLog WHERE Action = 'multi-save'";
        var count = (long)(checkCmd.ExecuteScalar()!);
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task Interceptor_runs_before_and_after_saving()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, CreatedAt TEXT, ModifiedAt TEXT);";
            cmd.ExecuteNonQuery();
        }

        var interceptor = new AuditInterceptor();
        var options = new DbContextOptions();
        options.SaveChangesInterceptors.Add(interceptor);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var user = new User { Name = "Alice" };
        ctx.Add(user);
        var result = await ctx.SaveChangesAsync();

        Assert.Equal(1, result);
        Assert.NotEqual(default, user.CreatedAt);
        Assert.Equal(1, interceptor.SavingCalls);
        Assert.Equal(1, interceptor.SavedCalls);
        Assert.Equal(1, interceptor.LastResult);

        user.Name = "Bob";
        var entry = ctx.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
        await ctx.SaveChangesAsync();
        Assert.NotNull(user.ModifiedAt);
        Assert.Equal(2, interceptor.SavingCalls);
        Assert.Equal(2, interceptor.SavedCalls);
    }
}
