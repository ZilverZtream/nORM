using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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

//<summary>
//Verifies that property mutations made to previously-Unchanged entities
//by SavingChangesAsync interceptors are picked up and persisted to the database.
//Root cause: DetectAllChanges() was not re-run after SavingChangesAsync interceptors,
//so interceptor-induced property changes on Unchanged entities were silently dropped.
//</summary>
public class InterceptorMutationTests
{
    [Table("ImuItem")]
    private class ImuItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? AuditStamp { get; set; }
    }

 // ── Interceptor that mutates a previously-Unchanged entity ────────────────

    private sealed class AuditStampInterceptor : ISaveChangesInterceptor
    {
        private readonly string _stamp;
        private readonly ImuItem _targetEntity;

        public AuditStampInterceptor(ImuItem target, string stamp)
        {
            _targetEntity = target;
            _stamp = stamp;
        }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            CancellationToken cancellationToken)
        {
 // Mutate a property on the entity. The entity was previously
 // Unchanged in the tracker. Without re-running DetectAllChanges after this interceptor,
 // this mutation is invisible to the write pipeline.
            _targetEntity.AuditStamp = _stamp;
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            int result, CancellationToken cancellationToken) => Task.CompletedTask;
    }

 // ── Setup helpers ─────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx, ImuItem Entity) CreateContextWithTrackedEntity(
        ISaveChangesInterceptor interceptor)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE ImuItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                AuditStamp TEXT NULL
            )";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(interceptor);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

 // Insert the entity so it's tracked as Unchanged
        var entity = new ImuItem { Name = "original" };
        ctx.Add(entity);
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
 // entity.Id is now set; entity state is Unchanged

        return (cn, ctx, entity);
    }

 // ── Tests ─────────────────────────────────────────────────────────────────

 //<summary>
 //When a SavingChangesAsync interceptor mutates a property on a previously-Unchanged
 //tracked entity, that mutation must appear in the database after SaveChangesAsync completes.
 //Before the fix, DetectAllChanges was not re-run after interceptors, so this update was dropped.
 //</summary>
    [Fact]
    public async Task SavingChangesAsync_InterceptorMutatesUnchangedEntity_MutationIsPersistedToDatabase()
    {
        var entity = new ImuItem { Name = "placeholder" }; // created before context so interceptor can hold ref
        var interceptor = new AuditStampInterceptor(entity, "audit-2024");

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE ImuItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                AuditStamp TEXT NULL
            )";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(interceptor);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

 // Insert the entity and let it become Unchanged
        ctx.Add(entity);
        await ctx.SaveChangesAsync(); // entity is now Unchanged in tracker, AuditStamp=null

 // Now save a DIFFERENT entity — this triggers SavingChangesAsync on ALL interceptors.
 // The interceptor mutates entity.AuditStamp on the Unchanged entity.
        var other = new ImuItem { Name = "trigger" };
        ctx.Add(other);
        await ctx.SaveChangesAsync();

 // The AuditStamp mutation must be in the database.
        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT AuditStamp FROM ImuItem WHERE Id = {entity.Id}";
        var stamp = check.ExecuteScalar()?.ToString();

        Assert.Equal("audit-2024", stamp);
    }

 //<summary>
 //The interceptor-mutated entity must transition from Unchanged to Modified
 //so the write pipeline includes it.
 //</summary>
    [Fact]
    public async Task SavingChangesAsync_InterceptorMutatesUnchangedEntity_EntryBecomesModified()
    {
        var entity = new ImuItem { Name = "placeholder" };
        var interceptor = new AuditStampInterceptor(entity, "modified-stamp");

        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE ImuItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                AuditStamp TEXT NULL
            )";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(interceptor);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

        ctx.Add(entity);
        await ctx.SaveChangesAsync(); // entity Unchanged

 // Trigger a save with another entity; interceptor mutates the Unchanged entity.
 // After the fix, DetectAllChanges re-runs and picks up the mutation.
        var other = new ImuItem { Name = "trigger" };
        ctx.Add(other);
        await ctx.SaveChangesAsync();

 // Verify the mutated entity was persisted
        Assert.Equal("modified-stamp", entity.AuditStamp);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT AuditStamp FROM ImuItem WHERE Id = {entity.Id}";
        var dbValue = check.ExecuteScalar()?.ToString();
        Assert.Equal("modified-stamp", dbValue);
    }

 //<summary>
 //When no interceptor mutations occur, the normal save path is unaffected.
 //Regression guard: existing interceptors that only read (no mutations) should still work.
 //</summary>
    [Fact]
    public async Task SavingChangesAsync_NonMutatingInterceptor_DoesNotAffectNormalSave()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE ImuItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                AuditStamp TEXT NULL
            )";
        cmd.ExecuteNonQuery();

        var readOnlyInterceptor = new ReadOnlyInterceptor();
        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(readOnlyInterceptor);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var entity = new ImuItem { Name = "no-mutation" };
        ctx.Add(entity);
        int affected = await ctx.SaveChangesAsync();

        Assert.Equal(1, affected);
        Assert.Equal(1, readOnlyInterceptor.SavingCalls);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Name FROM ImuItem WHERE Id = 1";
        Assert.Equal("no-mutation", check.ExecuteScalar()?.ToString());
    }

    private sealed class ReadOnlyInterceptor : ISaveChangesInterceptor
    {
        public int SavingCalls { get; private set; }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            CancellationToken cancellationToken)
        {
            SavingCalls++;
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries,
            int result, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
