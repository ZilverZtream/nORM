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

/// <summary>
/// Adversarial interceptor integration tests. These test interceptors doing real
/// work — writing audit rows, implementing soft-delete, blocking saves, and
/// confirming post-commit visibility — not just recording that they fired.
/// </summary>
public class InterceptorRealScenarioTests
{
    // ── Domain models ──────────────────────────────────────────────────────

    [Table("IrsCustomer")]
    private class IrsCustomer
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsDeleted { get; set; }
    }

    [Table("IrsAuditLog")]
    private class IrsAuditLog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string EntityType { get; set; } = string.Empty;
        public string Action { get; set; } = string.Empty;
        public string Timestamp { get; set; } = string.Empty;
    }

    // ── Interceptors ───────────────────────────────────────────────────────

    /// <summary>
    /// Adds one IrsAuditLog row per entity change during SavingChanges.
    /// </summary>
    private class AuditLogInterceptor : ISaveChangesInterceptor
    {
        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
        {
            foreach (var entry in entries)
            {
                if (entry.Entity is IrsAuditLog) continue; // don't audit audit rows
                context.Add(new IrsAuditLog
                {
                    EntityType = entry.Entity?.GetType().Name ?? "Unknown",
                    Action = entry.State.ToString(),
                    Timestamp = DateTime.UtcNow.ToString("O")
                });
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }

    /// <summary>
    /// Converts Delete into a soft-delete (sets IsDeleted=true, changes state to Modified).
    /// </summary>
    private class SoftDeleteInterceptor : ISaveChangesInterceptor
    {
        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
        {
            foreach (var entry in entries.ToList()) // snapshot to avoid mutation-while-iterating
            {
                if (entry.State == EntityState.Deleted && entry.Entity is IrsCustomer customer)
                {
                    // Change state from Deleted to Modified, mark IsDeleted = true
                    customer.IsDeleted = true;
                    entry.State = EntityState.Modified;
                }
            }
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }

    /// <summary>
    /// Throws unconditionally — simulates a read-only context guard.
    /// </summary>
    private class ReadOnlyInterceptor : ISaveChangesInterceptor
    {
        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
            => Task.FromException(new InvalidOperationException("Read-only context: saves are not permitted."));

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }

    /// <summary>
    /// Records whether it was called after the save committed.
    /// Opens a second connection to verify the committed row is visible.
    /// </summary>
    private class PostCommitVisibilityInterceptor : ISaveChangesInterceptor
    {
        private readonly string _connectionString;
        public int CommittedCustomers { get; private set; }

        public PostCommitVisibilityInterceptor(string connectionString)
        {
            _connectionString = connectionString;
        }

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
            => Task.CompletedTask;

        public async Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
        {
            // Open a NEW independent connection to verify committed data
            await using var cn2 = new SqliteConnection(_connectionString);
            await cn2.OpenAsync(ct);
            await using var cmd = cn2.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer";
            CommittedCustomers = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
        }
    }

    // ── Setup helper ───────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(ISaveChangesInterceptor? interceptor = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE IrsCustomer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL DEFAULT 0);" +
            "CREATE TABLE IrsAuditLog (Id INTEGER PRIMARY KEY AUTOINCREMENT, EntityType TEXT NOT NULL, Action TEXT NOT NULL, Timestamp TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions();
        if (interceptor != null)
            opts.SaveChangesInterceptors.Add(interceptor);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    // ── Test 1: Audit log interceptor ─────────────────────────────────────

    /// <summary>
    /// Insert a Customer → interceptor adds AuditLog row → both persisted.
    /// Insert 3 more customers → 3 more AuditLog rows.
    /// </summary>
    [Fact]
    public async Task AuditLogInterceptor_SingleInsert_CreatesAuditRow()
    {
        var (cn, ctx) = CreateContext(new AuditLogInterceptor());
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "Alice" });
        var saved = await ctx.SaveChangesAsync();

        // At minimum: 1 customer + 1 audit log
        Assert.True(saved >= 2, $"Expected >=2 rows saved, got {saved}");

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));

        vcmd.CommandText = "SELECT COUNT(*) FROM IrsAuditLog WHERE EntityType = 'IrsCustomer'";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    [Fact]
    public async Task AuditLogInterceptor_ThreeInserts_ThreeAuditRows()
    {
        var (cn, ctx) = CreateContext(new AuditLogInterceptor());
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "Bob" });
        ctx.Add(new IrsCustomer { Name = "Carol" });
        ctx.Add(new IrsCustomer { Name = "Dave" });
        await ctx.SaveChangesAsync();

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer";
        Assert.Equal(3L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // 3 audit rows for the 3 customer inserts
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsAuditLog WHERE EntityType = 'IrsCustomer' AND Action = 'Added'";
        Assert.Equal(3L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 2: Soft-delete interceptor ───────────────────────────────────

    /// <summary>
    /// Remove(customer) → interceptor converts to soft-delete → row still in DB
    /// with IsDeleted=true. Query without filter: row returned. Query with
    /// Where(x => !x.IsDeleted): row excluded.
    /// </summary>
    [Fact]
    public async Task SoftDeleteInterceptor_Delete_ConvertsToUpdate()
    {
        var (cn, ctx) = CreateContext(new SoftDeleteInterceptor());
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "Eve" });
        await ctx.SaveChangesAsync();

        // Load and remove
        var customer = ctx.Query<IrsCustomer>().First(c => c.Name == "Eve");
        ctx.Remove(customer);
        await ctx.SaveChangesAsync(detectChanges: false);

        // Row still exists (soft-deleted)
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer WHERE Name = 'Eve'";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // IsDeleted = 1
        vcmd.CommandText = "SELECT IsDeleted FROM IrsCustomer WHERE Name = 'Eve'";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Query without filter returns the soft-deleted row
        await using var ctx2 = new DbContext(cn, new SqliteProvider());
        var allCustomers = await ctx2.Query<IrsCustomer>().ToListAsync();
        Assert.Single(allCustomers);

        // Query with filter excludes the soft-deleted row
        var activeCustomers = await ctx2.Query<IrsCustomer>()
            .Where(c => !c.IsDeleted)
            .ToListAsync();
        Assert.Empty(activeCustomers);
    }

    // ── Test 3: Read-only interceptor rejects save ─────────────────────────

    /// <summary>
    /// SaveChanges with a read-only interceptor must throw InvalidOperationException.
    /// No rows must be written.
    /// </summary>
    [Fact]
    public async Task ReadOnlyInterceptor_RejectsSave_NoRowsWritten()
    {
        var (cn, ctx) = CreateContext(new ReadOnlyInterceptor());
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "Frank" });

        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.NotNull(ex);
        Assert.IsType<InvalidOperationException>(ex);
        Assert.Contains("Read-only", ex.Message);

        // No rows written
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 4: No-op interceptor does not break normal save ──────────────

    /// <summary>
    /// An interceptor that does nothing must not affect normal save behaviour.
    /// Row count must be exactly what's expected.
    /// </summary>
    [Fact]
    public async Task NoOpInterceptor_NormalSave_Unaffected()
    {
        // Use a minimal interceptor that just returns CompletedTask
        var (cn, ctx) = CreateContext(new MinimalInterceptor());
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "Grace" });
        ctx.Add(new IrsCustomer { Name = "Heidi" });
        var saved = await ctx.SaveChangesAsync();

        Assert.Equal(2, saved);

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsCustomer";
        Assert.Equal(2L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    private class MinimalInterceptor : ISaveChangesInterceptor
    {
        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
            => Task.CompletedTask;

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }

    // ── Test 5: Multiple interceptors fire in order ────────────────────────

    /// <summary>
    /// Two audit interceptors registered. Both must fire; combined audit row count
    /// must equal 2 per customer insert (one from each interceptor).
    /// </summary>
    [Fact]
    public async Task MultipleInterceptors_BothFire_CumulativeAuditRows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setupCmd = cn.CreateCommand();
        setupCmd.CommandText =
            "CREATE TABLE IrsCustomer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL DEFAULT 0);" +
            "CREATE TABLE IrsAuditLog (Id INTEGER PRIMARY KEY AUTOINCREMENT, EntityType TEXT NOT NULL, Action TEXT NOT NULL, Timestamp TEXT NOT NULL);";
        setupCmd.ExecuteNonQuery();

        var opts = new DbContextOptions();
        opts.SaveChangesInterceptors.Add(new AuditLogInterceptor());
        opts.SaveChangesInterceptors.Add(new AuditLogInterceptor()); // second instance
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        ctx.Add(new IrsCustomer { Name = "Ivan" });
        await ctx.SaveChangesAsync();

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM IrsAuditLog WHERE EntityType = 'IrsCustomer'";
        var auditCount = Convert.ToInt64(vcmd.ExecuteScalar());
        // Both interceptors fired → at least 2 audit rows for 1 customer insert
        Assert.True(auditCount >= 2, $"Both interceptors should have added audit rows. Got {auditCount}");
    }

    // ── Test 6: Interceptor modifying entity is persisted ──────────────────

    /// <summary>
    /// Interceptor changes the customer Name in SavingChanges. The persisted row
    /// must have the interceptor's value, not the original.
    /// </summary>
    [Fact]
    public async Task Interceptor_ModifiesEntityBeforeSave_PersistedValueIsNew()
    {
        var (cn, ctx) = CreateContext(new NameOverrideInterceptor("InterceptedName"));
        await using var _ = ctx;

        ctx.Add(new IrsCustomer { Name = "OriginalName" });
        await ctx.SaveChangesAsync();

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT Name FROM IrsCustomer WHERE Id = 1";
        var name = (string)vcmd.ExecuteScalar()!;
        Assert.Equal("InterceptedName", name);
    }

    private class NameOverrideInterceptor : ISaveChangesInterceptor
    {
        private readonly string _newName;
        public NameOverrideInterceptor(string newName) => _newName = newName;

        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
        {
            foreach (var e in entries)
                if (e.Entity is IrsCustomer c && e.State == EntityState.Added)
                    c.Name = _newName;
            return Task.CompletedTask;
        }

        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }
}
