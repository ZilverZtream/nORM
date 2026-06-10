using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
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

[Trait("Category", "Fast")]
public class TemporalLifecycleHardeningTests
{
    [Table("TlhProduct")]
    private sealed class TlhProduct
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public decimal Price { get; set; }
    }

    [Table("TlhCompositeProduct")]
    private sealed class TlhCompositeProduct
    {
        public int TenantId { get; set; }
        public int ProductId { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("TlhTenantProduct")]
    private sealed class TlhTenantProduct
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task Temporal_disabled_context_creates_no_temporal_artifacts()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: false);

        await ctx.EnsureConnectionAsync();
        Assert.False(TableExists(cn, "__NormTemporalTags"));
        Assert.False(TableExists(cn, "TlhProduct_History"));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("information_schema")]
    public async Task Temporal_bootstrap_rejects_provider_owned_database(string databaseName)
    {
        using var inner = OpenDb();
        using var cn = new DatabaseNameConnection(inner, databaseName);
        var options = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TlhProduct>()
        };
        options.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new MySqlProvider(new SqliteParameterFactory()), options);

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(
            () => ctx.EnsureConnectionAsync());

        Assert.Contains("Temporal versioning", ex.Message, StringComparison.Ordinal);
        Assert.Contains(databaseName, ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(TableExists(inner, "__NormTemporalTags"));
        Assert.False(TableExists(inner, "TlhProduct_History"));
    }

    [Fact]
    public async Task Temporal_bulk_insert_update_delete_records_history_rows()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        await ctx.BulkInsertAsync(new[] { new TlhProduct { Id = 1, Name = "alpha", Price = 10m } });
        await ctx.BulkUpdateAsync(new[] { new TlhProduct { Id = 1, Name = "beta", Price = 20m } });
        await ctx.BulkDeleteAsync(new[] { new TlhProduct { Id = 1, Name = "beta", Price = 20m } });

        Assert.Equal(3, CountHistoryRows(cn));
        Assert.Equal(1, CountHistoryRows(cn, "I"));
        Assert.Equal(1, CountHistoryRows(cn, "U"));
        Assert.Equal(1, CountHistoryRows(cn, "D"));

        var history = await ctx.GetTemporalHistoryAsync<TlhProduct>(1);
        Assert.Equal(new[] { "I", "U", "D" }, history.Select(h => h.Operation).ToArray());
        Assert.Equal("alpha", history[0].Entity.Name);
        Assert.Equal("beta", history[1].Entity.Name);
        Assert.Equal("beta", history[2].Entity.Name);
        Assert.True(history[0].ValidFrom <= history[0].ValidTo);
    }

    [Fact]
    public async Task Temporal_as_of_tag_after_bulk_update_returns_previous_state()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        await ctx.BulkInsertAsync(new[] { new TlhProduct { Id = 1, Name = "alpha", Price = 10m } });

        var tag = "before-bulk-update-" + Guid.NewGuid().ToString("N");
        await ctx.CreateTagAsync(tag);

        // SQLite temporal triggers use second precision.
        await Task.Delay(TimeSpan.FromMilliseconds(1100));

        await ctx.BulkUpdateAsync(new[] { new TlhProduct { Id = 1, Name = "beta", Price = 20m } });

        var historical = await ctx.Query<TlhProduct>()
            .AsOf(tag)
            .SingleAsync(p => p.Id == 1);
        var current = await ctx.Query<TlhProduct>()
            .SingleAsync(p => p.Id == 1);

        Assert.Equal("alpha", historical.Name);
        Assert.Equal(10m, historical.Price);
        Assert.Equal("beta", current.Name);
        Assert.Equal(20m, current.Price);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task CreateTagAsync_rejects_empty_or_whitespace_names(string tagName)
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => ctx.CreateTagAsync(tagName));
        Assert.Contains("Temporal tag name", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTagAsync_rejects_names_longer_than_sql_server_contract()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => ctx.CreateTagAsync(new string('x', 201)));
        Assert.Contains("200", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTagAsync_requires_temporal_versioning()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: false);

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.CreateTagAsync("disabled"));
        Assert.Contains("EnableTemporalVersioning", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTagAsync_binds_active_transaction_and_rolls_back_with_it()
    {
        using var cn = OpenDb();
        var interceptor = new TemporalTransactionInterceptor();
        using var ctx = CreateContext(cn, temporal: true, interceptor: interceptor);

        await ctx.EnsureConnectionAsync();
        await using var tx = await ctx.Database.BeginTransactionAsync();
        var tag = "tx-tag-" + Guid.NewGuid().ToString("N");

        await ctx.CreateTagAsync(tag);

        Assert.NotNull(interceptor.TagCommandTransaction);
        await tx.RollbackAsync();
        Assert.Equal(0, CountTags(cn, tag));
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_requires_temporal_versioning()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: false);

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
            ctx.GetTemporalHistoryAsync<TlhProduct>(1));
        Assert.Contains("EnableTemporalVersioning", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_rejects_wrong_key_arity()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.GetTemporalHistoryAsync<TlhProduct>(Array.Empty<object?>()));
        Assert.Contains("requires 1 key", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_rejects_null_non_nullable_key()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.GetTemporalHistoryAsync<TlhProduct>((object?)null));
        Assert.Contains("non-null key", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_coerces_key_values_to_mapped_key_type()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        var history = await ctx.GetTemporalHistoryAsync<TlhProduct>("1");

        Assert.Single(history);
        Assert.Equal("alpha", history[0].Entity.Name);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_supports_composite_keys_in_mapping_order()
    {
        using var cn = OpenDb(includeComposite: true);
        using var ctx = CreateContext(cn, temporal: true, includeComposite: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhCompositeProduct { TenantId = 7, ProductId = 42, Name = "before" });
        await ctx.SaveChangesAsync();

        var row = await ctx.Query<TlhCompositeProduct>()
            .SingleAsync(p => p.TenantId == 7 && p.ProductId == 42);
        row.Name = "after";
        await ctx.SaveChangesAsync();

        var history = await ctx.GetTemporalHistoryAsync<TlhCompositeProduct>(new object?[] { 7, 42 });

        Assert.Equal(new[] { "I", "U" }, history.Select(h => h.Operation).ToArray());
        Assert.Equal("before", history[0].Entity.Name);
        Assert.Equal("after", history[1].Entity.Name);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_keeps_tenant_boundary()
    {
        using var cn = OpenDb(includeTenant: true);
        using var ctxA = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 101);
        using var ctxB = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 202);

        await ctxA.EnsureConnectionAsync();
        ctxA.Add(new TlhTenantProduct { Id = 1, TenantId = 101, Name = "tenant-a" });
        await ctxA.SaveChangesAsync();

        ctxB.Add(new TlhTenantProduct { Id = 2, TenantId = 202, Name = "tenant-b" });
        await ctxB.SaveChangesAsync();

        var ownHistory = await ctxA.GetTemporalHistoryAsync<TlhTenantProduct>(1);
        var foreignHistory = await ctxA.GetTemporalHistoryAsync<TlhTenantProduct>(2);

        Assert.Single(ownHistory);
        Assert.Equal("tenant-a", ownHistory[0].Entity.Name);
        Assert.Empty(foreignHistory);
    }

    [Fact]
    public async Task GetTemporalHistoryAsync_binds_active_transaction()
    {
        using var cn = OpenDb();
        var interceptor = new TemporalTransactionInterceptor();
        using var ctx = CreateContext(cn, temporal: true, interceptor: interceptor);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        await using var tx = await ctx.Database.BeginTransactionAsync();
        var history = await ctx.GetTemporalHistoryAsync<TlhProduct>(1);

        Assert.Single(history);
        Assert.NotNull(interceptor.HistoryCommandTransaction);
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task GetTemporalDiffAsync_returns_changed_mapped_properties()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        var row = await ctx.Query<TlhProduct>().SingleAsync(p => p.Id == 1);
        row.Name = "beta";
        row.Price = 12.5m;
        await ctx.SaveChangesAsync();

        var diff = await ctx.GetTemporalDiffAsync<TlhProduct>(1);

        var entry = Assert.Single(diff);
        Assert.Equal("alpha", entry.Previous.Entity.Name);
        Assert.Equal("beta", entry.Current.Entity.Name);
        Assert.Contains(entry.Changes, c => c.PropertyName == nameof(TlhProduct.Name) &&
                                            Equals(c.PreviousValue, "alpha") &&
                                            Equals(c.CurrentValue, "beta"));
        Assert.Contains(entry.Changes, c => c.PropertyName == nameof(TlhProduct.Price) &&
                                            Equals(c.PreviousValue, 10m) &&
                                            Equals(c.CurrentValue, 12.5m));
    }

    [Fact]
    public async Task GetTemporalDiffAsync_keeps_tenant_boundary()
    {
        using var cn = OpenDb(includeTenant: true);
        using var ctxA = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 101);
        using var ctxB = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 202);

        await ctxA.EnsureConnectionAsync();
        ctxB.Add(new TlhTenantProduct { Id = 2, TenantId = 202, Name = "tenant-b" });
        await ctxB.SaveChangesAsync();
        var b = await ctxB.Query<TlhTenantProduct>().SingleAsync(p => p.Id == 2);
        b.Name = "tenant-b-updated";
        await ctxB.SaveChangesAsync();

        var diff = await ctxA.GetTemporalDiffAsync<TlhTenantProduct>(2);

        Assert.Empty(diff);
    }

    [Fact]
    public async Task RestoreTemporalVersionAsync_tag_restores_existing_current_row()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        var tag = "before-restore-" + Guid.NewGuid().ToString("N");
        await ctx.CreateTagAsync(tag);
        await Task.Delay(TimeSpan.FromMilliseconds(1100));

        var row = await ctx.Query<TlhProduct>().SingleAsync(p => p.Id == 1);
        row.Name = "beta";
        row.Price = 20m;
        await ctx.SaveChangesAsync();

        var restored = await ctx.RestoreTemporalVersionAsync<TlhProduct>(1, tag);
        var current = await ctx.Query<TlhProduct>().SingleAsync(p => p.Id == 1);

        Assert.Equal(1, restored);
        Assert.Equal("alpha", current.Name);
        Assert.Equal(10m, current.Price);
    }

    [Fact]
    public async Task RestoreTemporalVersionAsync_returns_zero_when_current_row_is_missing()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        var tag = "before-delete-" + Guid.NewGuid().ToString("N");
        await ctx.CreateTagAsync(tag);
        await Task.Delay(TimeSpan.FromMilliseconds(1100));

        await ctx.DeleteAsync(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });

        var restored = await ctx.RestoreTemporalVersionAsync<TlhProduct>(1, tag);

        Assert.Equal(0, restored);
        Assert.False(await ctx.Query<TlhProduct>().AnyAsync(p => p.Id == 1));
    }

    [Fact]
    public async Task RestoreTemporalVersionAsync_requires_temporal_versioning()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: false);

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
            ctx.RestoreTemporalVersionAsync<TlhProduct>(1, DateTime.UtcNow));
        Assert.Contains("EnableTemporalVersioning", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PruneTemporalHistoryAsync_deletes_closed_history_rows_before_cutoff()
    {
        using var cn = OpenDb();
        using var ctx = CreateContext(cn, temporal: true);

        await ctx.EnsureConnectionAsync();
        ctx.Add(new TlhProduct { Id = 1, Name = "alpha", Price = 10m });
        await ctx.SaveChangesAsync();

        var row = await ctx.Query<TlhProduct>().SingleAsync(p => p.Id == 1);
        row.Name = "beta";
        await ctx.SaveChangesAsync();

        Assert.True(CountHistoryRows(cn) >= 2);

        var deleted = await ctx.PruneTemporalHistoryAsync(DateTime.UtcNow.AddDays(1));

        Assert.True(deleted >= 1);
        Assert.Equal(1, CountHistoryRows(cn));
        Assert.Equal(0, CountHistoryRows(cn, "I"));
        Assert.Equal("beta", (await ctx.Query<TlhProduct>().SingleAsync(p => p.Id == 1)).Name);
    }

    [Fact]
    public async Task PruneTemporalHistoryAsync_keeps_tenant_boundary()
    {
        using var cn = OpenDb(includeTenant: true);
        using var ctxA = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 101);
        using var ctxB = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 202);

        await ctxA.EnsureConnectionAsync();
        ctxA.Add(new TlhTenantProduct { Id = 1, TenantId = 101, Name = "tenant-a" });
        await ctxA.SaveChangesAsync();
        ctxB.Add(new TlhTenantProduct { Id = 2, TenantId = 202, Name = "tenant-b" });
        await ctxB.SaveChangesAsync();

        var a = await ctxA.Query<TlhTenantProduct>().SingleAsync(p => p.Id == 1);
        a.Name = "tenant-a-updated";
        await ctxA.SaveChangesAsync();
        var b = await ctxB.Query<TlhTenantProduct>().SingleAsync(p => p.Id == 2);
        b.Name = "tenant-b-updated";
        await ctxB.SaveChangesAsync();

        Assert.True(CountHistoryRows(cn, tableName: "TlhTenantProduct_History", tenantId: 101) >= 2);
        Assert.True(CountHistoryRows(cn, tableName: "TlhTenantProduct_History", tenantId: 202) >= 2);

        var deleted = await ctxA.PruneTemporalHistoryAsync<TlhTenantProduct>(DateTime.UtcNow.AddDays(1));

        Assert.True(deleted >= 1);
        Assert.Equal(1, CountHistoryRows(cn, tableName: "TlhTenantProduct_History", tenantId: 101));
        Assert.Equal(0, CountHistoryRows(cn, "I", tableName: "TlhTenantProduct_History", tenantId: 101));
        Assert.True(CountHistoryRows(cn, tableName: "TlhTenantProduct_History", tenantId: 202) >= 2);
    }

    [Fact]
    public async Task PruneTemporalHistoryAsync_rejects_global_tag_pruning_in_tenant_mode()
    {
        using var cn = OpenDb(includeTenant: true);
        using var ctx = CreateContext(cn, temporal: true, includeTenant: true, tenantId: 101);

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.PruneTemporalHistoryAsync(DateTime.UtcNow.AddDays(1), pruneTags: true));
        Assert.Contains("Temporal tags are global", ex.Message, StringComparison.Ordinal);
    }

    private static SqliteConnection OpenDb(bool includeComposite = false, bool includeTenant = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TlhProduct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price TEXT NOT NULL);";
        if (includeComposite)
        {
            cmd.CommandText +=
                "CREATE TABLE TlhCompositeProduct (" +
                "TenantId INTEGER NOT NULL, ProductId INTEGER NOT NULL, Name TEXT NOT NULL, " +
                "PRIMARY KEY (TenantId, ProductId));";
        }
        if (includeTenant)
        {
            cmd.CommandText +=
                "CREATE TABLE TlhTenantProduct (" +
                "Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL);";
        }
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateContext(
        SqliteConnection cn,
        bool temporal,
        bool includeComposite = false,
        bool includeTenant = false,
        int? tenantId = null,
        IDbCommandInterceptor? interceptor = null)
    {
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TlhProduct>();
                if (includeComposite)
                    mb.Entity<TlhCompositeProduct>().HasKey(p => new { p.TenantId, p.ProductId });
                if (includeTenant)
                    mb.Entity<TlhTenantProduct>();
            }
        };
        if (tenantId != null)
        {
            options.TenantColumnName = "TenantId";
            options.TenantProvider = new FixedTenantProvider(tenantId.Value);
        }
        if (temporal)
            options.EnableTemporalVersioning();
        if (interceptor != null)
            options.CommandInterceptors.Add(interceptor);
        return new DbContext(cn, new SqliteProvider(), options);
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private sealed class DatabaseNameConnection(DbConnection inner, string databaseName) : DbConnection
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => inner.ConnectionString;
            set => inner.ConnectionString = value!;
        }

        public override string Database => databaseName;
        public override string DataSource => inner.DataSource;
        public override string ServerVersion => inner.ServerVersion;
        public override ConnectionState State => inner.State;

        public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
        public override void Close() => inner.Close();
        public override void Open() => inner.Open();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => inner.BeginTransaction(isolationLevel);

        protected override DbCommand CreateDbCommand()
            => inner.CreateCommand();
    }

    private static bool TableExists(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @name";
        cmd.Parameters.AddWithValue("@name", table);
        return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
    }

    private static long CountHistoryRows(
        SqliteConnection cn,
        string? operation = null,
        string tableName = "TlhProduct_History",
        int? tenantId = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
        var clauses = new System.Collections.Generic.List<string>();
        if (operation != null)
        {
            clauses.Add("__Operation = @op");
            cmd.Parameters.AddWithValue("@op", operation);
        }
        if (tenantId != null)
        {
            clauses.Add("TenantId = @tenant");
            cmd.Parameters.AddWithValue("@tenant", tenantId.Value);
        }
        if (clauses.Count > 0)
            cmd.CommandText += " WHERE " + string.Join(" AND ", clauses);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static long CountTags(SqliteConnection cn, string tagName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM __NormTemporalTags WHERE TagName = @tag";
        cmd.Parameters.AddWithValue("@tag", tagName);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private sealed class TemporalTransactionInterceptor : IDbCommandInterceptor
    {
        public DbTransaction? TagCommandTransaction { get; private set; }
        public DbTransaction? HistoryCommandTransaction { get; private set; }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command,
            DbContext context,
            CancellationToken cancellationToken)
        {
            if (command.CommandText.Contains("__NormTemporalTags", StringComparison.Ordinal) &&
                command.CommandText.Contains("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                TagCommandTransaction = command.Transaction;
            }

            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            DbContext context,
            CancellationToken cancellationToken)
        {
            if (command.CommandText.Contains("TlhProduct_History", StringComparison.Ordinal))
                HistoryCommandTransaction = command.Transaction;
            return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
        }

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
