using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;

namespace nORM.Benchmarks;

/// <summary>
/// Focused benchmark suite for tenant and temporal overhead.
/// It is intentionally separate from the provider matrix so release engineers
/// can run it quickly while evaluating tenant/temporal changes.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[SimpleJob(RuntimeMoniker.Net80, launchCount: 2, warmupCount: 3, iterationCount: 10)]
public class TenantTemporalBenchmarks
{
    private const string DatabasePath = "tenant_temporal_benchmark.db";
    private static readonly SemaphoreSlim SetupLock = new(1, 1);
    private static SharedState? Shared;
    private static int NextWriteId = 100_000;

    [GlobalSetup]
    public async Task Setup()
        => await EnsureSharedAsync();

    private static async Task<SharedState> EnsureSharedAsync()
    {
        if (Shared != null)
            return Shared;

        await SetupLock.WaitAsync();
        try
        {
            if (Shared != null)
                return Shared;

            if (File.Exists(DatabasePath))
                File.Delete(DatabasePath);

            var connection = new SqliteConnection($"Data Source={DatabasePath}");
            await connection.OpenAsync();
            await ConfigureSqliteAsync(connection);

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
CREATE TABLE TtbProduct (
    Id INTEGER PRIMARY KEY,
    TenantId INTEGER NOT NULL,
    Name TEXT NOT NULL,
    IsActive INTEGER NOT NULL,
    Price TEXT NOT NULL
);";
                await cmd.ExecuteNonQueryAsync();
            }

            var plainOptions = CreateOptions(null, temporal: false);
            var tenantOptions = CreateOptions(1, temporal: false);
            var temporalOptions = CreateOptions(1, temporal: true);
            var plainContext = new DbContext(connection, new SqliteProvider(), plainOptions);
            var tenantContext = new DbContext(connection, new SqliteProvider(), tenantOptions);
            var temporalContext = new DbContext(connection, new SqliteProvider(), temporalOptions);

            await temporalContext.EnsureConnectionAsync();

            var rows = Enumerable.Range(1, 2_000).Select(i => new TtbProduct
            {
                Id = i,
                TenantId = i % 2 == 0 ? 1 : 2,
                Name = "Product " + i,
                IsActive = i % 3 != 0,
                Price = 10m + i
            }).ToArray();
            await plainContext.BulkInsertAsync(rows);

            Shared = new SharedState(connection, plainContext, tenantContext, temporalContext);
            return Shared;
        }
        finally
        {
            SetupLock.Release();
        }
    }

    public async Task Cleanup()
    {
        var shared = Shared;
        if (shared != null)
        {
            shared.PlainContext.Dispose();
            shared.TenantContext.Dispose();
            shared.TemporalContext.Dispose();
            await shared.Connection.DisposeAsync();
            Shared = null;
        }

        if (File.Exists(DatabasePath))
            File.Delete(DatabasePath);
    }

    [Benchmark(Baseline = true)]
    public async Task<int> Query_ManualTenantPredicate_CountActive()
    {
        var shared = await EnsureSharedAsync();
        return await shared.PlainContext.Query<TtbProduct>()
            .Where(p => p.TenantId == 1)
            .CountAsync(p => p.IsActive);
    }

    [Benchmark]
    public async Task<int> Query_InjectedTenantPredicate_CountActive()
    {
        var shared = await EnsureSharedAsync();
        return await shared.TenantContext.Query<TtbProduct>().CountAsync(p => p.IsActive);
    }

    [Benchmark]
    public async Task Insert_NoTemporal()
    {
        var shared = await EnsureSharedAsync();
        var id = Interlocked.Increment(ref NextWriteId);
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "write", IsActive = true, Price = 1m };
        await shared.TenantContext.InsertAsync(entity);
    }

    [Benchmark]
    public async Task Insert_Temporal()
    {
        var shared = await EnsureSharedAsync();
        var id = Interlocked.Increment(ref NextWriteId);
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "write", IsActive = true, Price = 1m };
        await shared.TemporalContext.InsertAsync(entity);
    }

    [Benchmark]
    public async Task Update_NoTemporal()
    {
        var shared = await EnsureSharedAsync();
        var id = NextTenantOneUpdateId();
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "updated", IsActive = true, Price = NextWriteId % 1000 };
        await shared.TenantContext.UpdateAsync(entity);
    }

    [Benchmark]
    public async Task Update_Temporal()
    {
        var shared = await EnsureSharedAsync();
        var id = NextTenantOneUpdateId();
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "updated", IsActive = true, Price = NextWriteId % 1000 };
        await shared.TemporalContext.UpdateAsync(entity);
    }

    [Benchmark]
    public async Task Delete_NoTemporal()
    {
        var shared = await EnsureSharedAsync();
        var id = Interlocked.Increment(ref NextWriteId);
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "write", IsActive = true, Price = 1m };
        await shared.TenantContext.InsertAsync(entity);
        await shared.TenantContext.DeleteAsync(entity);
    }

    [Benchmark]
    public async Task Delete_Temporal()
    {
        var shared = await EnsureSharedAsync();
        var id = Interlocked.Increment(ref NextWriteId);
        var entity = new TtbProduct { Id = id, TenantId = 1, Name = "write", IsActive = true, Price = 1m };
        await shared.TemporalContext.InsertAsync(entity);
        await shared.TemporalContext.DeleteAsync(entity);
    }

    private static int NextTenantOneUpdateId()
    {
        var offset = Math.Abs(Interlocked.Increment(ref NextWriteId)) % 1000;
        return (offset + 1) * 2;
    }

    private static async Task ConfigureSqliteAsync(SqliteConnection connection)
    {
        foreach (var sql in new[]
        {
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA busy_timeout = 5000"
        })
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static DbContextOptions CreateOptions(int? tenantId, bool temporal)
    {
        var options = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TtbProduct>()
        };
        if (tenantId != null)
        {
            options.TenantColumnName = "TenantId";
            options.TenantProvider = new FixedTenantProvider(tenantId.Value);
        }
        if (temporal)
            options.EnableTemporalVersioning();
        return options;
    }

    [Table("TtbProduct")]
    public sealed class TtbProduct
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
        public decimal Price { get; set; }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private sealed record SharedState(
        SqliteConnection Connection,
        DbContext PlainContext,
        DbContext TenantContext,
        DbContext TemporalContext);
}
