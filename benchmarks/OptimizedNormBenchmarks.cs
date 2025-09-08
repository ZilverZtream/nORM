using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class OptimizedNormBenchmarks
    {
        private nORM.Configuration.DbContextOptions _nOrmOptions = null!;
        private string _connectionString = null!;
        private Func<Task<SqliteConnection>> _connectionFactory = null!;
        private IDbCacheProvider _cacheProvider = null!;

        // For Compiled Query
        private Func<DbContext, int, Task<List<BenchmarkUser>>> _compiledQuery = null!;

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            var dbPath = Path.Combine(Path.GetTempPath(), "optimized_benchmark.db");
            _connectionString = $"Data Source={dbPath};Cache=Shared;Pooling=True";
            _connectionFactory = async () =>
            {
                var conn = new SqliteConnection(_connectionString);
                await conn.OpenAsync();
                return conn;
            };

            // Setup cache and options
            _cacheProvider = new NormMemoryCacheProvider();
            _nOrmOptions = new nORM.Configuration.DbContextOptions { CacheProvider = _cacheProvider };

            // Setup database with one user
            if (File.Exists(dbPath)) File.Delete(dbPath);
            await using var connection = await _connectionFactory();
            await using var cmd = connection.CreateCommand();

            // FIX: The CREATE TABLE statement now includes all columns from the BenchmarkUser model.
            cmd.CommandText = @"
        CREATE TABLE BenchmarkUser (
            Id INTEGER PRIMARY KEY, 
            Name TEXT,
            Email TEXT,
            CreatedAt TEXT,
            IsActive INTEGER,
            Age INTEGER,
            City TEXT,
            Department TEXT,
            Salary REAL
        );";
            await cmd.ExecuteNonQueryAsync();

            // FIX: The INSERT statement now provides data for all columns.
            cmd.CommandText = @"
        INSERT INTO BenchmarkUser (Id, Name, Email, CreatedAt, IsActive, Age, City, Department, Salary) 
        VALUES (1, 'Test User', 'test@example.com', '2025-09-07', 1, 30, 'New York', 'Sales', 60000.0);";
            await cmd.ExecuteNonQueryAsync();

            // Pre-compile the query
            _compiledQuery = Norm.CompileQuery((DbContext ctx, int age) =>
                ctx.Query<BenchmarkUser>().Where(u => u.Age > age));

            // Warm up the cache by running the cacheable query once
            await Query_Cached();
        }

        [GlobalCleanup]
        public void GlobalCleanup() => SqliteConnection.ClearAllPools();

        [Benchmark(Baseline = true)]
        public async Task<List<BenchmarkUser>> Query_Standard()
        {
            await using var connection = await _connectionFactory();
            await using var context = new DbContext(connection, new SqliteProvider(), _nOrmOptions);
            return await context.Query<BenchmarkUser>().Where(u => u.Age > 25).ToListAsync();
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Compiled()
        {
            await using var connection = await _connectionFactory();
            await using var context = new DbContext(connection, new SqliteProvider(), _nOrmOptions);
            // Execute the pre-compiled query delegate
            return await _compiledQuery(context, 25);
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Cached()
        {
            await using var connection = await _connectionFactory();
            await using var context = new DbContext(connection, new SqliteProvider(), _nOrmOptions);
            // The second run of this will be near-instantaneous as it hits the cache
            return await context.Query<BenchmarkUser>().Where(u => u.Age > 25).Cacheable(TimeSpan.FromMinutes(1)).ToListAsync();
        }
    }
}