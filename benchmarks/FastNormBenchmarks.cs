using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Microsoft.Data.Sqlite;
using Dapper;
using nORM.Core;
using nORM.Providers;
using NormAsyncExtensions = nORM.Core.NormAsyncExtensions;

namespace nORM.Benchmarks
{
    /// <summary>
    /// Fast nORM-only benchmarks for debugging and development
    /// </summary>
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [SimpleJob(warmupCount: 1, iterationCount: 3)] // Fast execution
    public class FastNormBenchmarks
    {
        private const int UserCount = 100;  // Smaller dataset for speed
        private const int OrderCount = 200;
        private List<BenchmarkUser> _testUsers = new();
        private List<BenchmarkOrder> _testOrders = new();
        
        private readonly string _connectionString = "Data Source=fast_norm_benchmark.db";
        private SqliteConnection? _connection;
        private nORM.Core.DbContext? _context;

        private nORM.Core.DbContext GetContext()
        {
            return _context ?? throw new InvalidOperationException("nORM context not initialized. Make sure GlobalSetup was called.");
        }

        [GlobalSetup]
        public async Task Setup()
        {
            Console.WriteLine("🔧 Setting up fast nORM benchmark...");
            
            // Delete existing database
            if (System.IO.File.Exists("fast_norm_benchmark.db"))
                System.IO.File.Delete("fast_norm_benchmark.db");

            // Generate smaller test data
            var random = new Random(42);
            _testUsers = Enumerable.Range(1, UserCount)
                .Select(i => new BenchmarkUser
                {
                    Name = $"User {i}",
                    Email = $"user{i}@example.com",
                    CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                    IsActive = random.Next(10) > 2,
                    Age = random.Next(18, 80),
                    City = GetRandomCity(random)
                }).ToList();

            _testOrders = Enumerable.Range(1, OrderCount)
                .Select(i => new BenchmarkOrder
                {
                    UserId = random.Next(1, UserCount + 1),
                    Amount = (decimal)(random.NextDouble() * 1000),
                    OrderDate = DateTime.Now.AddDays(-random.Next(30)),
                    ProductName = $"Product {random.Next(1, 100)}"
                }).ToList();

            // Setup nORM with optimized options
            _connection = new SqliteConnection(_connectionString);
            await _connection.OpenAsync();
            
            var options = new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50, // Optimized for SQLite parameter limits
                CommandTimeout = TimeSpan.FromSeconds(30)
            };
            
            _context = new nORM.Core.DbContext(_connection, new SqliteProvider(), options);

            // Create tables
            var createUserTableSql = @"
                CREATE TABLE BenchmarkUser (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Email TEXT NOT NULL,
                    CreatedAt TEXT NOT NULL,
                    IsActive INTEGER NOT NULL,
                    Age INTEGER NOT NULL,
                    City TEXT NOT NULL
                )";

            var createOrderTableSql = @"
                CREATE TABLE BenchmarkOrder (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UserId INTEGER NOT NULL,
                    Amount REAL NOT NULL,
                    OrderDate TEXT NOT NULL,
                    ProductName TEXT NOT NULL
                )";

            await _connection.ExecuteAsync(createUserTableSql);
            await _connection.ExecuteAsync(createOrderTableSql);

            // Insert test data
            foreach (var user in _testUsers)
            {
                await _context.InsertAsync(user);
            }
            
            foreach (var order in _testOrders)
            {
                await _context.InsertAsync(order);
            }
            
            Console.WriteLine("✅ Fast nORM benchmark setup complete!");
        }

        private static string GetRandomCity(Random random)
        {
            var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney" };
            return cities[random.Next(cities.Length)];
        }

        // ========== BASIC OPERATIONS ==========

        [Benchmark]
        public async Task Insert_Single()
        {
            var context = GetContext();
            
            var user = new BenchmarkUser
            {
                Name = "Test User",
                Email = "test@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity"
            };

            await context.InsertAsync(user);
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple()
        {
            var context = GetContext();
            
            return await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive)
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex()
        {
            var context = GetContext();
            
            return await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive && u.Age > 25)
                .OrderBy(u => u.Name)
                .Skip(2)
                .Take(10));
        }

        [Benchmark]
        public async Task<int> Count_Operation()
        {
            var context = GetContext();
            
            return await NormAsyncExtensions.CountAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive));
        }

        [Benchmark]
        public async Task<List<object>> Query_Join()
        {
            var context = GetContext();
            
            var result = await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Join(
                    context.Query<BenchmarkOrder>(),
                    u => u.Id,
                    o => o.UserId,
                    (u, o) => new { u.Name, o.Amount, o.ProductName }
                )
                .Where(x => x.Amount > 100)
                .Take(20));
            return result.Cast<object>().ToList();
        }

        [Benchmark]
        public async Task BulkInsert_Small()
        {
            var context = GetContext();
            
            var users = Enumerable.Range(1, 50).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User {i}",
                Email = $"bulk{i}@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity"
            }).ToList();

            await context.BulkInsertAsync(users);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _context?.Dispose();
            _connection?.Dispose();

            try
            {
                if (System.IO.File.Exists("fast_norm_benchmark.db"))
                    System.IO.File.Delete("fast_norm_benchmark.db");
            }
            catch { }
        }
    }
}
