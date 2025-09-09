using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using nORM.Core;
using nORM.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
// Explicitly alias this to avoid ambiguity
using NormAsyncExtensions = nORM.Core.NormAsyncExtensions;
namespace nORM.Benchmarks
{
    /// <summary>
    /// A comprehensive, fast-running benchmark suite for nORM's features.
    /// Acts as a performance and functional regression test.
    /// </summary>
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [SimpleJob(warmupCount: 1, iterationCount: 3)] // Fast execution settings
    public class FastNormBenchmarks
    {
        private const int UserCount = 100;
        private const int OrderCount = 200;
        private List<BenchmarkUser> _testUsers = new();
        private List<BenchmarkOrder> _testOrders = new();
        private readonly string _connectionString = "Data Source=fast_norm_benchmark.db";
        private SqliteConnection? _connection;
        private nORM.Core.DbContext? _context;
        // --- Compiled Queries ---
        private static Func<nORM.Core.DbContext, int, Task<List<BenchmarkUser>>>? _normSimpleCompiled;
        private static Func<nORM.Core.DbContext, (int, string), Task<List<BenchmarkUser>>>? _normComplexCompiled;
        private nORM.Core.DbContext GetContext()
        {
            return _context ?? throw new InvalidOperationException("nORM context not initialized.");
        }
        [GlobalSetup]
        public async Task Setup()
        {
            Console.WriteLine("🔧 Setting up fast nORM benchmark...");
            if (System.IO.File.Exists("fast_norm_benchmark.db"))
                System.IO.File.Delete("fast_norm_benchmark.db");
            var random = new Random(42);
            _testUsers = Enumerable.Range(1, UserCount)
                .Select(i => new BenchmarkUser
                {
                    Id = i, // Set predictable IDs for update/delete tests
                    Name = $"User {i}",
                    Email = $"user{i}@example.com",
                    CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                    IsActive = random.Next(10) > 2,
                    Age = random.Next(18, 80),
                    City = GetRandomCity(random),
                    Department = GetRandomDepartment(random),
                    Salary = random.Next(40_000, 100_000)
                }).ToList();
            _testOrders = Enumerable.Range(1, OrderCount)
                .Select(i => new BenchmarkOrder
                {
                    UserId = random.Next(1, UserCount + 1),
                    Amount = (decimal)(random.NextDouble() * 1000),
                    OrderDate = DateTime.Now.AddDays(-random.Next(30)),
                    ProductName = $"Product {random.Next(1, 100)}"
                }).ToList();
            _connection = new SqliteConnection(_connectionString);
            await _connection.OpenAsync();
            var options = new nORM.Configuration.DbContextOptions();
            _context = new nORM.Core.DbContext(_connection, new SqliteProvider(), options);
            var createUserTableSql = @"
                CREATE TABLE BenchmarkUser (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Email TEXT NOT NULL,
                    CreatedAt TEXT NOT NULL,
                    IsActive INTEGER NOT NULL,
                    Age INTEGER NOT NULL,
                    City TEXT NOT NULL,
                    Department TEXT,
                    Salary REAL
                );
                CREATE INDEX IX_User_IsActive ON BenchmarkUser (IsActive);
                CREATE INDEX IX_User_AgeCity ON BenchmarkUser (Age, City);";
            var createOrderTableSql = @"
                CREATE TABLE BenchmarkOrder (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UserId INTEGER NOT NULL,
                    Amount REAL NOT NULL,
                    OrderDate TEXT NOT NULL,
                    ProductName TEXT NOT NULL
                );
                CREATE INDEX IX_Order_UserId ON BenchmarkOrder (UserId);";
            await _connection.ExecuteAsync(createUserTableSql);
            await _connection.ExecuteAsync(createOrderTableSql);
            _normSimpleCompiled = Norm.CompileQuery<nORM.Core.DbContext, int, BenchmarkUser>(
                (c, take) => c.Query<BenchmarkUser>().Where(u => u.IsActive == true).AsNoTracking().Take(take));
            _normComplexCompiled = Norm.CompileQuery<nORM.Core.DbContext, (int, string), BenchmarkUser>(
                (c, p) => c.Query<BenchmarkUser>()
                    .Where(u => u.IsActive == true && u.Age > p.Item1 && u.City == p.Item2)
                    .AsNoTracking().OrderBy(u => u.Name).Skip(5).Take(20));
            await _context.BulkInsertAsync(_testUsers);
            await _context.BulkInsertAsync(_testOrders);
            Console.WriteLine("✅ Fast nORM benchmark setup complete!");
        }
        private static string GetRandomCity(Random random)
        {
            var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney" };
            return cities[random.Next(cities.Length)];
        }
        private static string GetRandomDepartment(Random random)
        {
            var departments = new[] { "Sales", "Engineering", "HR", "Marketing" };
            return departments[random.Next(departments.Length)];
        }
        // This method will run before each iteration of the ExecuteUpdate and ExecuteDelete benchmarks
        // In FastNormBenchmarks.cs
        [IterationSetup(Targets = new[] { nameof(ExecuteUpdate), nameof(ExecuteDelete) })]
        public void SetupForUpdateDelete()
        {
            // Quickly reset a couple of records to a known state
            GetContext().Connection.ExecuteAsync("UPDATE BenchmarkUser SET Name = 'User 1' WHERE Id = 1").GetAwaiter().GetResult();
            GetContext().Connection.ExecuteAsync("INSERT OR IGNORE INTO BenchmarkUser (Id, Name, Email, CreatedAt, IsActive, Age, City, Department, Salary) VALUES (2, 'User 2', 'user2@example.com', '2023-01-01', 1, 30, 'New York', 'Sales', 50000)").GetAwaiter().GetResult();
        }
        // ========== Read Queries (Ad-hoc) ==========
        [Benchmark]
        public Task<List<BenchmarkUser>> Query_Simple() => NormAsyncExtensions.ToListAsync(GetContext().Query<BenchmarkUser>().Where(u => u.IsActive).Take(10));
        [Benchmark]
        public Task<List<BenchmarkUser>> Query_Complex() => NormAsyncExtensions.ToListAsync(GetContext().Query<BenchmarkUser>().Where(u => u.IsActive && u.Age > 25).OrderBy(u => u.Name).Skip(2).Take(10));
        [Benchmark]
        public Task<List<object>> Query_Join() => NormAsyncExtensions.ToListAsync(GetContext().Query<BenchmarkUser>().Join(GetContext().Query<BenchmarkOrder>(), u => u.Id, o => o.UserId, (u, o) => new { u.Name, o.Amount, o.ProductName }).Where(x => x.Amount > 100).Take(20)).ContinueWith(t => t.Result.Cast<object>().ToList());
        [Benchmark]
        public Task<List<BenchmarkUser>> Query_Complex_NoResults() => NormAsyncExtensions.ToListAsync(GetContext().Query<BenchmarkUser>().Where(u => u.Age > 9999));
        // ========== Read Queries (Compiled) ==========
        [Benchmark]
        public Task<List<BenchmarkUser>> Query_Simple_Compiled() => _normSimpleCompiled!(GetContext(), 10);
        [Benchmark]
        public Task<List<BenchmarkUser>> Query_Complex_Compiled() => _normComplexCompiled!(GetContext(), (25, "New York"));
        // ========== Aggregates & Streaming ==========
        [Benchmark]
        public Task<int> Count_Operation() => NormAsyncExtensions.CountAsync(GetContext().Query<BenchmarkUser>().Where(u => u.IsActive));
        [Benchmark]
        public async Task<int> Query_Simple_Streaming()
        {
            var count = 0;
            // This is the query that needs to be passed to the static method
            var query = GetContext().Query<BenchmarkUser>().Where(u => u.IsActive).Take(10);
            // Call your extension method directly to resolve the ambiguity
            await foreach (var user in NormAsyncExtensions.AsAsyncEnumerable(query))
            {
                count++;
            }
            return count;
        }
        // ========== CUD Operations ==========
        [Benchmark]
        public Task Insert_Single()
        {
            var user = new BenchmarkUser { Name = "Temp User", Email = "temp@norm.com", CreatedAt = DateTime.Now, IsActive = true, Age = 30, City = "TempCity", Department = "Temp" };
            return GetContext().InsertAsync(user);
        }
        [Benchmark]
        public Task BulkInsert_Small()
        {
            var users = Enumerable.Range(1, 50).Select(i => new BenchmarkUser { Name = $"Bulk User {i}", Email = $"bulk{i}@norm.com", CreatedAt = DateTime.Now, IsActive = true, Age = 30, City = "BulkCity", Department = "Bulk" }).ToList();
            return GetContext().BulkInsertAsync(users);
        }
        [Benchmark]
        public Task ExecuteUpdate()
        {
            var query = GetContext().Query<BenchmarkUser>().Where(u => u.Id == 1);
            return NormAsyncExtensions.ExecuteUpdateAsync(query, s => s.SetProperty(p => p.Name, "Updated Name"));
        }
        [Benchmark]
        public Task ExecuteDelete()
        {
            var query = GetContext().Query<BenchmarkUser>().Where(u => u.Id == 2);
            return NormAsyncExtensions.ExecuteDeleteAsync(query);
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