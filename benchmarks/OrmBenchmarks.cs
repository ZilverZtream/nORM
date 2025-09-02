using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Order;
using Microsoft.Data.Sqlite;
using Dapper;
using Microsoft.EntityFrameworkCore;
using nORM.Core;
using nORM.Providers;
using EfDbContext = Microsoft.EntityFrameworkCore.DbContext;
using EfQueryableExtensions = Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions;
using NormAsyncExtensions = nORM.Core.NormAsyncExtensions;

namespace nORM.Benchmarks
{

    // EF Core Context
    public class EfCoreContext : EfDbContext
    {
        private readonly string _connectionString;

        public EfCoreContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        public Microsoft.EntityFrameworkCore.DbSet<BenchmarkUser> Users { get; set; }
        public Microsoft.EntityFrameworkCore.DbSet<BenchmarkOrder> Orders { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite(_connectionString);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BenchmarkUser>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.ToTable("BenchmarkUser");
            });

            modelBuilder.Entity<BenchmarkOrder>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.ToTable("BenchmarkOrder");
            });
        }
    }

    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class OrmBenchmarks
    {
        private const int UserCount = 1000;
        private const int OrderCount = 2000;
        private List<BenchmarkUser> _testUsers = new();
        private List<BenchmarkOrder> _testOrders = new();

        private readonly string _efConnectionString = "Data Source=ef_benchmark.db";
        private readonly string _nOrmConnectionString = "Data Source=norm_benchmark.db";
        private readonly string _dapperConnectionString = "Data Source=dapper_benchmark.db";

        [GlobalSetup]
        public async Task Setup()
        {
            Console.WriteLine("🔧 Setting up benchmark databases...");

            // Generate test data
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

            // Setup all databases
            await SetupEfCore();
            await SetupnORM();
            await SetupDapper();

            Console.WriteLine("✅ Benchmark setup complete!");
        }

        private static string GetRandomCity(Random random)
        {
            var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Madrid" };
            return cities[random.Next(cities.Length)];
        }

        private async Task SetupEfCore()
        {
            // Delete existing database
            if (System.IO.File.Exists("ef_benchmark.db"))
                System.IO.File.Delete("ef_benchmark.db");

            using var context = new EfCoreContext(_efConnectionString);
            await context.Database.EnsureCreatedAsync();

            context.Users.AddRange(_testUsers);
            context.Orders.AddRange(_testOrders);
            await context.SaveChangesAsync();
        }

        private async Task SetupnORM()
        {
            // Delete existing database
            if (System.IO.File.Exists("norm_benchmark.db"))
                System.IO.File.Delete("norm_benchmark.db");

            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();

            var options = new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50, // Optimized for SQLite parameter limits
                CommandTimeout = TimeSpan.FromSeconds(30)
            };

            using var context = new nORM.Core.DbContext(connection, new SqliteProvider(), options);

            // Create tables manually
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

            await connection.ExecuteAsync(createUserTableSql);
            await connection.ExecuteAsync(createOrderTableSql);

            // Insert test data using nORM
            await context.BulkInsertAsync(_testUsers);
            await context.BulkInsertAsync(_testOrders);
        }

        private async Task SetupDapper()
        {
            // Delete existing database
            if (System.IO.File.Exists("dapper_benchmark.db"))
                System.IO.File.Delete("dapper_benchmark.db");

            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

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

            await connection.ExecuteAsync(createUserTableSql);
            await connection.ExecuteAsync(createOrderTableSql);

            var insertUserSql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City) 
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City)";

            var insertOrderSql = @"
                INSERT INTO BenchmarkOrder (UserId, Amount, OrderDate, ProductName) 
                VALUES (@UserId, @Amount, @OrderDate, @ProductName)";

            await connection.ExecuteAsync(insertUserSql, _testUsers);
            await connection.ExecuteAsync(insertOrderSql, _testOrders);
        }

        // ========== SINGLE INSERT BENCHMARKS ==========

        [Benchmark]
        public async Task Insert_Single_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            var user = new BenchmarkUser
            {
                Name = "Test User EF",
                Email = "test@ef.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity"
            };

            context.Users.Add(user);
            await context.SaveChangesAsync();
        }

        [Benchmark]
        public async Task Insert_Single_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            var user = new BenchmarkUser
            {
                Name = "Test User nORM",
                Email = "test@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity"
            };

            await context.InsertAsync(user);
        }

        [Benchmark]
        public async Task Insert_Single_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var user = new BenchmarkUser
            {
                Name = "Test User Dapper",
                Email = "test@dapper.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity"
            };

            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City) 
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City)";

            await connection.ExecuteAsync(sql, user);
        }

        [Benchmark]
        public async Task Insert_Single_RawAdo()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City) 
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City)";

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.Parameters.AddWithValue("@Name", "Test User ADO");
            command.Parameters.AddWithValue("@Email", "test@ado.com");
            command.Parameters.AddWithValue("@CreatedAt", DateTime.Now.ToString("O"));
            command.Parameters.AddWithValue("@IsActive", 1);
            command.Parameters.AddWithValue("@Age", 25);
            command.Parameters.AddWithValue("@City", "TestCity");

            await command.ExecuteNonQueryAsync();
        }

        // ========== QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            return await EfQueryableExtensions.ToListAsync(context.Users
                .Where(u => u.IsActive)
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            return await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive)
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            var result = await connection.QueryAsync<BenchmarkUser>(sql);
            return result.ToList();
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_RawAdo()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            using var command = connection.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();

            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32("Id"),
                    Name = reader.GetString("Name"),
                    Email = reader.GetString("Email"),
                    CreatedAt = DateTime.Parse(reader.GetString("CreatedAt")),
                    IsActive = reader.GetInt32("IsActive") == 1,
                    Age = reader.GetInt32("Age"),
                    City = reader.GetString("City")
                });
            }

            return users;
        }

        // ========== COMPLEX QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            return await EfQueryableExtensions.ToListAsync(context.Users
                .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            return await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = @"
                SELECT * FROM BenchmarkUser 
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name 
                LIMIT 20 OFFSET 5";

            var result = await connection.QueryAsync<BenchmarkUser>(sql, new { Age = 25, City = "New York" });
            return result.ToList();
        }

        // ========== JOIN BENCHMARKS ==========

        [Benchmark]
        public async Task<List<object>> Query_Join_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            var result = await EfQueryableExtensions.ToListAsync(context.Users
                .Join(context.Orders, u => u.Id, o => o.UserId, (u, o) => new { u.Name, o.Amount, o.ProductName })
                .Where(x => x.Amount > 100)
                .Take(50));
            return result.Cast<object>().ToList();
        }

        [Benchmark]
        public async Task<List<object>> Query_Join_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            var result = await NormAsyncExtensions.ToListAsync(context.Query<BenchmarkUser>()
                .Join(
                    context.Query<BenchmarkOrder>(),
                    u => u.Id,
                    o => o.UserId,
                    (u, o) => new { u.Name, o.Amount, o.ProductName }
                )
                .Where(x => x.Amount > 100)
                .Take(50));
            return result.Cast<object>().ToList();
        }

        [Benchmark]
        public async Task<List<object>> Query_Join_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = @"
                SELECT u.Name, o.Amount, o.ProductName
                FROM BenchmarkUser u 
                INNER JOIN BenchmarkOrder o ON u.Id = o.UserId
                WHERE o.Amount > @Amount
                LIMIT 50";

            var result = await connection.QueryAsync(sql, new { Amount = 100 });
            return result.Cast<object>().ToList();
        }

        // ========== COUNT BENCHMARKS ==========

        [Benchmark]
        public async Task<int> Count_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            return await EfQueryableExtensions.CountAsync(context.Users, u => u.IsActive);
        }

        [Benchmark]
        public async Task<int> Count_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            return await NormAsyncExtensions.CountAsync(context.Query<BenchmarkUser>()
                .Where(u => u.IsActive));
        }

        [Benchmark]
        public async Task<int> Count_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var sql = "SELECT COUNT(*) FROM BenchmarkUser WHERE IsActive = 1";
            return await connection.QuerySingleAsync<int>(sql);
        }

        // ========== BULK OPERATIONS BENCHMARKS ==========

        [Benchmark]
        public async Task BulkInsert_EfCore()
        {
            using var context = new EfCoreContext(_efConnectionString);
            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User EF {i}",
                Email = $"bulk{i}@ef.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity"
            }).ToList();

            context.Users.AddRange(users);
            await context.SaveChangesAsync();
        }

        [Benchmark]
        public async Task BulkInsert_nORM()
        {
            using var connection = new SqliteConnection(_nOrmConnectionString);
            await connection.OpenAsync();
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider());

            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User nORM {i}",
                Email = $"bulk{i}@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity"
            }).ToList();

            await context.BulkInsertAsync(users);
        }

        [Benchmark]
        public async Task BulkInsert_Dapper()
        {
            using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User Dapper {i}",
                Email = $"bulk{i}@dapper.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity"
            }).ToList();

            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City) 
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City)";

            await connection.ExecuteAsync(sql, users);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            // Clean up database files
            try
            {
                if (System.IO.File.Exists("ef_benchmark.db"))
                    System.IO.File.Delete("ef_benchmark.db");
                if (System.IO.File.Exists("norm_benchmark.db"))
                    System.IO.File.Delete("norm_benchmark.db");
                if (System.IO.File.Exists("dapper_benchmark.db"))
                    System.IO.File.Delete("dapper_benchmark.db");
            }
            catch { /* Ignore cleanup errors */ }
        }
    }
}