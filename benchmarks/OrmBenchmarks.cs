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

        private readonly string _efConnectionString = "Data Source=benchmark.db";
        private readonly string _nOrmConnectionString = "Data Source=benchmark.db";
        private readonly string _dapperConnectionString = "Data Source=benchmark.db";

        private EfCoreContext? _efContext;
        private SqliteConnection? _nOrmConnection;
        private nORM.Core.DbContext? _nOrmContext;
        private SqliteConnection? _dapperConnection;

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

            // Setup all databases
            await SetupDatabase();

            Console.WriteLine("✅ Benchmark setup complete!");
        }

        private static string GetRandomCity(Random random)
        {
            var cities = new[] { "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Madrid" };
            return cities[random.Next(cities.Length)];
        }

        private static string GetRandomDepartment(Random random)
        {
            var departments = new[] { "Sales", "Engineering", "HR", "Marketing" };
            return departments[random.Next(departments.Length)];
        }

        private async Task SetupDatabase()
        {
            // Delete existing database
            if (System.IO.File.Exists("benchmark.db"))
                System.IO.File.Delete("benchmark.db");

            // Use EF Core to create the database and initial data
            _efContext = new EfCoreContext(_efConnectionString);
            await _efContext.Database.OpenConnectionAsync();
            await _efContext.Database.EnsureCreatedAsync();
            
            // 1. Add and save the users FIRST. This generates their IDs.
            _efContext.Users.AddRange(_testUsers);
            await _efContext.SaveChangesAsync();

            // 2. Now that users exist, add and save the orders.
            _efContext.Orders.AddRange(_testOrders);
            await _efContext.SaveChangesAsync();

            _efContext.ChangeTracker.Clear();

            // Setup nORM connection
            var options = new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50, // Optimized for SQLite parameter limits
                TimeoutConfiguration = { BaseTimeout = TimeSpan.FromSeconds(30) }
            };

            _nOrmConnection = new SqliteConnection(_nOrmConnectionString);
            await _nOrmConnection.OpenAsync();
            _nOrmContext = new nORM.Core.DbContext(_nOrmConnection, new SqliteProvider(), options);

            // Setup Dapper connection
            _dapperConnection = new SqliteConnection(_dapperConnectionString);
            await _dapperConnection.OpenAsync();
        }

        // ========== SINGLE INSERT BENCHMARKS ==========

        [Benchmark]
        public async Task Insert_Single_EfCore()
        {
            await using var context = new EfCoreContext(_efConnectionString);

            var user = new BenchmarkUser
            {
                Name = "Test User EF",
                Email = "test@ef.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity",
                Department = "TestDept",
                Salary = 50_000
            };

            context.Users.Add(user);
            await context.SaveChangesAsync();
        }

        [Benchmark]
        public async Task Insert_Single_nORM()
        {
            var options = new nORM.Configuration.DbContextOptions();
            await using var context = new nORM.Core.DbContext(_nOrmConnectionString, new SqliteProvider(), options);

            var user = new BenchmarkUser
            {
                Name = "Test User nORM",
                Email = "test@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity",
                Department = "TestDept",
                Salary = 50_000
            };

            await context.InsertAsync(user);
        }

        [Benchmark]
        public async Task Insert_Single_Dapper()
        {
            await using var connection = new SqliteConnection(_dapperConnectionString);
            await connection.OpenAsync();

            var user = new BenchmarkUser
            {
                Name = "Test User Dapper",
                Email = "test@dapper.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity",
                Department = "TestDept",
                Salary = 50_000
            };

            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

            await connection.ExecuteAsync(sql, user);
        }

        [Benchmark]
        public async Task Insert_Single_RawAdo()
        {
            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";
            using var command = _dapperConnection!.CreateCommand();
            command.CommandText = sql;
            command.Parameters.AddWithValue("@Name", "Test User ADO");
            command.Parameters.AddWithValue("@Email", "test@ado.com");
            command.Parameters.AddWithValue("@CreatedAt", DateTime.Now.ToString("O"));
            command.Parameters.AddWithValue("@IsActive", 1);
            command.Parameters.AddWithValue("@Age", 25);
            command.Parameters.AddWithValue("@City", "TestCity");
            command.Parameters.AddWithValue("@Department", "TestDept");
            command.Parameters.AddWithValue("@Salary", 50_000);

            await command.ExecuteNonQueryAsync();
        }

        // ========== QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(_efContext!.Users
                .Where(u => u.IsActive)
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_nORM()
        {
            return await NormAsyncExtensions.ToListAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Where(u => u.IsActive)
                .AsNoTracking()
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_Dapper()
        {
            var sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql);
            return result.ToList();
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_RawAdo()
        {
            var sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            using var command = _dapperConnection!.CreateCommand();
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
                    City = reader.GetString("City"),
                    Department = reader.GetString("Department"),
                    Salary = reader.GetDouble("Salary")
                });
            }

            return users;
        }

        // ========== COMPLEX QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(_efContext!.Users
                .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_nORM()
        {
            return await NormAsyncExtensions.ToListAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Where(u => u.IsActive && u.Age > 25 && u.City == "New York")
                .AsNoTracking()
                .OrderBy(u => u.Name)
                .Skip(5)
                .Take(20));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_Dapper()
        {
            var sql = @"
                SELECT * FROM BenchmarkUser
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name
                LIMIT 20 OFFSET 5";

            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql, new { Age = 25, City = "New York" });
            return result.ToList();
        }

        // ========== JOIN BENCHMARKS ==========

        [Benchmark]
        public async Task<List<object>> Query_Join_EfCore()
        {
            var result = await EfQueryableExtensions.ToListAsync(_efContext!.Users
                .Join(_efContext.Orders, u => u.Id, o => o.UserId, (u, o) => new { u.Name, o.Amount, o.ProductName })
                .Where(x => x.Amount > 100)
                .Take(50));
            return result.Cast<object>().ToList();
        }

        [Benchmark]
        public async Task<List<object>> Query_Join_nORM()
        {
            var result = await NormAsyncExtensions.ToListAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Join(
                    _nOrmContext!.Query<BenchmarkOrder>(),
                    u => u.Id,
                    o => o.UserId,
                    (u, o) => new { u.Name, o.Amount, o.ProductName }
                )
                .AsNoTracking()
                .Where(x => x.Amount > 100)
                .Take(50));
            return result.Cast<object>().ToList();
        }

        [Benchmark]
        public async Task<List<object>> Query_Join_Dapper()
        {
            var sql = @"
                SELECT u.Name, o.Amount, o.ProductName
                FROM BenchmarkUser u
                INNER JOIN BenchmarkOrder o ON u.Id = o.UserId
                WHERE o.Amount > @Amount
                LIMIT 50";

            var result = await _dapperConnection!.QueryAsync(sql, new { Amount = 100 });
            return result.Cast<object>().ToList();
        }

        // ========== COUNT BENCHMARKS ==========

        [Benchmark]
        public async Task<int> Count_EfCore()
        {
            return await EfQueryableExtensions.CountAsync(_efContext!.Users, u => u.IsActive);
        }

        [Benchmark]
        public async Task<int> Count_nORM()
        {
            return await NormAsyncExtensions.CountAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Where(u => u.IsActive));
        }

        [Benchmark]
        public async Task<int> Count_Dapper()
        {
            var sql = "SELECT COUNT(*) FROM BenchmarkUser WHERE IsActive = 1";
            return await _dapperConnection!.QuerySingleAsync<int>(sql);
        }

        // ========== BULK OPERATIONS BENCHMARKS ==========

        [Benchmark]
        public async Task BulkInsert_EfCore()
        {
            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User EF {i}",
                Email = $"bulk{i}@ef.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity",
                Department = "BulkDept",
                Salary = 60_000
            }).ToList();

            _efContext!.Users.AddRange(users);
            await _efContext.SaveChangesAsync();
            _efContext.ChangeTracker.Clear();
        }

        [Benchmark]
        public async Task BulkInsert_nORM()
        {
            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User nORM {i}",
                Email = $"bulk{i}@norm.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity",
                Department = "BulkDept",
                Salary = 60_000
            }).ToList();

            await _nOrmContext!.BulkInsertAsync(users);
        }

        [Benchmark]
        public async Task BulkInsert_Dapper()
        {
            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Bulk User Dapper {i}",
                Email = $"bulk{i}@dapper.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity",
                Department = "BulkDept",
                Salary = 60_000
            }).ToList();

            var sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

            await using var transaction = await _dapperConnection!.BeginTransactionAsync();
            await _dapperConnection!.ExecuteAsync(sql, users, transaction);
            await transaction.CommitAsync();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _efContext?.Dispose();
            _nOrmContext?.Dispose();
            _nOrmConnection?.Dispose();
            _dapperConnection?.Dispose();

            // Clean up database file
            try
            {
                if (System.IO.File.Exists("benchmark.db"))
                    System.IO.File.Delete("benchmark.db");
            }
            catch { /* Ignore cleanup errors */ }
        }
    }
}

