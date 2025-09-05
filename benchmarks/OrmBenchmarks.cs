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

        private static BenchmarkUser NewUser(string source)
        {
            return new BenchmarkUser
            {
                Name = $"Test User {source}",
                Email = $"test@{source.ToLower()}.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 25,
                City = "TestCity",
                Department = "TestDept",
                Salary = 50_000
            };
        }

        private static void AddUserParams(SqliteCommand cmd, BenchmarkUser user)
        {
            cmd.Parameters.Add(new SqliteParameter("@Name", DbType.String) { Value = user.Name });
            cmd.Parameters.Add(new SqliteParameter("@Email", DbType.String) { Value = user.Email });
            cmd.Parameters.Add(new SqliteParameter("@CreatedAt", DbType.DateTime) { Value = user.CreatedAt });
            cmd.Parameters.Add(new SqliteParameter("@IsActive", DbType.Int32) { Value = user.IsActive ? 1 : 0 });
            cmd.Parameters.Add(new SqliteParameter("@Age", DbType.Int32) { Value = user.Age });
            cmd.Parameters.Add(new SqliteParameter("@City", DbType.String) { Value = user.City });
            cmd.Parameters.Add(new SqliteParameter("@Department", DbType.String) { Value = user.Department });
            cmd.Parameters.Add(new SqliteParameter("@Salary", DbType.Decimal) { Value = user.Salary });
        }

        // ========== SINGLE INSERT BENCHMARKS ==========

        [Benchmark]
        public async Task Insert_Single_EfCore()
        {
            var userEf = NewUser("EF");
            _efContext!.Users.Add(userEf);
            await _efContext.SaveChangesAsync();
            _efContext.ChangeTracker.Clear();
        }

        [Benchmark]
        public async Task Insert_Single_nORM()
        {
            var userNorm = NewUser("nORM");
            await _nOrmContext!.InsertAsync(userNorm);
        }

        [Benchmark]
        public async Task Insert_Single_Dapper()
        {
            var userDap = NewUser("Dapper");
            const string sql = @"INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                     VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";
            await _dapperConnection!.ExecuteAsync(sql, userDap);
        }

        [Benchmark]
        public async Task Insert_Single_RawAdo()
        {
            const string sql = @"INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                     VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";
            await using var cmd = _dapperConnection!.CreateCommand();
            cmd.CommandText = sql;
            AddUserParams(cmd, NewUser("ADO"));
            await cmd.ExecuteNonQueryAsync();
        }

        // ========== QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(
                EfQueryableExtensions.AsNoTracking(_efContext!.Users)
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
            var sql = "SELECT Id, Name, Email, CreatedAt, IsActive, Age, City, Department, Salary FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql);
            return result.ToList();
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_RawAdo()
        {
            var sql = "SELECT Id, Name, Email, CreatedAt, IsActive, Age, City, Department, Salary FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            using var command = _dapperConnection!.CreateCommand();
            command.CommandText = sql;

            using var reader = await command.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();

            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                    Name = reader.GetString(reader.GetOrdinal("Name")),
                    Email = reader.GetString(reader.GetOrdinal("Email")),
                    CreatedAt = reader.GetDateTime(reader.GetOrdinal("CreatedAt")),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDecimal(reader.GetOrdinal("Salary"))
                });
            }

            return users;
        }

        // ========== COMPLEX QUERY BENCHMARKS ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(
                EfQueryableExtensions.AsNoTracking(_efContext!.Users)
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
                SELECT Id, Name, Email, CreatedAt, IsActive, Age, City, Department, Salary
                FROM BenchmarkUser
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name
                LIMIT 20 OFFSET 5";

            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql, new { Age = 25, City = "New York" });
            return result.ToList();
        }

        // ========== JOIN BENCHMARKS ==========

        [Benchmark]
        public async Task<List<JoinDto>> Query_Join_EfCore()
        {
            var result = await EfQueryableExtensions.ToListAsync(
                EfQueryableExtensions.AsNoTracking(_efContext!.Users)
                    .Join(
                        EfQueryableExtensions.AsNoTracking(_efContext.Orders),
                        u => u.Id,
                        o => o.UserId,
                        (u, o) => new JoinDto { Name = u.Name, Amount = o.Amount, ProductName = o.ProductName })
                    .Where(x => x.Amount > 100)
                    .Take(50));
            return result;
        }

        [Benchmark]
        public async Task<List<JoinDto>> Query_Join_nORM()
        {
            var result = await NormAsyncExtensions.ToListAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Join(
                    _nOrmContext.Query<BenchmarkOrder>(),
                    u => u.Id,
                    o => o.UserId,
                    (u, o) => new JoinDto { Name = u.Name, Amount = o.Amount, ProductName = o.ProductName }
                )
                .AsNoTracking()
                .Where(x => x.Amount > 100)
                .Take(50));
            return result;
        }

        [Benchmark]
        public async Task<List<JoinDto>> Query_Join_Dapper()
        {
            var sql = @"
                SELECT u.Name, o.Amount, o.ProductName
                FROM BenchmarkUser u
                INNER JOIN BenchmarkOrder o ON u.Id = o.UserId
                WHERE o.Amount > @Amount
                LIMIT 50";

            var result = await _dapperConnection!.QueryAsync<JoinDto>(sql, new { Amount = 100 });
            return result.ToList();
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

    public class JoinDto
    {
        public string Name { get; set; } = "";
        public decimal Amount { get; set; }
        public string ProductName { get; set; } = "";
    }
}

