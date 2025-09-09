using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using nORM.Core;
using nORM.Providers;
using EfDbContext = Microsoft.EntityFrameworkCore.DbContext;
using EfQueryableExtensions = Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions;
using NormAsyncExtensions = nORM.Core.NormAsyncExtensions;
using static Microsoft.EntityFrameworkCore.EF;

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

        public DbSet<BenchmarkUser> Users { get; set; }
        public DbSet<BenchmarkOrder> Orders { get; set; }

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
                entity.HasIndex(e => e.IsActive);
                entity.HasIndex(e => e.Age);
                entity.HasIndex(e => e.City);
            });

            modelBuilder.Entity<BenchmarkOrder>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedOnAdd();
                entity.ToTable("BenchmarkOrder");
                entity.HasIndex(e => e.UserId);
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

        // Prepared commands (for prepared/compiled-equivalent benchmarks)
        private SqliteCommand? _dapperSimplePrepared;
        private SqliteParameter? _dapperSimpleTakeParam;

        private SqliteCommand? _dapperComplexPrepared;
        private SqliteParameter? _dapperComplexAgeParam;
        private SqliteParameter? _dapperComplexCityParam;

        private SqliteCommand? _adoSimplePrepared;
        private SqliteParameter? _adoSimpleTakeParam;

        private SqliteCommand? _adoComplexPrepared;
        private SqliteParameter? _adoComplexAgeParam;
        private SqliteParameter? _adoComplexCityParam;

        // EF Core compiled queries
        private static readonly Func<EfCoreContext, int, IAsyncEnumerable<BenchmarkUser>> _efSimpleCompiled
            = CompileAsyncQuery((EfCoreContext ctx, int take)
                => ctx.Users.AsNoTracking().Where(u => u.IsActive).Take(take));

        private static readonly Func<EfCoreContext, int, string, IAsyncEnumerable<BenchmarkUser>> _efComplexCompiled
            = CompileAsyncQuery((EfCoreContext ctx, int age, string city)
                => ctx.Users.AsNoTracking()
                    .Where(u => u.IsActive && u.Age > age && u.City == city)
                    .OrderBy(u => u.Name)
                    .Skip(5)
                    .Take(20));

        // nORM compiled queries (via Norm.CompileQuery)
        private static readonly Func<nORM.Core.DbContext, int, Task<List<BenchmarkUser>>> _normSimpleCompiled
            = Norm.CompileQuery<nORM.Core.DbContext, int, BenchmarkUser>(
                (c, take) => c.Query<BenchmarkUser>().Where(u => u.IsActive == true).AsNoTracking().Take(take));

        private static readonly Func<nORM.Core.DbContext, (int, string), Task<List<BenchmarkUser>>> _normComplexCompiled
            = Norm.CompileQuery<nORM.Core.DbContext, (int, string), BenchmarkUser>(
                (c, p) => c.Query<BenchmarkUser>()
                    .Where(u => u.IsActive == true && u.Age > p.Item1 && u.City == p.Item2)
                    .AsNoTracking()
                    .OrderBy(u => u.Name)
                    .Skip(5)
                    .Take(20));

        // ---------- SQLite PRAGMA helpers (applied uniformly once per connection) ----------
        private static void ApplySqlitePragmas(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA foreign_keys = ON;
                PRAGMA temp_store = MEMORY;
                PRAGMA cache_size = -20000;
            ";
            cmd.ExecuteNonQuery();
        }

        private static async Task ApplySqlitePragmasAsync(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA foreign_keys = ON;
                PRAGMA temp_store = MEMORY;
                PRAGMA cache_size = -20000;
            ";
            await cmd.ExecuteNonQueryAsync();
        }

        [GlobalSetup]
        public async Task Setup()
        {
            Console.WriteLine("🔧 Setting up benchmark databases...");

            // Generate test data (deterministic)
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
            if (System.IO.File.Exists("benchmark.db"))
                System.IO.File.Delete("benchmark.db");

            // EF create + seed (so schema/indexes are shared)
            _efContext = new EfCoreContext(_efConnectionString);
            await _efContext.Database.OpenConnectionAsync();
            var efConn = (SqliteConnection)_efContext.Database.GetDbConnection();
            ApplySqlitePragmas(efConn);
            await _efContext.Database.EnsureCreatedAsync();

            _efContext.Users.AddRange(_testUsers);
            await _efContext.SaveChangesAsync();

            _efContext.Orders.AddRange(_testOrders);
            await _efContext.SaveChangesAsync();
            _efContext.ChangeTracker.Clear();

            // nORM setup
            var options = new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50,
                TimeoutConfiguration = { BaseTimeout = TimeSpan.FromSeconds(30) }
            };
            _nOrmConnection = new SqliteConnection(_nOrmConnectionString);
            await _nOrmConnection.OpenAsync();
            await ApplySqlitePragmasAsync(_nOrmConnection);
            _nOrmContext = new nORM.Core.DbContext(_nOrmConnection, new SqliteProvider(), options);

            // Dapper/ADO shared connection
            _dapperConnection = new SqliteConnection(_dapperConnectionString);
            await _dapperConnection.OpenAsync();
            await ApplySqlitePragmasAsync(_dapperConnection);

            // Prepared statements (once)
            // Simple
            _dapperSimplePrepared = _dapperConnection.CreateCommand();
            _dapperSimplePrepared.CommandText = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT @Take";
            _dapperSimpleTakeParam = _dapperSimplePrepared.CreateParameter();
            _dapperSimpleTakeParam.ParameterName = "@Take";
            _dapperSimplePrepared.Parameters.Add(_dapperSimpleTakeParam);
            _dapperSimplePrepared.Prepare();

            _adoSimplePrepared = _dapperConnection.CreateCommand();
            _adoSimplePrepared.CommandText = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT @Take";
            _adoSimpleTakeParam = _adoSimplePrepared.CreateParameter();
            _adoSimpleTakeParam.ParameterName = "@Take";
            _adoSimplePrepared.Parameters.Add(_adoSimpleTakeParam);
            _adoSimplePrepared.Prepare();

            // Complex
            _dapperComplexPrepared = _dapperConnection.CreateCommand();
            _dapperComplexPrepared.CommandText = @"
                SELECT * FROM BenchmarkUser
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name
                LIMIT 20 OFFSET 5";
            _dapperComplexAgeParam = _dapperComplexPrepared.CreateParameter();
            _dapperComplexAgeParam.ParameterName = "@Age";
            _dapperComplexCityParam = _dapperComplexPrepared.CreateParameter();
            _dapperComplexCityParam.ParameterName = "@City";
            _dapperComplexPrepared.Parameters.Add(_dapperComplexAgeParam);
            _dapperComplexPrepared.Parameters.Add(_dapperComplexCityParam);
            _dapperComplexPrepared.Prepare();

            _adoComplexPrepared = _dapperConnection.CreateCommand();
            _adoComplexPrepared.CommandText = @"
                SELECT * FROM BenchmarkUser
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name
                LIMIT 20 OFFSET 5";
            _adoComplexAgeParam = _adoComplexPrepared.CreateParameter();
            _adoComplexAgeParam.ParameterName = "@Age";
            _adoComplexCityParam = _adoComplexPrepared.CreateParameter();
            _adoComplexCityParam.ParameterName = "@City";
            _adoComplexPrepared.Parameters.Add(_adoComplexAgeParam);
            _adoComplexPrepared.Parameters.Add(_adoComplexCityParam);
            _adoComplexPrepared.Prepare();
        }

        // ========== SINGLE INSERT (reuse contexts/connections; no per-op PRAGMAs) ==========

        [Benchmark]
        public async Task Insert_Single_EfCore()
        {
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

            _efContext!.Users.Add(user);
            await _efContext.SaveChangesAsync();
            _efContext.ChangeTracker.Clear();
        }

        [Benchmark]
        public async Task Insert_Single_nORM()
        {
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

            await _nOrmContext!.InsertAsync(user);
        }

        [Benchmark]
        public async Task Insert_Single_Dapper()
        {
            const string sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

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

            await _dapperConnection!.ExecuteAsync(sql, user);
        }

        [Benchmark]
        public async Task Insert_Single_RawAdo()
        {
            const string sql = @"
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

        // ========== SIMPLE QUERY (standard) ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(_efContext!.Users
                .AsNoTracking()
                .Where(u => u.IsActive)
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_nORM()
        {
            return await NormAsyncExtensions.ToListAsync(_nOrmContext!.Query<BenchmarkUser>()
                .Where(u => u.IsActive == true)
                .AsNoTracking()
                .Take(10));
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_Dapper()
        {
            const string sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql);
            return result.ToList();
        }

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Simple_RawAdo()
        {
            const string sql = "SELECT * FROM BenchmarkUser WHERE IsActive = 1 LIMIT 10";
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
                    CreatedAt = DateTime.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDouble(reader.GetOrdinal("Salary"))
                });
            }

            return users;
        }

        // ========== SIMPLE QUERY (compiled/prepared) ==========

        [Benchmark(Description = "Query Simple EF Core (Compiled)")]
        public async Task<List<BenchmarkUser>> Query_Simple_EfCore_Compiled()
        {
            var list = new List<BenchmarkUser>();
            await foreach (var u in _efSimpleCompiled(_efContext!, 10))
                list.Add(u);
            return list;
        }

        [Benchmark(Description = "Query Simple nORM (Compiled)")]
        public Task<List<BenchmarkUser>> Query_Simple_nORM_Compiled()
            => _normSimpleCompiled(_nOrmContext!, 10);

        [Benchmark(Description = "Query Simple Dapper (Prepared)")]
        public async Task<List<BenchmarkUser>> Query_Simple_Dapper_Prepared()
        {
            _dapperSimpleTakeParam!.Value = 10;
            using var reader = await _dapperSimplePrepared!.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();
            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                    Name = reader.GetString(reader.GetOrdinal("Name")),
                    Email = reader.GetString(reader.GetOrdinal("Email")),
                    CreatedAt = DateTime.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDouble(reader.GetOrdinal("Salary"))
                });
            }
            return users;
        }

        [Benchmark(Description = "Query Simple Raw ADO (Prepared)")]
        public async Task<List<BenchmarkUser>> Query_Simple_RawAdo_Prepared()
        {
            _adoSimpleTakeParam!.Value = 10;
            using var reader = await _adoSimplePrepared!.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();
            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                    Name = reader.GetString(reader.GetOrdinal("Name")),
                    Email = reader.GetString(reader.GetOrdinal("Email")),
                    CreatedAt = DateTime.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDouble(reader.GetOrdinal("Salary"))
                });
            }
            return users;
        }

        // ========== COMPLEX QUERY (standard) ==========

        [Benchmark]
        public async Task<List<BenchmarkUser>> Query_Complex_EfCore()
        {
            return await EfQueryableExtensions.ToListAsync(_efContext!.Users
                .AsNoTracking()
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
            const string sql = @"
                SELECT * FROM BenchmarkUser
                WHERE IsActive = 1 AND Age > @Age AND City = @City
                ORDER BY Name
                LIMIT 20 OFFSET 5";

            var result = await _dapperConnection!.QueryAsync<BenchmarkUser>(sql, new { Age = 25, City = "New York" });
            return result.ToList();
        }

        // ========== COMPLEX QUERY (compiled/prepared) ==========

        [Benchmark(Description = "Query Complex EF Core (Compiled)")]
        public async Task<List<BenchmarkUser>> Query_Complex_EfCore_Compiled()
        {
            var list = new List<BenchmarkUser>();
            await foreach (var u in _efComplexCompiled(_efContext!, 25, "New York"))
                list.Add(u);
            return list;
        }

        [Benchmark(Description = "Query Complex nORM (Compiled)")]
        public Task<List<BenchmarkUser>> Query_Complex_nORM_Compiled()
            => _normComplexCompiled(_nOrmContext!, (25, "New York"));

        [Benchmark(Description = "Query Complex Dapper (Prepared)")]
        public async Task<List<BenchmarkUser>> Query_Complex_Dapper_Prepared()
        {
            _dapperComplexAgeParam!.Value = 25;
            _dapperComplexCityParam!.Value = "New York";
            using var reader = await _dapperComplexPrepared!.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();
            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                    Name = reader.GetString(reader.GetOrdinal("Name")),
                    Email = reader.GetString(reader.GetOrdinal("Email")),
                    CreatedAt = DateTime.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDouble(reader.GetOrdinal("Salary"))
                });
            }
            return users;
        }

        [Benchmark(Description = "Query Complex Raw ADO (Prepared)")]
        public async Task<List<BenchmarkUser>> Query_Complex_RawAdo_Prepared()
        {
            _adoComplexAgeParam!.Value = 25;
            _adoComplexCityParam!.Value = "New York";
            using var reader = await _adoComplexPrepared!.ExecuteReaderAsync();
            var users = new List<BenchmarkUser>();
            while (await reader.ReadAsync())
            {
                users.Add(new BenchmarkUser
                {
                    Id = reader.GetInt32(reader.GetOrdinal("Id")),
                    Name = reader.GetString(reader.GetOrdinal("Name")),
                    Email = reader.GetString(reader.GetOrdinal("Email")),
                    CreatedAt = DateTime.Parse(reader.GetString(reader.GetOrdinal("CreatedAt"))),
                    IsActive = reader.GetInt32(reader.GetOrdinal("IsActive")) == 1,
                    Age = reader.GetInt32(reader.GetOrdinal("Age")),
                    City = reader.GetString(reader.GetOrdinal("City")),
                    Department = reader.GetString(reader.GetOrdinal("Department")),
                    Salary = reader.GetDouble(reader.GetOrdinal("Salary"))
                });
            }
            return users;
        }

        // ========== JOIN ==========

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
            const string sql = @"
                SELECT u.Name, o.Amount, o.ProductName
                FROM BenchmarkUser u
                INNER JOIN BenchmarkOrder o ON u.Id = o.UserId
                WHERE o.Amount > @Amount
                LIMIT 50";

            var result = await _dapperConnection!.QueryAsync(sql, new { Amount = 100 });
            return result.Cast<object>().ToList();
        }

        // ========== COUNT ==========

        [Benchmark]
        public Task<int> Count_EfCore()
            => EfQueryableExtensions.CountAsync(_efContext!.Users, u => u.IsActive);

        [Benchmark]
        public Task<int> Count_nORM()
            => NormAsyncExtensions.CountAsync(_nOrmContext!.Query<BenchmarkUser>().Where(u => u.IsActive));

        [Benchmark]
        public Task<int> Count_Dapper()
            => _dapperConnection!.QuerySingleAsync<int>("SELECT COUNT(*) FROM BenchmarkUser WHERE IsActive = 1");

        // ========== BULK INSERTS: Naive / Batched / Idiomatic ==========

        [Benchmark(Description = "BulkInsert Naive - EF per row")]
        public async Task BulkInsert_Naive_EfCore()
        {
            for (int i = 1; i <= 100; i++)
            {
                var u = new BenchmarkUser
                {
                    Name = $"Naive EF {i}",
                    Email = $"naive{i}@ef.com",
                    CreatedAt = DateTime.Now,
                    IsActive = true,
                    Age = 30,
                    City = "BulkCity",
                    Department = "BulkDept",
                    Salary = 60_000
                };
                _efContext!.Users.Add(u);
                await _efContext.SaveChangesAsync();
            }
            _efContext!.ChangeTracker.Clear();
        }

        [Benchmark(Description = "BulkInsert Naive - nORM per row")]
        public async Task BulkInsert_Naive_nORM()
        {
            for (int i = 1; i <= 100; i++)
            {
                var u = new BenchmarkUser
                {
                    Name = $"Naive nORM {i}",
                    Email = $"naive{i}@norm.com",
                    CreatedAt = DateTime.Now,
                    IsActive = true,
                    Age = 30,
                    City = "BulkCity",
                    Department = "BulkDept",
                    Salary = 60_000
                };
                await _nOrmContext!.InsertAsync(u);
            }
        }

        [Benchmark(Description = "BulkInsert Naive - Dapper per row")]
        public async Task BulkInsert_Naive_Dapper()
        {
            const string sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

            for (int i = 1; i <= 100; i++)
            {
                var u = new BenchmarkUser
                {
                    Name = $"Naive Dapper {i}",
                    Email = $"naive{i}@dapper.com",
                    CreatedAt = DateTime.Now,
                    IsActive = true,
                    Age = 30,
                    City = "BulkCity",
                    Department = "BulkDept",
                    Salary = 60_000
                };
                await _dapperConnection!.ExecuteAsync(sql, u);
            }
        }

        [Benchmark(Description = "BulkInsert Batched - EF SaveChanges in Tx")]
        public async Task BulkInsert_Batched_EfCore()
        {
            var users = Enumerable.Range(1, 100).Select(i => new BenchmarkUser
            {
                Name = $"Batch EF {i}",
                Email = $"batch{i}@ef.com",
                CreatedAt = DateTime.Now,
                IsActive = true,
                Age = 30,
                City = "BulkCity",
                Department = "BulkDept",
                Salary = 60_000
            }).ToList();

            await using var tx = await _efContext!.Database.BeginTransactionAsync();
            _efContext!.Users.AddRange(users);
            await _efContext.SaveChangesAsync();
            await tx.CommitAsync();
            _efContext.ChangeTracker.Clear();
        }

        [Benchmark(Description = "BulkInsert Batched - nORM Tx + per row")]
        public async Task BulkInsert_Batched_nORM()
        {
            await using var tx = await _nOrmConnection!.BeginTransactionAsync();
            for (int i = 1; i <= 100; i++)
            {
                var u = new BenchmarkUser
                {
                    Name = $"Batch nORM {i}",
                    Email = $"batch{i}@norm.com",
                    CreatedAt = DateTime.Now,
                    IsActive = true,
                    Age = 30,
                    City = "BulkCity",
                    Department = "BulkDept",
                    Salary = 60_000
                };
                await _nOrmContext!.InsertAsync(u, tx);
            }
            await tx.CommitAsync();
        }

        [Benchmark(Description = "BulkInsert Batched - Dapper prepared")]
        public async Task BulkInsert_Batched_Dapper()
        {
            const string sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

            await using var tx = await _dapperConnection!.BeginTransactionAsync();

            using var cmd = _dapperConnection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = (SqliteTransaction)tx;

            var pName = cmd.CreateParameter(); pName.ParameterName = "@Name"; cmd.Parameters.Add(pName);
            var pEmail = cmd.CreateParameter(); pEmail.ParameterName = "@Email"; cmd.Parameters.Add(pEmail);
            var pCreated = cmd.CreateParameter(); pCreated.ParameterName = "@CreatedAt"; cmd.Parameters.Add(pCreated);
            var pActive = cmd.CreateParameter(); pActive.ParameterName = "@IsActive"; cmd.Parameters.Add(pActive);
            var pAge = cmd.CreateParameter(); pAge.ParameterName = "@Age"; cmd.Parameters.Add(pAge);
            var pCity = cmd.CreateParameter(); pCity.ParameterName = "@City"; cmd.Parameters.Add(pCity);
            var pDept = cmd.CreateParameter(); pDept.ParameterName = "@Department"; cmd.Parameters.Add(pDept);
            var pSalary = cmd.CreateParameter(); pSalary.ParameterName = "@Salary"; cmd.Parameters.Add(pSalary);

            for (int i = 1; i <= 100; i++)
            {
                pName.Value = $"Batch Dapper {i}";
                pEmail.Value = $"batch{i}@dapper.com";
                pCreated.Value = DateTime.Now.ToString("O");
                pActive.Value = 1;
                pAge.Value = 30;
                pCity.Value = "BulkCity";
                pDept.Value = "BulkDept";
                pSalary.Value = 60_000;

                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
        }

        [Benchmark(Description = "BulkInsert Idiomatic - EF AddRange")]
        public async Task BulkInsert_Idiomatic_EfCore()
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

        [Benchmark(Description = "BulkInsert Idiomatic - nORM BulkInsert")]
        public Task BulkInsert_Idiomatic_nORM()
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

            return _nOrmContext!.BulkInsertAsync(users);
        }

        [Benchmark(Description = "BulkInsert Idiomatic - Dapper list in Tx")]
        public async Task BulkInsert_Idiomatic_Dapper()
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

            const string sql = @"
                INSERT INTO BenchmarkUser (Name, Email, CreatedAt, IsActive, Age, City, Department, Salary)
                VALUES (@Name, @Email, @CreatedAt, @IsActive, @Age, @City, @Department, @Salary)";

            await using var transaction = await _dapperConnection!.BeginTransactionAsync();
            await _dapperConnection.ExecuteAsync(sql, users, transaction);
            await transaction.CommitAsync();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _efContext?.Dispose();
            _nOrmContext?.Dispose();
            _nOrmConnection?.Dispose();
            _dapperConnection?.Dispose();

            try
            {
                if (System.IO.File.Exists("benchmark.db"))
                    System.IO.File.Delete("benchmark.db");
            }
            catch { /* ignore */ }
        }
    }
}
