using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Order;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using nORM.Core;
using nORM.Providers;
using nORM.Navigation;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

// Namespace aliases to avoid ambiguity between EF Core and nORM extension methods
using EfExtensions = Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions;
using NormAsync = nORM.Core.NormAsyncExtensions;
using NormAdvanced = nORM.Core.AdvancedLinqExtensions;
using DbContext = nORM.Core.DbContext;

namespace nORM.Benchmarks
{
    /// <summary>
    /// Advanced benchmarks testing the new aggregate and navigation property features
    /// Ensures that our enterprise-grade features maintain nORM's performance characteristics
    /// </summary>
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    public class AdvancedFeaturesBenchmarks
    {
        private DbContext? _nOrmContext;
        private TestDbContext? _efContext;
        private SqliteConnection? _nOrmConnection;
        private SqliteConnection? _dapperConnection;
        private const int DataSize = 1000;
        private const int UsersCount = 100;
        private const int OrdersPerUser = 10;
        
        [GlobalSetup]
        public async Task Setup()
        {
            // Setup nORM
            _nOrmConnection = new SqliteConnection("Data Source=:memory:");
            await _nOrmConnection.OpenAsync();
            await CreateSchema(_nOrmConnection);
            var provider = new SqliteProvider();
            _nOrmContext = new DbContext(_nOrmConnection, provider);
            await SeedData(_nOrmContext);

            // Setup EF Core
            _efContext = new TestDbContext();
            await _efContext.Database.OpenConnectionAsync();
            await _efContext.Database.EnsureCreatedAsync();
            await SeedDataEF(_efContext);

            // Setup Dapper
            _dapperConnection = new SqliteConnection("Data Source=:memory:");
            await _dapperConnection.OpenAsync();
            await CreateSchema(_dapperConnection);
            await SeedDataDapper(_dapperConnection);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _nOrmContext?.Dispose();
            _nOrmConnection?.Dispose();
            _efContext?.Dispose();
            _dapperConnection?.Dispose();
        }
        
        #region Aggregate Benchmarks
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("Aggregates")]
        public async Task<decimal> Sum_nORM()
        {
            return await NormAdvanced.SumAsync(_nOrmContext!.Query<BenchmarkUser>(), u => u.Salary);
        }
        
        [Benchmark]
        [BenchmarkCategory("Aggregates")]
        public async Task<decimal> Sum_EfCore()
        {
            return await EfExtensions.SumAsync(_efContext!.Users, u => u.Salary);
        }
        
        [Benchmark]
        [BenchmarkCategory("Aggregates")]
        public async Task<decimal> Sum_Dapper()
        {
            return await _dapperConnection!.QuerySingleAsync<decimal>(
                "SELECT SUM(Salary) FROM Users");
        }
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("GroupByAggregate")]
        public async Task<List<object>> GroupByWithAggregates_nORM()
        {
            var query = _nOrmContext!.Query<BenchmarkUser>()
                .GroupBy(u => u.Department)
                .Select(g => new {
                    Department = g.Key,
                    Count = g.Count(),
                    AvgSalary = g.Average(u => u.Salary),
                    TotalSalary = g.Sum(u => u.Salary)
                });
            var result = await NormAsync.ToListAsync(query);
            return result.Cast<object>().ToList();
        }
        
        [Benchmark]
        [BenchmarkCategory("GroupByAggregate")]
        public async Task<List<object>> GroupByWithAggregates_EfCore()
        {
            var result = await EfExtensions.ToListAsync(_efContext!.Users
                .GroupBy(u => u.Department)
                .Select(g => new {
                    Department = g.Key,
                    Count = g.Count(),
                    AvgSalary = g.Average(u => u.Salary),
                    TotalSalary = g.Sum(u => u.Salary)
                }));
            return result.Cast<object>().ToList();
        }
        
        [Benchmark]
        [BenchmarkCategory("GroupByAggregate")]
        public async Task<List<object>> GroupByWithAggregates_Dapper()
        {
            return (await _dapperConnection!.QueryAsync<object>(@"
                SELECT Department, 
                       COUNT(*) as Count,
                       AVG(Salary) as AvgSalary,
                       SUM(Salary) as TotalSalary
                FROM Users 
                GROUP BY Department")).ToList();
        }
        
        #endregion
        
        #region Navigation Property Benchmarks
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("Navigation")]
        public async Task<List<BenchmarkUser>> EagerLoading_nORM()
        {
            var query = _nOrmContext!.Query<BenchmarkUser>()
                .Include(u => u.Orders)
                .Take(10);
            return await NormAsync.ToListAsync(query);
        }
        
        [Benchmark]
        [BenchmarkCategory("Navigation")]
        public async Task<List<BenchmarkUser>> EagerLoading_EfCore()
        {
            return await EfExtensions.ToListAsync(
                EfExtensions.Include(_efContext!.Users, u => u.Orders)
                    .Take(10));
        }
        
        [Benchmark]
        [BenchmarkCategory("Navigation")]
        public async Task<List<BenchmarkUser>> EagerLoading_Dapper()
        {
            var sql = @"
                SELECT u.*, o.*
                FROM Users u
                LEFT JOIN Orders o ON u.Id = o.UserId
                WHERE u.Id IN (SELECT Id FROM Users LIMIT 10)
                ORDER BY u.Id";
                
            var userDict = new Dictionary<int, BenchmarkUser>();
            
            await _dapperConnection!.QueryAsync<BenchmarkUser, BenchmarkOrder?, BenchmarkUser>(
                sql,
                (user, order) => {
                    if (!userDict.TryGetValue(user.Id, out var existingUser))
                    {
                        existingUser = user;
                        existingUser.Orders = new List<BenchmarkOrder>();
                        userDict[user.Id] = existingUser;
                    }
                    
                    if (order != null)
                    {
                        existingUser.Orders.Add(order);
                    }
                    
                    return existingUser;
                },
                splitOn: "Id");
                
            return userDict.Values.ToList();
        }
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("LazyLoading")]
        public async Task<int> LazyLoading_nORM()
        {
            var user = await _nOrmContext!.Query<BenchmarkUser>()
                .FirstAsync(u => u.Id == 1);
                
            // This should trigger lazy loading
            return user.Orders?.Count ?? 0;
        }
        
        [Benchmark]
        [BenchmarkCategory("LazyLoading")]
        public async Task<int> ExplicitLoading_EfCore()
        {
            var user = await EfExtensions.FirstAsync(_efContext!.Users, u => u.Id == 1);
                
            await _efContext.Entry(user)
                .Collection(u => u.Orders)
                .LoadAsync();
                
            return user.Orders?.Count ?? 0;
        }
        
        #endregion
        
        #region Complex Query Benchmarks
        
        [Benchmark(Baseline = true)]
        [BenchmarkCategory("ComplexQuery")]
        public async Task<List<object>> ComplexAggregateQuery_nORM()
        {
            var query = _nOrmContext!.Query<BenchmarkUser>()
                .Where(u => u.Salary > 50000)
                .GroupBy(u => new { u.Department, u.IsActive })
                .Select(g => new {
                    g.Key.Department,
                    g.Key.IsActive,
                    Count = g.Count(),
                    AvgSalary = g.Average(u => u.Salary),
                    MinSalary = g.Min(u => u.Salary),
                    MaxSalary = g.Max(u => u.Salary)
                });
            var result = await NormAsync.ToListAsync(query);
            return result.Cast<object>().ToList();
        }
        
        [Benchmark]
        [BenchmarkCategory("ComplexQuery")]
        public async Task<List<object>> ComplexAggregateQuery_EfCore()
        {
            var result = await EfExtensions.ToListAsync(_efContext!.Users
                .Where(u => u.Salary > 50000)
                .GroupBy(u => new { u.Department, u.IsActive })
                .Select(g => new {
                    g.Key.Department,
                    g.Key.IsActive,
                    Count = g.Count(),
                    AvgSalary = g.Average(u => u.Salary),
                    MinSalary = g.Min(u => u.Salary),
                    MaxSalary = g.Max(u => u.Salary)
                }));
            return result.Cast<object>().ToList();
        }
        
        [Benchmark]
        [BenchmarkCategory("ComplexQuery")]
        public async Task<List<object>> ComplexAggregateQuery_Dapper()
        {
            return (await _dapperConnection!.QueryAsync<object>(@"
                SELECT Department, IsActive,
                       COUNT(*) as Count,
                       AVG(Salary) as AvgSalary,
                       MIN(Salary) as MinSalary,
                       MAX(Salary) as MaxSalary
                FROM Users 
                WHERE Salary > 50000
                GROUP BY Department, IsActive")).ToList();
        }
        
        #endregion
        
        #region Helper Methods
        
        private static async Task CreateSchema(SqliteConnection connection)
        {
            var sql = @"
                CREATE TABLE Users (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Email TEXT NOT NULL,
                    Department TEXT NOT NULL,
                    Salary DECIMAL(10,2) NOT NULL,
                    IsActive BOOLEAN NOT NULL,
                    CreatedAt DATETIME NOT NULL,
                    Age INTEGER NOT NULL,
                    City TEXT NOT NULL
                );
                
                CREATE TABLE Orders (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UserId INTEGER NOT NULL,
                    Amount DECIMAL(10,2) NOT NULL,
                    OrderDate DATETIME NOT NULL,
                    ProductName TEXT NOT NULL,
                    FOREIGN KEY (UserId) REFERENCES Users(Id)
                );";
            
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
        
        private async Task SeedData(DbContext context)
        {
            var departments = new[] { "Engineering", "Sales", "Marketing", "HR" };
            var products = new[] { "Laptop", "Mouse", "Keyboard", "Monitor", "Software" };
            var random = new Random(42); // Fixed seed for consistent benchmarks
            
            // Create users
            for (int i = 1; i <= UsersCount; i++)
            {
                var user = new BenchmarkUser
                {
                    Name = $"User {i}",
                    Email = $"user{i}@company.com",
                    Department = departments[random.Next(departments.Length)],
                    Salary = 40000 + random.Next(80000),
                    IsActive = random.NextDouble() > 0.1, // 90% active
                    CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                    Age = 25 + random.Next(40),
                    City = "City" + random.Next(1, 6)
                };
                await context.InsertAsync(user);
                
                // Create orders for each user
                for (int j = 1; j <= OrdersPerUser; j++)
                {
                    var order = new BenchmarkOrder
                    {
                        UserId = i,
                        Amount = 50 + random.Next(2000),
                        OrderDate = DateTime.Now.AddDays(-random.Next(180)),
                        ProductName = products[random.Next(products.Length)]
                    };
                    await context.InsertAsync(order);
                }
            }
        }
        
        private async Task SeedDataEF(TestDbContext context)
        {
            var departments = new[] { "Engineering", "Sales", "Marketing", "HR" };
            var products = new[] { "Laptop", "Mouse", "Keyboard", "Monitor", "Software" };
            var random = new Random(42); // Fixed seed for consistent benchmarks
            
            // Create users
            for (int i = 1; i <= UsersCount; i++)
            {
                var user = new BenchmarkUser
                {
                    Name = $"User {i}",
                    Email = $"user{i}@company.com",
                    Department = departments[random.Next(departments.Length)],
                    Salary = 40000 + random.Next(80000),
                    IsActive = random.NextDouble() > 0.1, // 90% active
                    CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                    Age = 25 + random.Next(40),
                    City = "City" + random.Next(1, 6),
                    Orders = new List<BenchmarkOrder>()
                };
                
                // Create orders for each user
                for (int j = 1; j <= OrdersPerUser; j++)
                {
                    var order = new BenchmarkOrder
                    {
                        Amount = 50 + random.Next(2000),
                        OrderDate = DateTime.Now.AddDays(-random.Next(180)),
                        ProductName = products[random.Next(products.Length)]
                    };
                    user.Orders.Add(order);
                }
                
                context.Users.Add(user);
            }
            
            await context.SaveChangesAsync();
        }
        
        private async Task SeedDataDapper(SqliteConnection connection)
        {
            var departments = new[] { "Engineering", "Sales", "Marketing", "HR" };
            var products = new[] { "Laptop", "Mouse", "Keyboard", "Monitor", "Software" };
            var random = new Random(42); // Fixed seed for consistent benchmarks
            
            using var transaction = connection.BeginTransaction();
            
            // Create users
            for (int i = 1; i <= UsersCount; i++)
            {
                await connection.ExecuteAsync(@"
                    INSERT INTO Users (Name, Email, Department, Salary, IsActive, CreatedAt, Age, City)
                    VALUES (@Name, @Email, @Department, @Salary, @IsActive, @CreatedAt, @Age, @City)",
                    new {
                        Name = $"User {i}",
                        Email = $"user{i}@company.com",
                        Department = departments[random.Next(departments.Length)],
                        Salary = 40000 + random.Next(80000),
                        IsActive = random.NextDouble() > 0.1,
                        CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                        Age = 25 + random.Next(40),
                        City = "City" + random.Next(1, 6)
                    }, transaction);
                
                // Create orders for each user
                for (int j = 1; j <= OrdersPerUser; j++)
                {
                    await connection.ExecuteAsync(@"
                        INSERT INTO Orders (UserId, Amount, OrderDate, ProductName)
                        VALUES (@UserId, @Amount, @OrderDate, @ProductName)",
                        new {
                            UserId = i,
                            Amount = 50 + random.Next(2000),
                            OrderDate = DateTime.Now.AddDays(-random.Next(180)),
                            ProductName = products[random.Next(products.Length)]
                        }, transaction);
                }
            }
            
            transaction.Commit();
        }
        
        #endregion
    }
    
    #region Entity Models
    
    public class BenchmarkUser
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string Department { get; set; } = string.Empty;
        public decimal Salary { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = string.Empty;
        
        // Navigation property
        public virtual ICollection<BenchmarkOrder> Orders { get; set; } = new List<BenchmarkOrder>();
    }
    
    public class BenchmarkOrder
    {
        [Key]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public DateTime OrderDate { get; set; }
        public string ProductName { get; set; } = string.Empty;
        
        // Navigation property
        [ForeignKey(nameof(UserId))]
        public virtual BenchmarkUser? User { get; set; }
    }
    
    #endregion
    
    #region EF Core Context
    
    public class TestDbContext : Microsoft.EntityFrameworkCore.DbContext
    {
        public Microsoft.EntityFrameworkCore.DbSet<BenchmarkUser> Users { get; set; }
        public Microsoft.EntityFrameworkCore.DbSet<BenchmarkOrder> Orders { get; set; }
        
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite("Data Source=:memory:");
        }
        
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BenchmarkUser>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Name).IsRequired();
                entity.Property(e => e.Email).IsRequired();
                entity.Property(e => e.Department).IsRequired();
                entity.Property(e => e.Salary).HasColumnType("DECIMAL(10,2)");
                
                entity.HasMany(e => e.Orders)
                    .WithOne(e => e.User)
                    .HasForeignKey(e => e.UserId);
            });
            
            modelBuilder.Entity<BenchmarkOrder>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Amount).HasColumnType("DECIMAL(10,2)");
                entity.Property(e => e.ProductName).IsRequired();
            });
        }
    }
    
    #endregion
}
