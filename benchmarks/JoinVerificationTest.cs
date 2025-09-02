using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Dapper;
using nORM.Core;
using nORM.Providers;

namespace nORM.Benchmarks
{
    public static class JoinVerificationTest
    {
        public static async Task RunJoinTests()
        {
            Console.WriteLine("🧪 Running JOIN verification tests...");
            
            var connectionString = "Data Source=join_test.db";
            
            // Clean up existing database
            if (System.IO.File.Exists("join_test.db"))
                System.IO.File.Delete("join_test.db");
                
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync();
            
            var options = new nORM.Configuration.DbContextOptions
            {
                BulkBatchSize = 50,
                CommandTimeout = TimeSpan.FromSeconds(30)
            };
            
            using var context = new nORM.Core.DbContext(connection, new SqliteProvider(), options);
            
            try
            {
                // Create test tables
                await connection.ExecuteAsync(@"
                    CREATE TABLE BenchmarkUser (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        Email TEXT NOT NULL,
                        CreatedAt TEXT NOT NULL,
                        IsActive INTEGER NOT NULL,
                        Age INTEGER NOT NULL,
                        City TEXT NOT NULL
                    )");
                    
                await connection.ExecuteAsync(@"
                    CREATE TABLE BenchmarkOrder (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UserId INTEGER NOT NULL,
                        Amount REAL NOT NULL,
                        OrderDate TEXT NOT NULL,
                        ProductName TEXT NOT NULL
                    )");
                
                Console.WriteLine("  ✅ Test tables created");
                
                // Insert test data
                var testUsers = new[]
                {
                    new BenchmarkUser { Name = "Alice", Email = "alice@test.com", CreatedAt = DateTime.Now, IsActive = true, Age = 30, City = "NYC" },
                    new BenchmarkUser { Name = "Bob", Email = "bob@test.com", CreatedAt = DateTime.Now, IsActive = true, Age = 25, City = "LA" },
                    new BenchmarkUser { Name = "Charlie", Email = "charlie@test.com", CreatedAt = DateTime.Now, IsActive = false, Age = 35, City = "NYC" }
                };
                
                foreach (var user in testUsers)
                {
                    await context.InsertAsync(user);
                }
                
                var testOrders = new[]
                {
                    new BenchmarkOrder { UserId = 1, Amount = 150m, OrderDate = DateTime.Now, ProductName = "Laptop" },
                    new BenchmarkOrder { UserId = 1, Amount = 50m, OrderDate = DateTime.Now, ProductName = "Mouse" },
                    new BenchmarkOrder { UserId = 2, Amount = 200m, OrderDate = DateTime.Now, ProductName = "Phone" },
                    new BenchmarkOrder { UserId = 3, Amount = 75m, OrderDate = DateTime.Now, ProductName = "Book" }
                };
                
                foreach (var order in testOrders)
                {
                    await context.InsertAsync(order);
                }
                
                Console.WriteLine("  ✅ Test data inserted");
                
                // Test 1: Basic JOIN
                Console.WriteLine("\n  🧪 Test 1: Basic JOIN operation");
                try
                {
                    var joinQuery = context.Query<BenchmarkUser>()
                        .Join(
                            context.Query<BenchmarkOrder>(),
                            u => u.Id,
                            o => o.UserId,
                            (u, o) => new { u.Name, o.Amount, o.ProductName }
                        );
                        
                    var joinResult = await NormAsyncExtensions.ToListAsync(joinQuery);
                    
                    Console.WriteLine($"    ✅ JOIN returned {joinResult.Count} results");
                    foreach (var item in joinResult)
                    {
                        Console.WriteLine($"    📄 {item.Name}: {item.ProductName} (${item.Amount})");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"    ❌ JOIN failed: {ex.Message}");
                }
                
                // Test 2: JOIN with WHERE clause
                Console.WriteLine("\n  🧪 Test 2: JOIN with WHERE clause");
                try
                {
                    var filteredJoinQuery = context.Query<BenchmarkUser>()
                        .Join(
                            context.Query<BenchmarkOrder>(),
                            u => u.Id,
                            o => o.UserId,
                            (u, o) => new { u.Name, o.Amount, o.ProductName }
                        )
                        .Where(x => x.Amount > 100);
                        
                    var filteredJoin = await NormAsyncExtensions.ToListAsync(filteredJoinQuery);
                    
                    Console.WriteLine($"    ✅ Filtered JOIN returned {filteredJoin.Count} results");
                    foreach (var item in filteredJoin)
                    {
                        Console.WriteLine($"    📄 {item.Name}: {item.ProductName} (${item.Amount})");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"    ❌ Filtered JOIN failed: {ex.Message}");
                }
                
                // Test 3: Bulk operation test
                Console.WriteLine("\n  🧪 Test 3: Bulk insert operation");
                try
                {
                    var bulkUsers = new BenchmarkUser[50];
                    for (int i = 0; i < 50; i++)
                    {
                        bulkUsers[i] = new BenchmarkUser
                        {
                            Name = $"BulkUser{i}",
                            Email = $"bulk{i}@test.com",
                            CreatedAt = DateTime.Now,
                            IsActive = true,
                            Age = 25 + (i % 20),
                            City = "TestCity"
                        };
                    }
                    
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    await context.BulkInsertAsync(bulkUsers);
                    sw.Stop();
                    
                    Console.WriteLine($"    ✅ Bulk insert of 50 users completed in {sw.ElapsedMilliseconds}ms");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"    ❌ Bulk insert failed: {ex.Message}");
                }
                
                Console.WriteLine("\n✅ JOIN verification tests completed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Test setup failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            finally
            {
                // Clean up
                try
                {
                    if (System.IO.File.Exists("join_test.db"))
                        System.IO.File.Delete("join_test.db");
                }
                catch { }
            }
        }
    }
}
