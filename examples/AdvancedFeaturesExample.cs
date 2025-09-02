using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Navigation;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

#nullable enable

namespace nORM.Examples
{
    /// <summary>
    /// Comprehensive example demonstrating advanced LINQ aggregates and navigation properties
    /// Shows real-world usage patterns that achieve EF Core feature parity with Dapper performance
    /// </summary>
    public class AdvancedFeaturesExample
    {
        public static async Task RunExampleAsync()
        {
            // Setup database connection
            var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            
            // Create schema
            await CreateSchemaAsync(connection);
            
            var provider = new SqliteProvider();
            var context = new DbContext(connection, provider);
            
            // Seed sample data
            await SeedDataAsync(context);
            
            Console.WriteLine("=== Advanced LINQ Aggregates Demo ===");
            await DemonstrateAdvancedAggregatesAsync(context);
            
            Console.WriteLine("\n=== Navigation Properties Demo ===");
            await DemonstrateNavigationPropertiesAsync(context);
            
            Console.WriteLine("\n=== Complex Query with GroupBy and Aggregates ===");
            await DemonstrateComplexGroupByAsync(context);
            
            Console.WriteLine("\n=== Lazy Loading Demo ===");
            await DemonstrateLazyLoadingAsync(context);
        }
        
        private static async Task CreateSchemaAsync(SqliteConnection connection)
        {
            var createTables = @"
                CREATE TABLE Users (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Email TEXT NOT NULL,
                    Department TEXT,
                    Salary DECIMAL(10,2),
                    HireDate DATETIME NOT NULL
                );
                
                CREATE TABLE Orders (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    UserId INTEGER NOT NULL,
                    Amount DECIMAL(10,2) NOT NULL,
                    OrderDate DATETIME NOT NULL,
                    ProductName TEXT NOT NULL,
                    FOREIGN KEY (UserId) REFERENCES Users(Id)
                );
                
                CREATE TABLE OrderItems (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderId INTEGER NOT NULL,
                    ProductName TEXT NOT NULL,
                    Quantity INTEGER NOT NULL,
                    UnitPrice DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (OrderId) REFERENCES Orders(Id)
                );
            ";
            
            using var cmd = connection.CreateCommand();
            cmd.CommandText = createTables;
            await cmd.ExecuteNonQueryAsync();
        }
        
        private static async Task SeedDataAsync(DbContext context)
        {
            // Create sample users
            var users = new List<User>
            {
                new() { Name = "Alice Johnson", Email = "alice@company.com", Department = "Engineering", Salary = 95000, HireDate = DateTime.Now.AddYears(-2) },
                new() { Name = "Bob Smith", Email = "bob@company.com", Department = "Sales", Salary = 75000, HireDate = DateTime.Now.AddYears(-1) },
                new() { Name = "Carol Davis", Email = "carol@company.com", Department = "Engineering", Salary = 105000, HireDate = DateTime.Now.AddMonths(-8) },
                new() { Name = "David Wilson", Email = "david@company.com", Department = "Marketing", Salary = 65000, HireDate = DateTime.Now.AddMonths(-3) },
                new() { Name = "Eve Brown", Email = "eve@company.com", Department = "Engineering", Salary = 88000, HireDate = DateTime.Now.AddMonths(-15) }
            };
            
            foreach (var user in users)
            {
                await context.InsertAsync(user);
            }
            
            // Create sample orders
            var orders = new List<Order>
            {
                new() { UserId = 1, Amount = 1250.50m, OrderDate = DateTime.Now.AddDays(-10), ProductName = "Laptop" },
                new() { UserId = 1, Amount = 85.25m, OrderDate = DateTime.Now.AddDays(-5), ProductName = "Mouse" },
                new() { UserId = 2, Amount = 2100.00m, OrderDate = DateTime.Now.AddDays(-7), ProductName = "Workstation" },
                new() { UserId = 3, Amount = 450.75m, OrderDate = DateTime.Now.AddDays(-3), ProductName = "Monitor" },
                new() { UserId = 2, Amount = 320.00m, OrderDate = DateTime.Now.AddDays(-1), ProductName = "Keyboard" },
                new() { UserId = 4, Amount = 1800.00m, OrderDate = DateTime.Now.AddDays(-12), ProductName = "Design Software" },
                new() { UserId = 5, Amount = 299.99m, OrderDate = DateTime.Now.AddDays(-2), ProductName = "Development Tools" }
            };
            
            foreach (var order in orders)
            {
                await context.InsertAsync(order);
            }
            
            // Create sample order items
            var orderItems = new List<OrderItem>
            {
                new() { OrderId = 1, ProductName = "Laptop", Quantity = 1, UnitPrice = 1250.50m },
                new() { OrderId = 2, ProductName = "Mouse", Quantity = 1, UnitPrice = 85.25m },
                new() { OrderId = 3, ProductName = "Workstation", Quantity = 1, UnitPrice = 2100.00m },
                new() { OrderId = 4, ProductName = "Monitor", Quantity = 1, UnitPrice = 450.75m },
                new() { OrderId = 5, ProductName = "Keyboard", Quantity = 2, UnitPrice = 160.00m },
                new() { OrderId = 6, ProductName = "Design Software", Quantity = 1, UnitPrice = 1800.00m },
                new() { OrderId = 7, ProductName = "Development Tools", Quantity = 3, UnitPrice = 99.99m }
            };
            
            foreach (var item in orderItems)
            {
                await context.InsertAsync(item);
            }
        }
        
        private static async Task DemonstrateAdvancedAggregatesAsync(DbContext context)
        {
            Console.WriteLine("1. Sum of all salaries:");
            var totalSalaries = await context.Query<User>()
                .SumAsync(u => u.Salary);
            Console.WriteLine($"   Total: ${totalSalaries:N2}");
            
            Console.WriteLine("2. Average salary by department:");
            var avgSalaryByDept = await context.Query<User>()
                .GroupBy(u => u.Department)
                .Select(g => new { Department = g.Key, AvgSalary = g.Average(u => u.Salary) })
                .ToListAsync();
            
            foreach (var dept in avgSalaryByDept)
            {
                Console.WriteLine($"   {dept.Department}: ${dept.AvgSalary:N2}");
            }
            
            Console.WriteLine("3. Min and Max order amounts:");
            var minOrder = await context.Query<Order>()
                .MinAsync(o => o.Amount);
            var maxOrder = await context.Query<Order>()
                .MaxAsync(o => o.Amount);
            Console.WriteLine($"   Min: ${minOrder:N2}, Max: ${maxOrder:N2}");
            
            Console.WriteLine("4. Count of orders per user:");
            var orderCounts = await context.Query<Order>()
                .GroupBy(o => o.UserId)
                .Select(g => new { UserId = g.Key, OrderCount = g.Count() })
                .ToListAsync();
            
            foreach (var count in orderCounts)
            {
                Console.WriteLine($"   User {count.UserId}: {count.OrderCount} orders");
            }
            
            Console.WriteLine("5. Total revenue by product (with conditional aggregation):");
            var productRevenue = await context.Query<Order>()
                .Where(o => o.Amount > 100) // Only orders over $100
                .GroupBy(o => o.ProductName)
                .Select(g => new { 
                    Product = g.Key, 
                    Revenue = g.Sum(o => o.Amount),
                    OrderCount = g.Count()
                })
                .ToListAsync();
            
            foreach (var product in productRevenue)
            {
                Console.WriteLine($"   {product.Product}: ${product.Revenue:N2} ({product.OrderCount} orders)");
            }
        }
        
        private static async Task DemonstrateNavigationPropertiesAsync(DbContext context)
        {
            Console.WriteLine("1. Loading users with their orders (Include):");
            var usersWithOrders = await context.Query<User>()
                .Include(u => u.Orders)
                .ToListAsync();
            
            foreach (var user in usersWithOrders)
            {
                Console.WriteLine($"   {user.Name}: {user.Orders?.Count ?? 0} orders");
                if (user.Orders != null)
                {
                    foreach (var order in user.Orders.Take(2)) // Show first 2 orders
                    {
                        Console.WriteLine($"     - {order.ProductName}: ${order.Amount:N2}");
                    }
                }
            }
            
            Console.WriteLine("2. Explicit loading of navigation properties:");
            var user = await context.Query<User>()
                .FirstAsync(u => u.Name == "Alice Johnson");
            
            Console.WriteLine($"   Before loading: {user.Name} has {user.Orders?.Count ?? 0} orders loaded");
            
            // Explicitly load the orders
            await user.LoadAsync(u => u.Orders);
            
            Console.WriteLine($"   After loading: {user.Name} has {user.Orders?.Count ?? 0} orders loaded");
            
            Console.WriteLine("3. Checking if navigation property is loaded:");
            var isLoaded = user.IsLoaded(u => u.Orders);
            Console.WriteLine($"   Orders property is loaded: {isLoaded}");
        }
        
        private static async Task DemonstrateComplexGroupByAsync(DbContext context)
        {
            Console.WriteLine("Complex aggregation: Department statistics with multiple metrics:");
            
            var departmentStats = await context.Query<User>()
                .GroupBy(u => u.Department)
                .Select(g => new DepartmentStats
                {
                    Department = g.Key!,
                    EmployeeCount = g.Count(),
                    TotalSalary = g.Sum(u => u.Salary),
                    AverageSalary = g.Average(u => u.Salary),
                    MinSalary = g.Min(u => u.Salary),
                    MaxSalary = g.Max(u => u.Salary)
                })
                .ToListAsync();
            
            foreach (var stat in departmentStats)
            {
                Console.WriteLine($"   {stat.Department}:");
                Console.WriteLine($"     Employees: {stat.EmployeeCount}");
                Console.WriteLine($"     Total Salary: ${stat.TotalSalary:N2}");
                Console.WriteLine($"     Avg Salary: ${stat.AverageSalary:N2}");
                Console.WriteLine($"     Salary Range: ${stat.MinSalary:N2} - ${stat.MaxSalary:N2}");
            }
            
            Console.WriteLine("\\nAdvanced: Users with above-average salary in their department:");
            var aboveAverageUsers = await context.Query<User>()
                .Where(u => u.Salary > context.Query<User>()
                    .Where(u2 => u2.Department == u.Department)
                    .Average(u2 => u2.Salary))
                .Select(u => new { u.Name, u.Department, u.Salary })
                .ToListAsync();
            
            foreach (var user in aboveAverageUsers)
            {
                Console.WriteLine($"   {user.Name} ({user.Department}): ${user.Salary:N2}");
            }
        }
        
        private static async Task DemonstrateLazyLoadingAsync(DbContext context)
        {
            Console.WriteLine("1. Lazy loading in action:");
            
            // Load a user without including orders
            var user = await context.Query<User>()
                .FirstAsync(u => u.Name == "Bob Smith");
            
            Console.WriteLine($"   Loaded user: {user.Name}");
            Console.WriteLine($"   Orders loaded initially: {user.Orders?.Count ?? 0}");
            
            // Access the Orders property - this should trigger lazy loading
            if (user.Orders != null)
            {
                Console.WriteLine($"   Accessing Orders property...");
                var orderCount = user.Orders.Count; // This triggers lazy loading
                Console.WriteLine($"   Orders after lazy loading: {orderCount}");
                
                foreach (var order in user.Orders)
                {
                    Console.WriteLine($"     - {order.ProductName}: ${order.Amount:N2}");
                }
            }
            
            Console.WriteLine("2. Performance comparison: Lazy vs Eager loading");
            
            // Measure eager loading
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var eagerUsers = await context.Query<User>()
                .Include(u => u.Orders)
                .ToListAsync();
            stopwatch.Stop();
            Console.WriteLine($"   Eager loading {eagerUsers.Count} users with orders: {stopwatch.ElapsedMilliseconds}ms");
            
            // Measure lazy loading (accessing one user's orders)
            stopwatch.Restart();
            var lazyUser = await context.Query<User>()
                .FirstAsync(u => u.Name == "Carol Davis");
            var lazyOrders = lazyUser.Orders?.Count ?? 0; // Triggers lazy loading
            stopwatch.Stop();
            Console.WriteLine($"   Lazy loading orders for 1 user: {stopwatch.ElapsedMilliseconds}ms");
        }
        
        // Window functions example
        private static async Task DemonstrateWindowFunctionsAsync(DbContext context)
        {
            Console.WriteLine("Window Functions: Users with row numbers ordered by salary:");
            
            var usersWithRowNumbers = await context.Query<User>()
                .OrderByDescending(u => u.Salary)
                .WithRowNumber((user, rowNum) => new { User = user, RowNumber = rowNum })
                .ToListAsync();
            
            foreach (var item in usersWithRowNumbers)
            {
                Console.WriteLine($"   #{item.RowNumber}: {item.User.Name} - ${item.User.Salary:N2}");
            }
        }
    }
    
    // Entity Models
    public class User
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string? Department { get; set; }
        public decimal Salary { get; set; }
        public DateTime HireDate { get; set; }
        
        // Navigation property - will be lazy loaded
        public virtual ICollection<Order>? Orders { get; set; }
    }
    
    public class Order
    {
        [Key]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public DateTime OrderDate { get; set; }
        public string ProductName { get; set; } = string.Empty;
        
        // Navigation properties
        [ForeignKey(nameof(UserId))]
        public virtual User? User { get; set; }
        
        public virtual ICollection<OrderItem>? OrderItems { get; set; }
    }
    
    public class OrderItem
    {
        [Key]
        public int Id { get; set; }
        public int OrderId { get; set; }
        public string ProductName { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
        
        // Navigation property
        [ForeignKey(nameof(OrderId))]
        public virtual Order? Order { get; set; }
    }
    
    // DTO for complex aggregations
    public class DepartmentStats
    {
        public string Department { get; set; } = string.Empty;
        public int EmployeeCount { get; set; }
        public decimal TotalSalary { get; set; }
        public decimal AverageSalary { get; set; }
        public decimal MinSalary { get; set; }
        public decimal MaxSalary { get; set; }
    }
}
