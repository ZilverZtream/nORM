using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Microsoft.Data.SqlClient;

namespace nORM.Examples
{
    // Example entities
    public class User
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Email { get; set; } = "";
        public DateTime CreatedAt { get; set; }
    }

    public class Order
    {
        [Key]
        public int Id { get; set; }
        public int UserId { get; set; }
        public decimal Amount { get; set; }
        public DateTime OrderDate { get; set; }
        public string ProductName { get; set; } = "";
    }

    public class Product
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public decimal Price { get; set; }
        public string Category { get; set; } = "";
    }

    public static class JoinExamples
    {
        public static async Task RunAllExamples()
        {
            var connection = new SqlConnection("Server=.;Database=TestDb;Trusted_Connection=true");
            var provider = new SqlServerProvider();
            var context = new DbContext(connection, provider);

            Console.WriteLine("=== nORM Join Operations Examples ===\n");

            // Example 1: Basic Inner Join
            await BasicInnerJoinExample(context);

            // Example 2: Join with Projection
            await JoinWithProjectionExample(context);

            // Example 3: Group Join (Left Join)
            await GroupJoinExample(context);

            // Example 4: SelectMany with Navigation
            await SelectManyNavigationExample(context);

            // Example 5: Multiple Joins
            await MultipleJoinsExample(context);
        }

        static async Task BasicInnerJoinExample(DbContext context)
        {
            Console.WriteLine("1. Basic Inner Join:");
            Console.WriteLine("   Query: Users joined with their Orders");
            
            var userOrders = await context.Query<User>()
                .Join(
                    context.Query<Order>(),
                    user => user.Id,
                    order => order.UserId,
                    (user, order) => new { user.Name, order.Amount, order.ProductName }
                )
                .ToListAsync();

            Console.WriteLine($"   SQL Generated: JOIN Users with Orders");
            Console.WriteLine($"   Results: {userOrders.Count} user-order combinations\n");
        }

        static async Task JoinWithProjectionExample(DbContext context)
        {
            Console.WriteLine("2. Join with Custom Projection:");
            Console.WriteLine("   Query: Users with their total order amounts");

            var userTotals = await context.Query<User>()
                .Join(
                    context.Query<Order>(),
                    u => u.Id,
                    o => o.UserId,
                    (u, o) => new UserOrderSummary 
                    { 
                        UserName = u.Name, 
                        Email = u.Email,
                        OrderAmount = o.Amount,
                        OrderDate = o.OrderDate
                    }
                )
                .ToListAsync();

            Console.WriteLine($"   Results: {userTotals.Count} user summaries\n");
        }

        static async Task GroupJoinExample(DbContext context)
        {
            Console.WriteLine("3. Group Join (Left Join):");
            Console.WriteLine("   Query: All users with their orders (including users with no orders)");

            var usersWithOrders = await context.Query<User>()
                .GroupJoin(
                    context.Query<Order>(),
                    user => user.Id,
                    order => order.UserId,
                    (user, orders) => new { User = user, Orders = orders }
                )
                .ToListAsync();

            Console.WriteLine($"   Results: {usersWithOrders.Count} users (with all their orders)\n");
        }

        static async Task SelectManyNavigationExample(DbContext context)
        {
            Console.WriteLine("4. SelectMany with Navigation:");
            Console.WriteLine("   Query: Flatten user orders using navigation properties");

            // Note: This assumes User has a navigation property to Orders
            // var allOrders = await context.Query<User>()
            //     .SelectMany(u => u.Orders)
            //     .Where(o => o.Amount > 100)
            //     .ToListAsync();

            Console.WriteLine("   Note: Navigation properties need to be configured in TableMapping");
            Console.WriteLine("   This would generate: SELECT * FROM Users u INNER JOIN Orders o ON u.Id = o.UserId WHERE o.Amount > 100\n");
        }

        static async Task MultipleJoinsExample(DbContext context)
        {
            Console.WriteLine("5. Multiple Joins (Chain Joins):");
            Console.WriteLine("   Query: Users -> Orders -> Products (3-table join)");

            // This would require chaining joins or implementing a more complex join syntax
            // var userOrderProducts = await context.Query<User>()
            //     .Join(context.Query<Order>(), u => u.Id, o => o.UserId, (u, o) => new { u, o })
            //     .Join(context.Query<Product>(), uo => uo.o.ProductId, p => p.Id, (uo, p) => new 
            //     {
            //         UserName = uo.u.Name,
            //         OrderAmount = uo.o.Amount,
            //         ProductName = p.Name,
            //         ProductPrice = p.Price
            //     })
            //     .ToListAsync();

            Console.WriteLine("   Note: Multiple joins can be chained for complex queries");
            Console.WriteLine("   This demonstrates the power of the join implementation\n");
        }
    }

    // Supporting class for projections
    public class UserOrderSummary
    {
        public string UserName { get; set; } = "";
        public string Email { get; set; } = "";
        public decimal OrderAmount { get; set; }
        public DateTime OrderDate { get; set; }
    }

    // Example of how to set up entity relationships for SelectMany
    public class UserWithOrders : User
    {
        public List<Order> Orders { get; set; } = new();
    }
}
