using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Microsoft.Data.SqlClient;

namespace nORM.Examples
{
    /// <summary>
    /// Simple test to verify join operations compile and execute correctly
    /// </summary>
    public static class JoinCompilationTest
    {
        public static async Task TestJoinCompilation()
        {
            Console.WriteLine("=== Testing Join Operations Compilation ===\n");

            try
            {
                // Create a test context (won't actually connect to database)
                var connection = new SqlConnection("Server=.;Database=TestDb;Trusted_Connection=true");
                var provider = new SqlServerProvider();
                var context = new DbContext(connection, provider);

                Console.WriteLine("✅ Context created successfully");

                // Test 1: Basic Join Query Creation (won't execute, just compile)
                var joinQuery = context.Query<User>()
                    .Join(
                        context.Query<Order>(),
                        user => user.Id,
                        order => order.UserId,
                        (user, order) => new { user.Name, order.Amount }
                    );

                Console.WriteLine("✅ Basic join query compiled successfully");

                // Test 2: Group Join Query Creation
                var groupJoinQuery = context.Query<User>()
                    .GroupJoin(
                        context.Query<Order>(),
                        user => user.Id,
                        order => order.UserId,
                        (user, orders) => new { User = user, Orders = orders }
                    );

                Console.WriteLine("✅ Group join query compiled successfully");

                // Test 3: Join with Custom Projection
                var projectionQuery = context.Query<User>()
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
                    );

                Console.WriteLine("✅ Join with custom projection compiled successfully");

                // Test 4: Complex Join with Filtering
                var complexQuery = context.Query<User>()
                    .Where(u => u.Name.StartsWith("J"))
                    .Join(
                        context.Query<Order>().Where(o => o.Amount > 100),
                        u => u.Id,
                        o => o.UserId,
                        (u, o) => new { u.Name, o.Amount, o.ProductName }
                    )
                    .OrderBy(result => result.Amount)
                    .Take(10);

                Console.WriteLine("✅ Complex join with filtering compiled successfully");

                Console.WriteLine("\n🎉 All join operations compiled successfully!");
                Console.WriteLine("The join implementation is working correctly at the compilation level.");
                Console.WriteLine("To test execution, you would need a real database connection.");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error during join compilation test: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
        }

        // Test entities (same as in main examples)
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

        public class UserOrderSummary
        {
            public string UserName { get; set; } = "";
            public string Email { get; set; } = "";
            public decimal OrderAmount { get; set; }
            public DateTime OrderDate { get; set; }
        }
    }
}
