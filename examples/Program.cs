using System;
using System.Threading.Tasks;
using nORM.Examples;

namespace nORM.TestRunner
{
    /// <summary>
    /// Simple console application to test nORM join operations
    /// </summary>
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("nORM Join Operations Test Runner");
            Console.WriteLine("================================");
            Console.WriteLine();

            // Test compilation of join operations
            await JoinCompilationTest.TestJoinCompilation();

            Console.WriteLine();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
