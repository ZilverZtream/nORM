// nORM under NativeAOT — a realistic read/write demo that publishes to a self-contained native
// binary with zero trimmer ceremony (see the .csproj for the four-line consumer contract).
//
// The pattern that makes it AOT-safe:
//   * Entities are marked [GenerateMaterializer]. The source generator emits their materializer AND a
//     [DynamicDependency] that preserves exactly the property/attribute metadata nORM reflects over,
//     so trimming never strips it — no <TrimmerRootAssembly> required.
//   * Reads use the ordinary runtime LINQ API (ctx.Query<T>()...). [CompileTimeQuery] methods work too
//     and skip expression translation entirely; either is fully native.

using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;

[Table("Product")]
[GenerateMaterializer]
public sealed class Product
{
    [Key] public int Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public bool InStock { get; set; }
}

public static class Program
{
    public static async Task<int> Main()
    {
        // A throwaway in-memory database so the sample is self-contained.
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Product (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price TEXT NOT NULL, InStock INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        await using var db = new DbContext(connection, new SqliteProvider());

        // Write — tracked insert via the change tracker.
        db.Add(new Product { Id = 1, Name = "Keyboard", Price = 79.99m, InStock = true });
        db.Add(new Product { Id = 2, Name = "Mouse", Price = 24.50m, InStock = true });
        db.Add(new Product { Id = 3, Name = "4K Monitor", Price = 429.00m, InStock = false });
        await db.SaveChangesAsync();

        // Read — ordinary runtime LINQ, translated to SQL and materialized by the generated delegate.
        var inStock = await db.Query<Product>()
            .Where(p => p.InStock && p.Price < 100m)
            .OrderBy(p => p.Price)
            .ToListAsync();

        Console.WriteLine("In-stock products under $100:");
        foreach (var p in inStock)
            Console.WriteLine($"  #{p.Id} {p.Name,-12} {p.Price,8:C}");

        // Write — tracked update: the query tracks the entity, so mutate it in place and save.
        var monitor = (await db.Query<Product>().Where(p => p.Id == 3).ToListAsync()).Single();
        monitor.Price = 399.00m;
        monitor.InStock = true;
        await db.SaveChangesAsync();

        var restocked = (await db.Query<Product>().Where(p => p.Id == 3).ToListAsync()).Single();
        Console.WriteLine($"\nRestocked: {restocked.Name} is now {restocked.Price:C}, in stock = {restocked.InStock}");

        Console.WriteLine("\nRan natively under NativeAOT — no trimmer rooting required.");
        return 0;
    }
}
