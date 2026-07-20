using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

// Not a gate test — an allocation bisect for the tracked full-row update write path. Prints bytes/op for
// each phase so the dominant SaveChanges allocator can be found. Trait excludes it from Fast gate runs.
[Trait("Category", "Diagnostic")]
public class WritePathAllocationProbe
{
    private readonly ITestOutputHelper _out;
    public WritePathAllocationProbe(ITestOutputHelper o) => _out = o;

    [Table("WpaUser")]
    public class User
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Email { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = "";
        public string Department { get; set; } = "";
        public double Salary { get; set; }
    }

    private static User Row() => new User
    {
        Id = 1, Name = "u", Email = "u@x.com", CreatedAt = DateTime.UtcNow, IsActive = true,
        Age = 41, City = "C", Department = "D", Salary = 61000
    };

    [Fact]
    public async Task Bisect_update_allocation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WpaUser (Id INTEGER PRIMARY KEY, Name TEXT, Email TEXT, CreatedAt TEXT, IsActive INTEGER, Age INTEGER, City TEXT, Department TEXT, Salary REAL);
                INSERT INTO WpaUser VALUES (1, 'u', 'u@x.com', '2024-01-01', 1, 40, 'C', 'D', 50000);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<User>().HasKey(u => u.Id)
        }, ownsConnection: false);

        // Warm up all the one-time caches (mapping, plan, prepared command).
        for (int i = 0; i < 20; i++)
        {
            ctx.Update(Row());
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        long Measure(Func<Task> op, int n)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < n; i++) op().GetAwaiter().GetResult();
            var after = GC.GetAllocatedBytesForCurrentThread();
            return (after - before) / n;
        }

        // Warm the set-based ExecuteUpdate path too.
        for (int i = 0; i < 20; i++)
            await NormAsyncExtensions.ExecuteUpdateAsync(ctx.Query<User>().Where(u => u.Id == 1), s => s.SetProperty(p => p.Salary, 61000.0));

        const int N = 200;
        var full = Measure(async () => { ctx.Update(Row()); await ctx.SaveChangesAsync(); ctx.ChangeTracker.Clear(); }, N);
        var updateOnly = Measure(() => { ctx.Update(Row()); ctx.ChangeTracker.Clear(); return Task.CompletedTask; }, N);
        var saveNoDetect = Measure(async () => { ctx.Update(Row()); await ctx.SaveChangesAsync(detectChanges: false); ctx.ChangeTracker.Clear(); }, N);
        var execUpdateSet = Measure(async () => { await NormAsyncExtensions.ExecuteUpdateAsync(ctx.Query<User>().Where(u => u.Id == 1), s => s.SetProperty(p => p.Salary, 61000.0)); }, N);

        // SaveChanges inside ONE explicit transaction: TransactionManager reuses the ambient tx (no per-save
        // BeginTransaction/linked-CTS/commit), isolating the LINQ grouping pipeline + accept from the tx cost.
        long saveInTx;
        using (var probeTx = ctx.Database.BeginTransaction())
        {
            saveInTx = Measure(async () => { ctx.Update(Row()); await ctx.SaveChangesAsync(); ctx.ChangeTracker.Clear(); }, N);
        }

        _out.WriteLine($"full  Update+SaveChanges+Clear   = {full} B/op");
        _out.WriteLine($"  Update+Clear (no save)         = {updateOnly} B/op");
        _out.WriteLine($"  Update+SaveChanges(noDetect)   = {saveNoDetect} B/op  (detect cost ~ {full - saveNoDetect})");
        _out.WriteLine($"  set-based ExecuteUpdate        = {execUpdateSet} B/op  (shares low-level execute+intercept, skips tracking/grouping/per-save-tx)");
        _out.WriteLine($"  SaveChanges inside explicit tx = {saveInTx} B/op  (=> per-save transaction cost ~ {full - saveInTx})");
        _out.WriteLine($"  => SaveChanges-orchestration share ~ {full - updateOnly - execUpdateSet} B/op (grouping + per-save transaction + accept)");

        Assert.True(full > 0);
    }
}
