using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The SQLite fast path reuses a pooled prepared command across calls (parity with the full query-plan
/// path), rebinding the parameter value each time rather than allocating a fresh command. This pins that
/// repeated fast-path queries with DIFFERENT parameter values return the correct rows — a stale reused
/// parameter would silently return the previous call's result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathPreparedCommandReuseTests
{
    [Table("FppWidget")]
    private class Widget
    {
        [Key] public int Id { get; set; }
        public int Kind { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id)
    }, ownsConnection: false);

    [Fact]
    public async Task Repeated_fast_path_queries_rebind_the_parameter_each_call()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FppWidget (Id INTEGER PRIMARY KEY, Kind INTEGER NOT NULL, Name TEXT NOT NULL);
                INSERT INTO FppWidget VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c'), (4, 20, 'd'), (5, 30, 'e');
                """;
            cmd.ExecuteNonQuery();
        }

        await using var ctx = Ctx(cn);

        // Same context → same pooled prepared command reused; each call must rebind @p0 to the new value.
        for (int round = 0; round < 3; round++)
        {
            var kind10 = await NormAsyncExtensions.ToListAsync(ctx.Query<Widget>().Where(w => w.Kind == 10));
            Assert.Equal(new[] { "a", "b" }, kind10.Select(w => w.Name).OrderBy(n => n));

            var kind20 = await NormAsyncExtensions.ToListAsync(ctx.Query<Widget>().Where(w => w.Kind == 20));
            Assert.Equal(new[] { "c", "d" }, kind20.Select(w => w.Name).OrderBy(n => n));

            var kind30 = await NormAsyncExtensions.ToListAsync(ctx.Query<Widget>().Where(w => w.Kind == 30));
            Assert.Equal(new[] { "e" }, kind30.Select(w => w.Name).OrderBy(n => n));

            var kindNone = await NormAsyncExtensions.ToListAsync(ctx.Query<Widget>().Where(w => w.Kind == 99));
            Assert.Empty(kindNone);
        }
    }
}
