using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the EF-parity raw non-query surface on <see cref="DatabaseFacade"/>:
/// ExecuteSqlRaw(Async) and ExecuteSqlInterpolated(Async) run parameterized INSERT/UPDATE/DELETE,
/// return the affected row count, and invalidate the result cache for the written table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DatabaseExecuteSqlRawContractTests
{
    [Table("EsrRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    private static DbContext NewCtx(SqliteConnection cn, IDbCacheProvider? cache = null)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id),
            CacheProvider = cache
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EsrRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL);
            INSERT INTO EsrRow VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task ExecuteSqlRawAsync_runs_a_parameterized_update_and_returns_affected_rows()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var affected = await ctx.Database.ExecuteSqlRawAsync(
            "UPDATE EsrRow SET Name = @p0 WHERE Value >= @p1", CancellationToken.None, "hit", 20);

        Assert.Equal(2, affected);

        await using var verify = NewCtx(cn);
        Assert.Equal(2, await verify.Query<Row>().Where(r => r.Name == "hit").CountAsync());
    }

    [Fact]
    public async Task ExecuteSqlInterpolatedAsync_parameterizes_interpolation_holes()
    {
        using var cn = OpenDb();
        await using var ctx = NewCtx(cn);

        var name = "int'erp";   // the apostrophe proves the value is bound as a parameter, not concatenated
        var id = 1;
        var affected = await ctx.Database.ExecuteSqlInterpolatedAsync($"UPDATE EsrRow SET Name = {name} WHERE Id = {id}");

        Assert.Equal(1, affected);
        await using var verify = NewCtx(cn);
        Assert.Equal("int'erp", (await verify.Query<Row>().FirstOrDefaultAsync(r => r.Id == 1))!.Name);
    }

    [Fact]
    public void ExecuteSqlRaw_sync_runs_a_delete()
    {
        using var cn = OpenDb();
        using var ctx = NewCtx(cn);

        var affected = ctx.Database.ExecuteSqlRaw("DELETE FROM EsrRow WHERE Id = @p0", 3);

        Assert.Equal(1, affected);
        Assert.Equal(2, ctx.Query<Row>().Count());
    }

    [Fact]
    public async Task ExecuteSqlRaw_invalidates_the_result_cache_for_the_target_table()
    {
        using var cn = OpenDb();
        var cache = new NormMemoryCacheProvider(null);
        await using var writer = NewCtx(cn, cache);

        // A first context populates the shared result cache for EsrRow.
        await using (var reader1 = NewCtx(cn, cache))
        {
            var before = await reader1.Query<Row>().Where(r => r.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
            Assert.Equal("a", before.Single().Name);
        }

        await writer.Database.ExecuteSqlRawAsync("UPDATE EsrRow SET Name = 'z' WHERE Id = 1");

        // A fresh context (empty identity map, shared cache) must not be served the stale cached result.
        await using var reader2 = NewCtx(cn, cache);
        var after = await reader2.Query<Row>().Where(r => r.Id == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal("z", after.Single().Name);
    }
}
