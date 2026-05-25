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

namespace nORM.Tests;

/// <summary>
/// Strict pin for <c>Math.BigMul(int, int)</c> in projection -- widens
/// the product of two ints to a long. SQLite REAL / INTEGER handles
/// this naturally; the SQL emit is just (a * b) since both operands
/// are integer and SQLite uses 64-bit integers throughout.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathBigMulTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmbmItem (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);
            INSERT INTO PmbmItem VALUES
                (1, 100000, 100000),     -- 1e10, overflows int32
                (2, 2000000000, 2),      -- 4e9, overflows int32
                (3, -1, 5);              -- negative
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmbmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_BigMul_int_columns_widens_product_to_long_per_row()
    {
        var r = await _ctx.Query<PmbmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.BigMul(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(10_000_000_000L, r[0].V);
        Assert.Equal(4_000_000_000L, r[1].V);
        Assert.Equal(-5L, r[2].V);
    }

    [Table("PmbmItem")]
    public sealed class PmbmItem
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
