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
/// Strict pin for bitwise XOR on integer / enum operands in projection
/// (the ExpressionType.ExclusiveOr deferred in f6576cd). SQLite has no
/// native `^` operator; rewrite as `(a | b) &amp; ~(a &amp; b)` which is
/// the standard bit-twiddling identity that works on 64-bit signed
/// integers in two's complement form.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionEnumXorTests : IAsyncLifetime
{
    [Flags]
    public enum Perm { None = 0, A = 1, B = 2, C = 4 }

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PexItem (Id INTEGER PRIMARY KEY, Flags INTEGER NOT NULL);
            INSERT INTO PexItem VALUES
                (1, 0),  -- None
                (2, 3),  -- A|B
                (3, 5),  -- A|C
                (4, 7);  -- A|B|C
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PexItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_enum_XOR_mask_toggles_bits_per_row()
    {
        var r = await _ctx.Query<PexItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, X = (int)(p.Flags ^ Perm.A) }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.Equal(1, r[0].X);   // 0 ^ 1 = 1
        Assert.Equal(2, r[1].X);   // 3 ^ 1 = 2
        Assert.Equal(4, r[2].X);   // 5 ^ 1 = 4
        Assert.Equal(6, r[3].X);   // 7 ^ 1 = 6
    }

    [Table("PexItem")]
    public sealed class PexItem
    {
        [Key] public int Id { get; set; }
        public Perm Flags { get; set; }
    }
}
