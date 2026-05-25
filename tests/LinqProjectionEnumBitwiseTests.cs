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
/// Strict pin for bitwise operators on enum-typed flags columns in
/// projection: <c>(p.Flags &amp; Mask) != 0</c>, <c>(p.Flags | Mask)</c>,
/// <c>(p.Flags ^ Mask)</c>. Common HasFlag-style intent expressed as
/// bitwise arithmetic on the underlying integer. SQLite's &amp; | ^
/// operators apply to INTEGER columns directly so the emit should be
/// the operator-equivalent.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionEnumBitwiseTests : IAsyncLifetime
{
    [Flags]
    public enum Perm
    {
        None = 0,
        Read = 1,
        Write = 2,
        Execute = 4,
        ReadWrite = Read | Write,
    }

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PebItem (Id INTEGER PRIMARY KEY, Flags INTEGER NOT NULL);
            INSERT INTO PebItem VALUES
                (1, 1),   -- Read
                (2, 3),   -- ReadWrite
                (3, 7),   -- Read|Write|Execute
                (4, 4),   -- Execute only
                (5, 0);   -- None
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PebItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_enum_AND_mask_nonzero_flags_rows_that_contain_mask_bit()
    {
        var r = await _ctx.Query<PebItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, HasRead = (p.Flags & Perm.Read) != 0 }).ToListAsync();
        Assert.Equal(5, r.Count);
        Assert.True(r[0].HasRead);   // 1 & 1 != 0
        Assert.True(r[1].HasRead);   // 3 & 1 != 0
        Assert.True(r[2].HasRead);   // 7 & 1 != 0
        Assert.False(r[3].HasRead);  // 4 & 1 == 0
        Assert.False(r[4].HasRead);  // 0 & 1 == 0
    }

    [Fact]
    public async Task Select_enum_OR_mask_projects_added_bits()
    {
        var r = await _ctx.Query<PebItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Combined = (int)(p.Flags | Perm.Execute) }).ToListAsync();
        Assert.Equal(5, r.Count);
        Assert.Equal(5, r[0].Combined);  // 1 | 4
        Assert.Equal(7, r[1].Combined);  // 3 | 4
        Assert.Equal(7, r[2].Combined);  // 7 | 4
        Assert.Equal(4, r[3].Combined);  // 4 | 4
        Assert.Equal(4, r[4].Combined);  // 0 | 4
    }

    [Table("PebItem")]
    public sealed class PebItem
    {
        [Key] public int Id { get; set; }
        public Perm Flags { get; set; }
    }
}
