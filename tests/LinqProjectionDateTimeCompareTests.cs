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
/// Strict pin for <c>DateTime.Compare(a, b)</c> in projection. .NET
/// returns -1/0/1 indicating less/equal/greater. SQLite's julianday()
/// converts both operands to fractional days since the Julian epoch;
/// SIGN(julianday(a) - julianday(b)) yields the same triple. Sister
/// to decimal.Compare (e975313).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtcItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PdtcItem VALUES
                (1, '2026-05-25 12:00:00', '2026-05-24 12:00:00'),  -- A > B
                (2, '2026-05-25 12:00:00', '2026-05-25 12:00:00'),  -- A == B
                (3, '2026-05-24 12:00:00', '2026-05-25 12:00:00');  -- A < B
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_Compare_two_columns_returns_sign_per_row()
    {
        var r = await _ctx.Query<PdtcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = DateTime.Compare(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(1, r[0].C);
        Assert.Equal(0, r[1].C);
        Assert.Equal(-1, r[2].C);
    }

    [Table("PdtcItem")]
    public sealed class PdtcItem
    {
        [Key] public int Id { get; set; }
        public DateTime A { get; set; }
        public DateTime B { get; set; }
    }
}
