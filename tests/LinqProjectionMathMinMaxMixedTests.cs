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
/// Verify pin for <c>Math.Min / Math.Max</c> with int operands in
/// projection. Provider has the MIN/MAX-2-arg entry; pin guards the
/// round-trip on integer column pairs.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathMinMaxMixedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmmmItem (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);
            INSERT INTO PmmmItem VALUES
                (1, 3, 7),
                (2, 10, 5),
                (3, -1, 0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmmmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Min_two_int_columns_returns_lower_per_row()
    {
        var r = await _ctx.Query<PmmmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Min(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 3, 5, -1 }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_Math_Max_two_int_columns_returns_higher_per_row()
    {
        var r = await _ctx.Query<PmmmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Max(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 7, 10, 0 }, r.Select(x => x.V).ToArray());
    }

    [Table("PmmmItem")]
    public sealed class PmmmItem
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
