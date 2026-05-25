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
/// Strict pin for <c>DateOnly.FromDayNumber(int)</c> in projection --
/// inverse of DateOnly.DayNumber (2fb4db4). Days since 0001-01-01 ->
/// DateOnly. SQLite expresses this via julianday-add then date() to
/// reformat as 'YYYY-MM-DD' which the materializer parses back to
/// DateOnly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateOnlyFromDayNumberTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE PdofdnItem (Id INTEGER PRIMARY KEY, N INTEGER NOT NULL);
            INSERT INTO PdofdnItem VALUES
                (1, {new DateOnly(2026, 5, 25).DayNumber}),
                (2, {new DateOnly(2000, 1, 1).DayNumber});
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdofdnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateOnly_FromDayNumber_int_column_returns_DateOnly_per_row()
    {
        var r = await _ctx.Query<PdofdnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = DateOnly.FromDayNumber(p.N) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(new DateOnly(2026, 5, 25), r[0].D);
        Assert.Equal(new DateOnly(2000, 1, 1), r[1].D);
    }

    [Table("PdofdnItem")]
    public sealed class PdofdnItem
    {
        [Key] public int Id { get; set; }
        public int N { get; set; }
    }
}
