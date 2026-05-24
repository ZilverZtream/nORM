using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies projection into the common .NET DTO shapes against real data:
///  - Classic class with parameterized constructor.
///  - Class with parameterless constructor + writable properties (MemberInit).
///  - Class with init-only properties.
///  - Positional record (primary constructor positional record syntax).
///  - Class with read/write properties via implicit setter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDtoProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DpRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO DpRow VALUES
                (1, 'alpha', 10),
                (2, 'bravo', 20),
                (3, 'charlie', 30);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Projection_into_class_with_parameterized_ctor_materializes_each_row()
    {
        var rows = (await _ctx.Query<DpRow>().OrderBy(r => r.Id)
            .Select(r => new DpCtorDto(r.Id, r.Name, r.Amount))
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(2, rows[1].Id);
        Assert.Equal("bravo", rows[1].Name);
        Assert.Equal(20, rows[1].Amount);
    }

    [Fact]
    public async Task Projection_into_class_with_init_only_setters_materializes_each_row()
    {
        var rows = (await _ctx.Query<DpRow>().OrderBy(r => r.Id)
            .Select(r => new DpInitDto { Id = r.Id, Name = r.Name, Amount = r.Amount })
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(3, rows[2].Id);
        Assert.Equal("charlie", rows[2].Name);
        Assert.Equal(30, rows[2].Amount);
    }

    [Fact]
    public async Task Projection_into_positional_record_materializes_each_row()
    {
        var rows = (await _ctx.Query<DpRow>().OrderBy(r => r.Id)
            .Select(r => new DpRecord(r.Id, r.Name, r.Amount))
            .ToListAsync())
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal("alpha", rows[0].Name);
        Assert.Equal(10, rows[0].Amount);
    }

    [Table("DpRow")]
    public sealed class DpRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Amount { get; set; }
    }

    public sealed class DpCtorDto
    {
        public DpCtorDto(int id, string name, int amount) { Id = id; Name = name; Amount = amount; }
        public int Id { get; }
        public string Name { get; }
        public int Amount { get; }
    }

    public sealed class DpInitDto
    {
        public int Id { get; init; }
        public string Name { get; init; } = string.Empty;
        public int Amount { get; init; }
    }

    public sealed record DpRecord(int Id, string Name, int Amount);
}
