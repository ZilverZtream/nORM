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
/// Pins server-side translation of <c>Guid.Parse(stringCol)</c> when comparing
/// against a constant Guid. The natural lowering is an identity pass-through
/// on the string column with the constant Guid bound as its canonical
/// hex-representation string — so the server-side compare reduces to plain
/// string equality without forcing every row through a client-side parse.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGuidParseTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private static readonly Guid TargetGuid = Guid.Parse("11111111-1111-1111-1111-111111111111");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE GpRow (Id INTEGER PRIMARY KEY, RowGuid TEXT NOT NULL);
            INSERT INTO GpRow VALUES
                (1, '11111111-1111-1111-1111-111111111111'),
                (2, '22222222-2222-2222-2222-222222222222'),
                (3, '33333333-3333-3333-3333-333333333333');
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
    public async Task Guid_parse_against_constant_filters_by_string_compare()
    {
        var rows = await _ctx.Query<GpRow>()
            .Where(r => Guid.Parse(r.RowGuid) == TargetGuid)
            .ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Table("GpRow")]
    public sealed class GpRow
    {
        [Key] public int Id { get; set; }
        public string RowGuid { get; set; } = string.Empty;
    }
}
