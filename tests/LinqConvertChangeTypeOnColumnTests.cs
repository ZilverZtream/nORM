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
/// Pins <c>Convert.ChangeType(col, typeof(T))</c> in projection. The
/// target type is conveyed as a <see cref="Type"/> constant in the
/// expression tree; the translator can pattern-match it at build time
/// and emit the equivalent CAST. Untranslatable shapes (runtime-variable
/// target type) remain unsupported and surface as the existing throw.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqConvertChangeTypeOnColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CcTRow (Id INTEGER PRIMARY KEY, IntVal INTEGER NOT NULL, TextVal TEXT NOT NULL);
            INSERT INTO CcTRow VALUES (1, 42, '99'), (2, 7, '13');
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
    public async Task Convert_ChangeType_int_column_to_string_emits_cast()
    {
        var rows = (await _ctx.Query<CcTRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, S = (string)Convert.ChangeType(r.IntVal, typeof(string)) })
            .ToListAsync())
            .ToArray();
        Assert.Equal("42", rows[0].S);
        Assert.Equal("7",  rows[1].S);
    }

    [Fact]
    public async Task Convert_ChangeType_text_column_to_int_emits_cast()
    {
        var rows = (await _ctx.Query<CcTRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, N = (int)Convert.ChangeType(r.TextVal, typeof(int)) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(99, rows[0].N);
        Assert.Equal(13, rows[1].N);
    }

    [Table("CcTRow")]
    public sealed class CcTRow
    {
        [Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string TextVal { get; set; } = "";
    }
}
