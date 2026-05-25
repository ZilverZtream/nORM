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
/// Strict pins for <c>string.Format(template, args...)</c> with column
/// arguments in projection. Common templating shape:
///   <c>$"Order #{p.OrderId} total ${p.Total:F2}"</c> compiles to
///   <c>string.Format("Order #{0} total ${1:F2}", p.OrderId, p.Total)</c>.
/// Translator should decompose the literal template + arg list into a
/// concat chain.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringFormatColumnsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsfcItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Qty INTEGER NOT NULL);
            INSERT INTO PsfcItem VALUES
                (1, 'widget', 3),
                (2, 'gadget', 7);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsfcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Format_template_with_column_args_returns_substituted_text_per_row()
    {
        var r = await _ctx.Query<PsfcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = string.Format("{0} x{1}", p.Name, p.Qty) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("widget x3", r[0].V);
        Assert.Equal("gadget x7", r[1].V);
    }

    [Fact]
    public async Task Select_interpolated_string_with_column_args_returns_substituted_text_per_row()
    {
        // C# interpolated strings compile to string.Format under the hood
        // (or string.Create in newer versions, but for simple cases that
        // include only string-typed substitutions, the IL is string.Format).
        var r = await _ctx.Query<PsfcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = $"Item:{p.Name}/{p.Qty}" }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("Item:widget/3", r[0].V);
        Assert.Equal("Item:gadget/7", r[1].V);
    }

    [Table("PsfcItem")]
    public sealed class PsfcItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Qty { get; set; }
    }
}
