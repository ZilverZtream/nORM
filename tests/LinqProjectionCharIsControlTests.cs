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
/// Strict pin + implement-first for <c>char.IsControl</c> in projection AND
/// Where. Last of the char-static-predicate set after IsDigit/IsLetter/
/// IsWhiteSpace/IsUpper/IsLower/IsPunctuation/IsSymbol. ASCII control chars
/// are codepoints 0-31 plus 127 (DEL).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharIsControlTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PcicItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Use raw INSERT so seed control chars survive (test-data probe).
        // \t (0x09) and \n (0x0A) are controls; '!' and 'A' are not.
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PcicItem (Id, Name) VALUES ($id, $n)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var pn = insert.CreateParameter(); pn.ParameterName = "$n"; insert.Parameters.Add(pn);
        var rows = new (int id, string name)[]
        {
            (1, "\tindent"),     // tab (0x09)
            (2, "\nnewline"),    // LF (0x0A)
            (3, "!alert"),       // punctuation
            (4, "Aletter"),      // letter
        };
        foreach (var (id, name) in rows)
        {
            pid.Value = id;
            pn.Value = name;
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PcicItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_IsControl_first_char_returns_true_only_for_control_rows()
    {
        var result = await _ctx.Query<PcicItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = char.IsControl(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].C);   // tab
        Assert.True(result[1].C);   // LF
        Assert.False(result[2].C);  // '!'
        Assert.False(result[3].C);  // 'A'
    }

    [Fact]
    public async Task Where_char_IsControl_first_char_filters_control_rows()
    {
        var result = await _ctx.Query<PcicItem>()
            .Where(p => char.IsControl(p.Name[0]))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PcicItem")]
    public sealed class PcicItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
