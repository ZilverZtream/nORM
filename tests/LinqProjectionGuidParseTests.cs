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
/// Strict pin for <c>Guid.Parse(string)</c> in projection. Sister to
/// bool.Parse / numeric.Parse / DateTime.Parse. SQLite stores Guid as
/// canonical text via Microsoft.Data.Sqlite -- the SQL emission is just
/// identity and GetGuid parses the result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionGuidParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private static readonly Guid A = new("11111111-2222-3333-4444-555555555555");
    private static readonly Guid B = new("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PgpItem (Id INTEGER PRIMARY KEY, TokenText TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        await using var ins = _cn.CreateCommand();
        ins.CommandText = "INSERT INTO PgpItem (Id, TokenText) VALUES ($id, $t)";
        var pid = ins.CreateParameter(); pid.ParameterName = "$id"; ins.Parameters.Add(pid);
        var pt = ins.CreateParameter(); pt.ParameterName = "$t"; ins.Parameters.Add(pt);
        foreach (var (id, g) in new[] { (1, A), (2, B) })
        {
            pid.Value = id;
            pt.Value = g.ToString();
            await ins.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PgpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Guid_Parse_string_column_projects_Guid_per_row()
    {
        var result = await _ctx.Query<PgpItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, G = Guid.Parse(p.TokenText) })
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(A, result[0].G);
        Assert.Equal(B, result[1].G);
    }

    [Table("PgpItem")]
    public sealed class PgpItem
    {
        [Key] public int Id { get; set; }
        public string TokenText { get; set; } = string.Empty;
    }
}
