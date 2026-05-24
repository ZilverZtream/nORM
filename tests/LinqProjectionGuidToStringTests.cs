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
/// Strict pin for <c>Guid.ToString()</c> in projection. Microsoft.Data.Sqlite
/// binds Guid as canonical 'D'-format text (xxxxxxxx-xxxx-xxxx-xxxx-
/// xxxxxxxxxxxx); the projection fall-through to CAST AS TEXT yields the
/// same text. This pin locks in the round-trip so a future regression that
/// switched to BLOB storage (which CAST AS TEXT renders as raw bytes) or
/// stripped the dashes would visibly fail.
///
/// Silent-wrongness shapes:
///   * BLOB storage round-trip -> CAST returns 16 raw bytes interpreted as
///     text -- gibberish or empty string.
///   * 'N' format (no dashes) leaking through -> 32-char hex instead of
///     36-char canonical D-form.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionGuidToStringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private static readonly Guid GuidA = new("11111111-2222-3333-4444-555555555555");
    private static readonly Guid GuidB = new("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PgtsItem (Id INTEGER PRIMARY KEY, Token TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        await using var ins = _cn.CreateCommand();
        ins.CommandText = "INSERT INTO PgtsItem (Id, Token) VALUES ($id, $t)";
        var pid = ins.CreateParameter(); pid.ParameterName = "$id"; ins.Parameters.Add(pid);
        var pt = ins.CreateParameter(); pt.ParameterName = "$t"; ins.Parameters.Add(pt);
        foreach (var (id, g) in new[] { (1, GuidA), (2, GuidB) })
        {
            pid.Value = id;
            pt.Value = g.ToString();
            await ins.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PgtsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Guid_ToString_projects_canonical_D_format_text()
    {
        var result = await _ctx.Query<PgtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Token.ToString() })
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(GuidA.ToString(), result[0].S);
        Assert.Equal(GuidB.ToString(), result[1].S);
    }

    [Table("PgtsItem")]
    public sealed class PgtsItem
    {
        [Key] public int Id { get; set; }
        public Guid Token { get; set; }
    }
}
