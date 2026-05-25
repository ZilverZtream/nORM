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
/// Strict pin for projection of closure-captured Guid / TimeSpan /
/// DateTime literal columns alongside an int. SCV.FormatLiteral has the
/// literal emit (9a7ca70) but MaterializerFactory.ExtractColumnsFromProjection
/// previously skipped pure-ConstantExpression args entirely -- the column
/// slot wasn't reserved, so the anonymous-type ctor lookup mismatched
/// arity. Fix mirrors the unary-NOT slot fix (72283a6).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionConstantLiteralCtorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PclItem (Id INTEGER PRIMARY KEY);
            INSERT INTO PclItem VALUES (1), (2);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PclItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Guid_literal_alongside_int_column_returns_canonical_guid_per_row()
    {
        var literal = new Guid("11111111-2222-3333-4444-555555555555");
        var r = await _ctx.Query<PclItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, G = literal }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(literal, r[0].G);
        Assert.Equal(literal, r[1].G);
    }

    [Fact]
    public async Task Select_TimeSpan_literal_alongside_int_column_returns_TimeSpan_per_row()
    {
        var literal = new TimeSpan(2, 30, 0);
        var r = await _ctx.Query<PclItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = literal }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(literal, r[0].T);
        Assert.Equal(literal, r[1].T);
    }

    [Table("PclItem")]
    public sealed class PclItem
    {
        [Key] public int Id { get; set; }
    }
}
