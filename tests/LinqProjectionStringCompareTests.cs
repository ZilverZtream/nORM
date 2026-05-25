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
/// Strict pin for <c>string.Compare(a, b)</c> in projection. .NET returns
/// a negative/zero/positive int; the documented contract only specifies
/// the SIGN. SQLite can't subtract text, but its &lt; / &gt; / =
/// operators on TEXT use BINARY collation by default, which matches the
/// ordinal-ish comparison used here. Emit a CASE that yields -1/0/1 to
/// match .NET's typical Comparer convention.
///
/// Sister to decimal.Compare (e975313) and DateTime.Compare (acc04c8).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PscItem VALUES
                (1, 'banana', 'apple'),
                (2, 'cherry', 'cherry'),
                (3, 'apple',  'banana');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Compare_two_columns_returns_signed_sign_per_row()
    {
        // Test SIGN of the result rather than exact magnitude -- .NET's
        // string.Compare contract only guarantees the sign, not the value.
        var r = await _ctx.Query<PscItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = string.Compare(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].C > 0);    // "banana" > "apple"
        Assert.Equal(0, r[1].C);    // equal
        Assert.True(r[2].C < 0);    // "apple" < "banana"
    }

    [Table("PscItem")]
    public sealed class PscItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
    }
}
