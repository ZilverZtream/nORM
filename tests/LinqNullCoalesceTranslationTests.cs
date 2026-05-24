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
/// Pins translation of the null-coalescing operator (<c>??</c>) on nullable
/// columns. Common shapes that should lower to SQL <c>COALESCE(col, fallback)</c>:
/// <list type="bullet">
///   <item><c>Where(x =&gt; (x.NullableName ?? "anon") == "anon")</c></item>
///   <item><c>Where(x =&gt; (x.NullableInt ?? 0) &gt; 5)</c></item>
///   <item><c>OrderBy(x =&gt; x.NullableName ?? "")</c></item>
///   <item><c>Select(x =&gt; new { Name = x.NullableName ?? "anon" })</c></item>
/// </list>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNullCoalesceTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NcRow (Id INTEGER PRIMARY KEY, Name TEXT NULL, Quantity INTEGER NULL);
            INSERT INTO NcRow VALUES
                (1, 'alpha', 10),
                (2, NULL,    20),
                (3, 'gamma', NULL),
                (4, NULL,    NULL);
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
    public async Task Null_coalesce_string_in_where_matches_fallback_and_real_values()
    {
        // Rows 2 and 4 have NULL Name → "anon" matches; rows 1 and 3 don't.
        var rows = (await _ctx.Query<NcRow>()
            .Where(r => (r.Name ?? "anon") == "anon")
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(4, rows[1].Id);
    }

    [Fact]
    public async Task Null_coalesce_int_in_where_uses_zero_fallback_for_null_quantity()
    {
        // (Quantity ?? 0) > 5 matches rows with Quantity > 5 OR Quantity NULL→0>5 (none).
        // So rows 1 (10) and 2 (20) only.
        var rows = (await _ctx.Query<NcRow>()
            .Where(r => (r.Quantity ?? 0) > 5)
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
    }

    [Fact]
    public async Task Null_coalesce_in_projection_returns_fallback_for_nulls()
    {
        var names = (await _ctx.Query<NcRow>()
            .Select(r => new { r.Id, Name = r.Name ?? "anon" })
            .ToListAsync())
            .OrderBy(x => x.Id).ToArray();
        Assert.Equal(4, names.Length);
        Assert.Equal("alpha", names[0].Name);
        Assert.Equal("anon",  names[1].Name);
        Assert.Equal("gamma", names[2].Name);
        Assert.Equal("anon",  names[3].Name);
    }

    [Table("NcRow")]
    public sealed class NcRow
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
        public int? Quantity { get; set; }
    }
}
