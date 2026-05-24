using System;
using System.Collections.Generic;
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
/// Pins LINQ `Cast&lt;T&gt;()` and `OfType&lt;T&gt;()` translation. Both methods
/// are identity operations at the SQL layer when the target type equals (or for
/// reference types, is assignable from) the element type — the CLR cast happens
/// on materialization. The translator must accept these methods rather than
/// throw `NormUnsupportedFeatureException`, so chained query syntax like
/// `query.Cast&lt;TBase&gt;().Where(...)` works.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCastOfTypeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CastRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO CastRow VALUES (1,'a'),(2,'b'),(3,'c');
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
    public async Task Cast_to_same_element_type_is_identity_passthrough()
    {
        // `Cast<CastRow>()` on `IQueryable<CastRow>` is a no-op — the translator
        // must not throw and must produce the same SQL as the source query.
        var rows = (await _ctx.Query<CastRow>().Cast<CastRow>().ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("a", rows[0].Name);
        Assert.Equal("b", rows[1].Name);
        Assert.Equal("c", rows[2].Name);
    }

    [Fact]
    public async Task Cast_then_Where_filters_after_identity_cast()
    {
        var rows = await _ctx.Query<CastRow>().Cast<CastRow>().Where(r => r.Name != "b").ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Name == "a");
        Assert.Contains(rows, r => r.Name == "c");
    }

    [Fact]
    public async Task OfType_with_same_element_type_returns_every_row()
    {
        // `OfType<CastRow>()` against `IQueryable<CastRow>` filters by IS — every
        // row qualifies because the source is already that type. Must not throw.
        var rows = await _ctx.Query<CastRow>().OfType<CastRow>().ToListAsync();
        Assert.Equal(3, rows.Count);
    }

    [Table("CastRow")]
    public sealed class CastRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
