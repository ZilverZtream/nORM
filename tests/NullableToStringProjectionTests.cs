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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// `Nullable&lt;T&gt;.ToString()` is documented to return String.Empty (never null) when HasValue is false, and
/// 'False' is wrong for a NULL nullable-bool. nORM emitted a bare CAST/CASE that yields SQL NULL (or 'False'
/// for bool) for the null element, so a projected `x.N.ToString()` materialized null — a downstream
/// NullReferenceException hazard and a divergence from LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NullableToStringProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE NtsRow (Id INTEGER PRIMARY KEY, N INTEGER, B INTEGER);" +
            "INSERT INTO NtsRow VALUES (1, 5, 1), (2, NULL, NULL);";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<NtsRow>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync() { _ctx.Dispose(); await _cn.DisposeAsync(); }

    [Fact]
    public async Task Nullable_int_ToString_on_null_returns_empty_string()
    {
        var actual = (await _ctx.Query<NtsRow>().OrderBy(p => p.Id)
            .Select(p => new { V = p.N.ToString() }).ToListAsync()).Select(x => x.V).ToArray();
        Assert.Equal(new[] { ((int?)5).ToString(), ((int?)null).ToString() }, actual); // ["5", ""]
    }

    [Fact]
    public async Task Nullable_bool_ToString_on_null_returns_empty_string()
    {
        var actual = (await _ctx.Query<NtsRow>().OrderBy(p => p.Id)
            .Select(p => new { V = p.B.ToString() }).ToListAsync()).Select(x => x.V).ToArray();
        Assert.Equal(new[] { ((bool?)true).ToString(), ((bool?)null).ToString() }, actual); // ["True", ""]
    }

    [Table("NtsRow")]
    public sealed class NtsRow
    {
        [Key] public int Id { get; set; }
        public int? N { get; set; }
        public bool? B { get; set; }
    }
}
