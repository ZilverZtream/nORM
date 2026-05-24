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
/// EF-parity addition: <c>ContainsAsync(value)</c> -- common idiom
/// <c>await q.Select(x =&gt; x.Id).ContainsAsync(42)</c>. Was missing
/// entirely from nORM's async surface despite the translator already
/// routing <c>Queryable.Contains</c> via <c>SetPredicateTranslator</c>
/// (which rewrites <c>Contains(x)</c> as <c>Where(p =&gt; p == x).Any()</c>).
///
/// Unconstrained TSource because the realistic shape is over a primitive
/// projection -- entity-typed Contains is reference-equality and not
/// translatable to useful SQL. Pins behavior on int/string projections
/// and the silent-wrongness probe (dropped value -> returns true for
/// any non-empty source).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqContainsAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ContainsItem (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO ContainsItem VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<ContainsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ContainsAsync_int_projection_returns_true_when_value_present()
    {
        var hit = await _ctx.Query<ContainsItem>().Select(i => i.Id).ContainsAsync(2);
        Assert.True(hit);
    }

    [Fact]
    public async Task ContainsAsync_int_projection_returns_false_when_value_absent()
    {
        // Silent-wrongness: dropped value -> ContainsAsync would return true
        // (any non-empty source) regardless of the missing 999.
        var hit = await _ctx.Query<ContainsItem>().Select(i => i.Id).ContainsAsync(999);
        Assert.False(hit);
    }

    [Fact]
    public async Task ContainsAsync_string_projection_returns_true_when_value_present()
    {
        var hit = await _ctx.Query<ContainsItem>().Select(i => i.Code).ContainsAsync("beta");
        Assert.True(hit);
    }

    [Fact]
    public async Task ContainsAsync_string_projection_returns_false_when_value_absent()
    {
        var hit = await _ctx.Query<ContainsItem>().Select(i => i.Code).ContainsAsync("zeta");
        Assert.False(hit);
    }

    [Fact]
    public async Task ContainsAsync_composes_after_outer_where()
    {
        // Outer Where narrows the source first; ContainsAsync(2) should still
        // find it because Id=2 (Code=beta) passes Code != 'alpha'.
        var hit = await _ctx.Query<ContainsItem>()
            .Where(i => i.Code != "alpha")
            .Select(i => i.Id)
            .ContainsAsync(2);
        Assert.True(hit);
    }

    [Fact]
    public async Task ContainsAsync_returns_false_when_outer_where_excludes_value()
    {
        // Outer Where excludes Id=2; ContainsAsync(2) must respect that.
        var hit = await _ctx.Query<ContainsItem>()
            .Where(i => i.Code == "alpha")
            .Select(i => i.Id)
            .ContainsAsync(2);
        Assert.False(hit);
    }

    [Fact]
    public async Task ContainsAsync_returns_false_on_empty_source()
    {
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM ContainsItem";
            await cmd.ExecuteNonQueryAsync();
        }
        var hit = await _ctx.Query<ContainsItem>().Select(i => i.Id).ContainsAsync(1);
        Assert.False(hit);
    }

    [Table("ContainsItem")]
    public sealed class ContainsItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
