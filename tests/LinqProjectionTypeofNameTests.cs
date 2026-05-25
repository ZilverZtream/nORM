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
/// Strict pin for <c>typeof(T).Name</c> in projection. The expression is
/// a compile-time constant string (the simple type name), so the
/// translator must fold it to a SQL string literal -- not pass through
/// as a Type-member access that has no SQL equivalent.
///
/// The expression tree shape is:
///   MemberExpression(ConstantExpression(typeof(Foo)), Name)
/// which already has a constant root, so the existing constant-fold
/// path should handle it -- this pin guards against future regressions
/// that might lose the fold.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTypeofNameTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtonItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
            INSERT INTO PtonItem VALUES
                (1, 'alpha'),
                (2, 'beta');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtonItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_typeof_T_Name_folds_to_literal_per_row()
    {
        var r = await _ctx.Query<PtonItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = typeof(PtonItem).Name }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("PtonItem", r[0].T);
        Assert.Equal("PtonItem", r[1].T);
    }

    [Fact]
    public async Task Select_typeof_T_FullName_folds_to_literal_per_row()
    {
        var r = await _ctx.Query<PtonItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = typeof(PtonItem).FullName }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(typeof(PtonItem).FullName, r[0].T);
        Assert.Equal(typeof(PtonItem).FullName, r[1].T);
    }

    /// <summary>
    /// <c>p.GetType().Name</c> is runtime per-row in general (would need a
    /// discriminator with inheritance), but for a sealed entity the value
    /// is statically known to be the entity's declared type. nORM should
    /// fold this to the same literal as <c>typeof(PtonItem).Name</c>.
    /// </summary>
    [Fact]
    public async Task Select_entity_GetType_Name_folds_to_declared_type_name()
    {
        var r = await _ctx.Query<PtonItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = p.GetType().Name }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("PtonItem", r[0].T);
        Assert.Equal("PtonItem", r[1].T);
    }

    [Table("PtonItem")]
    public sealed class PtonItem
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
