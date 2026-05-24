using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Sister probe of <see cref="LinqNavigationCountInProjectionTests"/> for the
/// filtered shape <c>parent.Children.Where(predicate).Count()</c>. The 49ac214
/// commit added <c>Where</c> to <c>_sqlTranslatableMethods</c> so the analyzer
/// doesn't reject the projection, but SCV's <c>EmitNavigationCountSubquery</c>
/// only emits a bare <c>COUNT(*) FROM Child WHERE FK = …</c> — the filter
/// predicate must be AND-ed in.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationFilteredCountInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private NfcContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NfcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NfcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, IsActive INTEGER NOT NULL);
            INSERT INTO NfcParent VALUES (1,'Alice'),(2,'Bob');
            -- Alice: 3 children, 2 active. Bob: 2 children, 0 active.
            INSERT INTO NfcChild VALUES
                (1,1,1),(2,1,1),(3,1,0),
                (4,2,0),(5,2,0);
            """;
        await cmd.ExecuteNonQueryAsync();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NfcParent>().HasKey(p => p.Id);
                mb.Entity<NfcParent>()
                    .HasMany<NfcChild>(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new NfcContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Navigation_children_where_count_in_projection_applies_filter_inside_subquery()
    {
        var rows = (await _ctx.Query<NfcParent>()
            .Select(p => new
            {
                p.Name,
                ActiveCount = p.Children.Where(c => c.IsActive == 1).Count()
            })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(2, rows[0].ActiveCount);
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(0, rows[1].ActiveCount);
    }

    [Table("NfcParent")]
    public sealed class NfcParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NfcChild> Children { get; set; } = new();
    }

    [Table("NfcChild")]
    public sealed class NfcChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int IsActive { get; set; }
    }

    private sealed class NfcContext : DbContext
    {
        public NfcContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
