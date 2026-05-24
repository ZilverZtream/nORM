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
/// Positive regression for the workaround recommended in <c>dad1fec</c>: when a
/// projection needs a correlated count, use a navigation property
/// (<c>parent.Children.Count()</c>) rather than the unsupported direct context
/// query (<c>ctx.Query&lt;C&gt;().Count(c => c.Pid == p.Id)</c>). The navigation
/// form is what <c>efba58f</c> made nORM emit as a correlated scalar subquery:
/// <c>(SELECT COUNT(*) FROM Child WHERE Child.PFK = Parent.PK)</c>. If this
/// stops working, the dad1fec error message would be actively misleading.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationCountInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private NcpContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NcpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NcpChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO NcpParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO NcpChild  VALUES (1,1,'a'),(2,1,'b'),(3,1,'c'),(4,2,'d'),(5,2,'e');
            -- Carol has zero children — subquery must produce 0, not drop the row.
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NcpParent>().HasKey(p => p.Id);
                mb.Entity<NcpParent>()
                    .HasMany<NcpChild>(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new NcpContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Navigation_children_count_in_projection_emits_correlated_subquery_and_returns_correct_counts()
    {
        var rows = (await _ctx.Query<NcpParent>()
            .Select(p => new { p.Name, ChildCount = p.Children.Count() })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        var dump = string.Join(" | ", rows.Select(r => $"{r.Name}={r.ChildCount}"));
        Assert.True(rows.Length == 3, $"Expected 3 rows, got {rows.Length}: [{dump}]");
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(3, rows[0].ChildCount);
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(2, rows[1].ChildCount);
        Assert.Equal("Carol", rows[2].Name); Assert.Equal(0, rows[2].ChildCount);
    }

    [Table("NcpParent")]
    public sealed class NcpParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NcpChild> Children { get; set; } = new();
    }

    [Table("NcpChild")]
    public sealed class NcpChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private sealed class NcpContext : DbContext
    {
        public NcpContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
