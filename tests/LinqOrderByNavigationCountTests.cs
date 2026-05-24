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
/// Pins <c>Query&lt;Parent&gt;().OrderBy(p =&gt; p.Children.Count())</c> — a
/// correlated scalar subquery in the ORDER BY clause. ExpressionToSqlVisitor
/// already emits this shape as a correlated subquery in WHERE-side contexts
/// (efba58f). The question for this probe: does OrderByTranslator route
/// through it, or does it fall through to the generic Count emit (which is
/// `COUNT(*)` against the outer table — same silent-wrongness as 0977c64,
/// just in the ORDER BY position)?
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByNavigationCountTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private OncContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OncParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE OncChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
            INSERT INTO OncParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Counts: Alice=3, Bob=1, Carol=0. ASC order: Carol(0), Bob(1), Alice(3).
            INSERT INTO OncChild VALUES (1,1),(2,1),(3,1),(4,2);
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<OncParent>().HasKey(p => p.Id);
                mb.Entity<OncParent>().HasMany<OncChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new OncContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_on_navigation_count_sorts_parents_by_correlated_subquery_value()
    {
        var rows = (await _ctx.Query<OncParent>()
            .OrderBy(p => p.Children.Count())
            .Select(p => p.Name)
            .ToListAsync())
            .ToArray();
        Assert.Equal(new[] { "Carol", "Bob", "Alice" }, rows);
    }

    [Table("OncParent")]
    public sealed class OncParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<OncChild> Children { get; set; } = new();
    }

    [Table("OncChild")]
    public sealed class OncChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private sealed class OncContext : DbContext
    {
        public OncContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
