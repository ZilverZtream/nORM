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
/// Sister of <see cref="LinqNavigationCountInProjectionTests"/> for the scalar
/// aggregates Sum / Average / Min / Max in a Select projection. nORM's
/// ExpressionToSqlVisitor recognises <c>parent.Children.Select(c => c.X).Sum()</c>
/// (efba58f) and emits a correlated subquery against the dependent table — but
/// 0977c64 only routed Count / LongCount / Any / All through SelectClauseVisitor's
/// new path. Sum / Avg / Min / Max in a SELECT projection would still fall through
/// to the generic aggregate emitter and produce <c>SUM(column) FROM Parent</c>
/// instead of a correlated subquery.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationScalarAggregateInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private NsaContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NsaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NsaChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount REAL NOT NULL);
            INSERT INTO NsaParent VALUES (1,'Alice'),(2,'Bob');
            INSERT INTO NsaChild  VALUES (1,1,10.0),(2,1,20.0),(3,1,30.0),(4,2, 5.0),(5,2,15.0);
            """;
        await cmd.ExecuteNonQueryAsync();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NsaParent>().HasKey(p => p.Id);
                mb.Entity<NsaParent>()
                    .HasMany<NsaChild>(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new NsaContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Navigation_children_sum_select_in_projection_emits_correlated_subquery_and_returns_per_parent_totals()
    {
        // Per-parent totals: Alice=60, Bob=20.
        var rows = (await _ctx.Query<NsaParent>()
            .Select(p => new { p.Name, Total = p.Children.Select(c => c.Amount).Sum() })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(60.0, rows[0].Total);
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(20.0, rows[1].Total);
    }

    [Table("NsaParent")]
    public sealed class NsaParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NsaChild> Children { get; set; } = new();
    }

    [Table("NsaChild")]
    public sealed class NsaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public double Amount { get; set; }
    }

    private sealed class NsaContext : DbContext
    {
        public NsaContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
