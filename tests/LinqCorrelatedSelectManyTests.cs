using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Correlated SelectMany: p =&gt; ctx.Query&lt;Child&gt;().Where(c =&gt; c.FK == p.PK)
/// must produce INNER JOIN ... ON [predicate], not a cartesian CROSS JOIN.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCorrelatedSelectManyTests : IAsyncLifetime
{
    [Table("CsmParent")]
    public sealed class CsmParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("CsmChild")]
    public sealed class CsmChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CsmParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE CsmChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO CsmParent VALUES (1,'Alpha'),(2,'Beta'),(3,'Gamma'),(4,'Lone');
            INSERT INTO CsmChild  VALUES (1,1,'A1'),(2,1,'A2'),(3,2,'B1'),(4,3,'C1'),(5,3,'C2'),(6,3,'C3');
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
    public void CorrelatedSelectMany_produces_INNER_JOIN_not_CROSS_JOIN()
    {
        var q = _ctx.Query<CsmParent>()
            .SelectMany(p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id));

        var sql = q.ToString()!;
        Assert.Contains("INNER JOIN", sql, System.StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CROSS JOIN", sql, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CorrelatedSelectMany_returns_correct_row_count()
    {
        var results = await _ctx.Query<CsmParent>()
            .SelectMany(p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id))
            .ToListAsync();

        // 3 parents × varying children = 6 children total; no cartesian explosion
        Assert.Equal(6, results.Count);
    }

    [Fact]
    public async Task CorrelatedSelectMany_children_belong_to_correct_parents()
    {
        var results = await _ctx.Query<CsmParent>()
            .SelectMany(p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id))
            .ToListAsync();

        Assert.Equal(2, results.Count(c => c.ParentId == 1));
        Assert.Equal(1, results.Count(c => c.ParentId == 2));
        Assert.Equal(3, results.Count(c => c.ParentId == 3));
    }

    [Fact]
    public async Task CorrelatedSelectMany_with_outer_Where_filters_parents()
    {
        var results = await _ctx.Query<CsmParent>()
            .Where(p => p.Id > 1)
            .SelectMany(p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id))
            .ToListAsync();

        // Beta (Id=2): 1 child; Gamma (Id=3): 3 children = 4 total; no Alpha children
        Assert.Equal(4, results.Count);
        Assert.DoesNotContain(results, c => c.ParentId == 1);
    }

    [Fact]
    public async Task CorrelatedSelectMany_result_selector_projects_both_sides()
    {
        var results = await _ctx.Query<CsmParent>()
            .SelectMany(
                p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id),
                (p, c) => new { ParentName = p.Name, ChildTag = c.Tag })
            .ToListAsync();

        Assert.Equal(6, results.Count);
        Assert.Contains(results, r => r.ParentName == "Alpha" && r.ChildTag == "A1");
        Assert.Contains(results, r => r.ParentName == "Alpha" && r.ChildTag == "A2");
        Assert.Contains(results, r => r.ParentName == "Beta"  && r.ChildTag == "B1");
        Assert.Contains(results, r => r.ParentName == "Gamma" && r.ChildTag == "C3");
    }

    [Fact]
    public async Task CorrelatedSelectMany_DefaultIfEmpty_keeps_unmatched_parents()
    {
        var rows = await _ctx.Query<CsmParent>()
            .SelectMany(
                p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id).DefaultIfEmpty(),
                (p, c) => new { ParentName = p.Name, ChildId = c == null ? (int?)null : c.Id })
            .ToListAsync();

        // 6 children + the childless parent with a null child side = 7 rows.
        Assert.Equal(7, rows.Count);
        Assert.Contains(rows, r => r.ParentName == "Lone" && r.ChildId == null);
        Assert.Contains(rows, r => r.ParentName == "Alpha" && r.ChildId == 1);
        Assert.Contains(rows, r => r.ParentName == "Gamma" && r.ChildId == 6);
    }

    [Fact]
    public async Task CorrelatedSelectMany_DefaultIfEmpty_entity_result_materializes_null_elements()
    {
        var entities = await _ctx.Query<CsmParent>()
            .SelectMany(p => _ctx.Query<CsmChild>().Where(c => c.ParentId == p.Id).DefaultIfEmpty())
            .ToListAsync();

        // DefaultIfEmpty semantics: the childless parent contributes a null element.
        Assert.Equal(7, entities.Count);
        Assert.Equal(1, entities.Count(e => e == null));
        Assert.Equal(6, entities.Count(e => e != null));
    }

    [Fact]
    public async Task CorrelatedSelectMany_additional_predicate_in_correlated_Where()
    {
        var results = await _ctx.Query<CsmParent>()
            .SelectMany(p => _ctx.Query<CsmChild>()
                .Where(c => c.ParentId == p.Id && c.Tag.StartsWith("A")))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, c => Assert.Equal(1, c.ParentId));
        Assert.All(results, c => Assert.StartsWith("A", c.Tag));
    }
}
