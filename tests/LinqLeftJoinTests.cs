using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the supported LEFT JOIN composition shapes.
///
/// Navigation form: `SelectMany(p => p.Children.DefaultIfEmpty(), ...)` —
/// emits LEFT JOIN with the relation's FK predicate.
///
/// Query-syntax form: `from p in P join c in C on p.Id equals c.Pid into
/// grp from c in grp.DefaultIfEmpty() select projection(p, c)` also emits LEFT JOIN.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLeftJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LjParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE LjChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO LjParent VALUES (1,'Alpha'),(2,'Beta'),(3,'Lone');
            INSERT INTO LjChild  VALUES (1,1,'a-1'),(2,1,'a-2'),(3,2,'b-1');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<LjParent>().HasKey(p => p.Id);
                mb.Entity<LjChild>().HasKey(c => c.Id);
                mb.Entity<LjParent>().HasMany(p => p.Children).WithOne()
                                     .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SelectMany_navigation_DefaultIfEmpty_returns_parents_without_children()
    {
        // Navigation form — supported. The lone parent (Id=3) has no children, so it should
        // still appear with a null child reference.
        var rows = (await _ctx.Query<LjParent>()
            .SelectMany(p => p.Children.DefaultIfEmpty(),
                        (p, c) => new { ParentName = p.Name, ChildId = c == null ? (int?)null : c.Id })
            .ToListAsync())
            .OrderBy(r => r.ParentName).ThenBy(r => r.ChildId).ToArray();

        // 3 children + 1 lone parent = 4 rows.
        Assert.Equal(4, rows.Length);
        Assert.Contains(rows, r => r.ParentName == "Lone" && r.ChildId == null);
        Assert.Contains(rows, r => r.ParentName == "Alpha" && r.ChildId == 1);
        Assert.Contains(rows, r => r.ParentName == "Alpha" && r.ChildId == 2);
        Assert.Contains(rows, r => r.ParentName == "Beta"  && r.ChildId == 3);
    }

    [Fact]
    public async Task QuerySyntax_GroupJoin_SelectMany_DefaultIfEmpty_emits_LEFT_JOIN_with_pair_projection()
    {
        // Project the child Tag (string, nullable by default) so the LEFT JOIN NULL row for
        // the lone parent comes back as a real null rather than the int default.
        var rows = (await (from p in _ctx.Query<LjParent>()
                           join c in _ctx.Query<LjChild>() on p.Id equals c.ParentId into grp
                           from c in grp.DefaultIfEmpty()
                           select new { ParentName = p.Name, ChildTag = c.Tag })
                          .ToListAsync())
                  .OrderBy(r => r.ParentName).ThenBy(r => r.ChildTag).ToArray();

        // 3 children + 1 lone parent = 4 rows; the lone parent's ChildTag is null.
        Assert.Equal(4, rows.Length);
        Assert.Contains(rows, r => r.ParentName == "Lone"  && r.ChildTag == null);
        Assert.Contains(rows, r => r.ParentName == "Alpha" && r.ChildTag == "a-1");
        Assert.Contains(rows, r => r.ParentName == "Alpha" && r.ChildTag == "a-2");
        Assert.Contains(rows, r => r.ParentName == "Beta"  && r.ChildTag == "b-1");
    }

    [Table("LjParent")]
    public sealed class LjParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<LjChild> Children { get; set; } = new();
    }

    [Table("LjChild")]
    public sealed class LjChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
