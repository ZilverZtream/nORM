using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Fourth adversarial leakage batch: projection-side nav quantifiers (Any/Any(pred) inside
/// a Select), scalar root operators over a filtered set (First/Single with a predicate,
/// Count with a predicate), and a SelectMany flattened into a downstream Where. Each seeds
/// a hidden soft-deleted row and asserts it never leaks into the scalar/boolean result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterLeakHuntProjectionScalarTests
{
    [Table("PsPa")]
    private class PsPa
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<PsKid> Kids { get; set; } = new();
    }

    [Table("PsKid")]
    private class PsKid
    {
        [Key] public int Id { get; set; }
        public int PaId { get; set; }
        public string Label { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PsPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE PsKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, Label TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO PsPa  VALUES (1,'only-deleted'),(2,'has-live');
                -- Parent 1's only kid is soft-deleted; parent 2 has a live kid then a deleted kid.
                INSERT INTO PsKid VALUES (10,1,'k1-del',1),(20,2,'k2-live',0),(21,2,'k2-del',1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PsPa>().HasKey(p => p.Id);
                mb.Entity<PsKid>().HasKey(k => k.Id);
                mb.Entity<PsPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        };
        opts.AddGlobalFilter<PsKid>(k => !k.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Projection_side_nav_Any_excludes_softdeleted()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        var rows = (await ctx.Query<PsPa>()
            .Select(p => new { p.Name, HasKids = p.Kids.Any() })
            .ToListAsync()).OrderBy(r => r.Name).ToList();

        // Parent 1's only kid is soft-deleted -> HasKids must be false, not true.
        Assert.False(rows.Single(r => r.Name == "only-deleted").HasKids);
        Assert.True(rows.Single(r => r.Name == "has-live").HasKids);
    }

    [Fact]
    public async Task Projection_side_nav_Any_predicate_excludes_softdeleted()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        var rows = (await ctx.Query<PsPa>()
            .Select(p => new { p.Name, Del = p.Kids.Any(k => k.Label.EndsWith("del")) })
            .ToListAsync()).OrderBy(r => r.Name).ToList();

        // Parent 1's only 'del' kid is soft-deleted; it must not satisfy the projection-side Any.
        Assert.False(rows.Single(r => r.Name == "only-deleted").Del);
        // Parent 2 has a soft-deleted 'k2-del' too -> also must read false.
        Assert.False(rows.Single(r => r.Name == "has-live").Del);
    }

    [Fact]
    public async Task Root_First_with_predicate_skips_softdeleted()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        // Parent 1's only kid is soft-deleted -> FirstOrDefault must return null, not the deleted kid.
        var forP1 = await ctx.Query<PsKid>().FirstOrDefaultAsync(k => k.PaId == 1);
        Assert.Null(forP1);

        // Parent 2: only the live kid (20) is visible.
        var forP2 = await ctx.Query<PsKid>().FirstOrDefaultAsync(k => k.PaId == 2);
        Assert.NotNull(forP2);
        Assert.Equal(20, forP2!.Id);
        Assert.False(forP2.IsDeleted);
    }

    [Fact]
    public async Task Root_Count_with_predicate_excludes_softdeleted()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        // Parent 2 has 2 kids in the table but only 1 visible (kid 21 is soft-deleted).
        var n = await ctx.Query<PsKid>().CountAsync(k => k.PaId == 2);
        Assert.Equal(1, n);

        // Parent 1's only kid is soft-deleted -> count 0.
        var nP1 = await ctx.Query<PsKid>().CountAsync(k => k.PaId == 1);
        Assert.Equal(0, nP1);

        // Whole-table visible count is 1 (only kid 20).
        var nAll = await ctx.Query<PsKid>().CountAsync();
        Assert.Equal(1, nAll);
    }

    [Fact]
    public async Task SelectMany_then_downstream_Where_keeps_global_filter()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        // Flatten kids, then a downstream user Where. The soft-delete filter must survive the
        // downstream Where (both deleted kids 10 and 21 excluded).
        var kids = (await ctx.Query<PsPa>()
            .SelectMany(p => p.Kids)
            .Where(k => k.Label != "nope")
            .ToListAsync()).OrderBy(k => k.Id).ToList();

        Assert.Equal(new[] { 20 }, kids.Select(k => k.Id).ToArray());
        Assert.DoesNotContain(kids, k => k.IsDeleted);
    }

    [Fact]
    public async Task Root_totalCount_excludes_all_softdeleted()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        // Only kid 20 is visible across the whole table.
        var all = (await ctx.Query<PsKid>().ToListAsync()).OrderBy(k => k.Id).ToList();
        Assert.Equal(new[] { 20 }, all.Select(k => k.Id).ToArray());
    }
}
