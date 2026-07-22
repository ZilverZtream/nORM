using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Predicate-side (WHERE clause) aggregates over many-to-many and owned collections —
/// <c>Where(p =&gt; p.Tags.Any(t =&gt; pred))</c>, <c>Count() OP n</c>, and <c>All(t =&gt; pred)</c>. These live in
/// ManyToManyJoins / OwnedCollections (not Relations); Any/Count already emitted correct correlated
/// EXISTS/COUNT subqueries through the bridge/owner FK, but m2m <c>All</c> in a predicate threw
/// NormUnsupportedFeatureException while owned/relation All worked. m2m All is now translated as
/// <c>NOT EXISTS(a visible element WHERE NOT pred)</c> — the same negate-and-NOT-EXISTS shape the
/// query-rooted All uses — so an empty collection is vacuously true, matching C# All semantics.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ManyToManyAndOwnedPredicateAggregateTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MopPost")]
    public sealed class Post
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MopTag")]
    public sealed class Tag
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public bool Active { get; set; }
        public int Weight { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MopOrder")]
    public sealed class Order
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public sealed class Line
    {
        public int Id { get; set; }
        public int OrderId { get; set; }
        public int Qty { get; set; }
    }

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MopPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE MopTag (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL, Weight INTEGER NOT NULL);" +
                "CREATE TABLE MopPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY(PostId, TagId));" +
                "CREATE TABLE MopOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE MopLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Qty INTEGER NOT NULL);" +
                // post1: {active w10, inactive w20}; post2: {inactive w5}; post3: none
                "INSERT INTO MopPost VALUES (1),(2),(3);" +
                "INSERT INTO MopTag VALUES (1,1,10),(2,0,20),(3,0,5);" +
                "INSERT INTO MopPostTag VALUES (1,1),(1,2),(2,3);" +
                // order1: {q10,q20}; order2: {q5}; order3: none
                "INSERT INTO MopOrder VALUES (1),(2),(3);" +
                "INSERT INTO MopLine VALUES (1,1,10),(2,1,20),(3,2,5);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("MopPostTag", "PostId", "TagId");
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "MopLine", foreignKey: "OrderId",
                    buildAction: b => b.HasKey(l => l.Id));
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static int[] PostIds(DbContext ctx, Func<IQueryable<Post>, IQueryable<Post>> q) =>
        q(ctx.Query<Post>()).Select(p => p.Id).OrderBy(x => x).ToArray();
    private static int[] OrderIds(DbContext ctx, Func<IQueryable<Order>, IQueryable<Order>> q) =>
        q(ctx.Query<Order>()).Select(o => o.Id).OrderBy(x => x).ToArray();

    [Fact] public void M2m_Any() { using var c = Ctx(); Assert.Equal(new[] { 1, 2 }, PostIds(c, q => q.Where(p => p.Tags.Any()))); }
    [Fact] public void M2m_Any_predicate() { using var c = Ctx(); Assert.Equal(new[] { 1 }, PostIds(c, q => q.Where(p => p.Tags.Any(t => t.Active)))); }
    [Fact] public void M2m_Count_comparison() { using var c = Ctx(); Assert.Equal(new[] { 1 }, PostIds(c, q => q.Where(p => p.Tags.Count() > 1))); }
    [Fact] public void M2m_Count_predicate() { using var c = Ctx(); Assert.Equal(new[] { 1 }, PostIds(c, q => q.Where(p => p.Tags.Count(t => t.Weight > 8) >= 1))); }

    [Fact] public void M2m_All_satisfied_and_empty_are_true()
    {
        using var c = Ctx();
        // post1 {10,20} all>6 true; post2 {5} false; post3 empty -> vacuously true.
        Assert.Equal(new[] { 1, 3 }, PostIds(c, q => q.Where(p => p.Tags.All(t => t.Weight > 6))));
    }

    [Fact] public void M2m_All_strict_predicate_leaves_only_empty()
    {
        using var c = Ctx();
        // No post has ALL tags > 100 except the empty one (vacuously true).
        Assert.Equal(new[] { 3 }, PostIds(c, q => q.Where(p => p.Tags.All(t => t.Weight > 100))));
    }

    [Fact] public void Owned_Any_predicate() { using var c = Ctx(); Assert.Equal(new[] { 1 }, OrderIds(c, q => q.Where(o => o.Lines.Any(l => l.Qty > 15)))); }
    [Fact] public void Owned_Count_comparison() { using var c = Ctx(); Assert.Equal(new[] { 1 }, OrderIds(c, q => q.Where(o => o.Lines.Count() >= 2))); }

    [Fact] public void Owned_All_satisfied_and_empty_are_true()
    {
        using var c = Ctx();
        Assert.Equal(new[] { 1, 2, 3 }, OrderIds(c, q => q.Where(o => o.Lines.All(l => l.Qty >= 5))));
    }
}
