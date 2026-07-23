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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An owned collection persists with replace semantics (the owner's Modified save reconciles the owned
/// child table against the current in-memory collection). That is exactly where rows silently vanish:
/// a removed item must be orphan-deleted, a modified item's new value must win, a cleared-then-refilled
/// collection must land the refill, and — the subtle one — a SECOND save after a first must reconcile
/// against what the first save actually persisted, not a stale snapshot. Example tests cover add/remove
/// in isolation; this drives random edit sequences with saves interspersed and checks the raw child
/// table against a reference multiset after every save, so a dropped, duplicated, or stale row surfaces.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class OwnedCollectionWriteFuzzTests
{
    [Table("OwPost")]
    public sealed class OwPost
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<OwLine> Lines { get; set; } = new();
    }

    public sealed class OwLine
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    private static SqliteConnection NewKeeperWithSeed(IReadOnlyList<string> initial)
    {
        var keeper = new SqliteConnection($"Data Source=file:owf_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OwPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OwLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO OwPost VALUES (1, 'p');
                """;
            cmd.ExecuteNonQuery();
        }
        foreach (var t in initial)
        {
            using var c2 = keeper.CreateCommand();
            c2.CommandText = "INSERT INTO OwLine (PostId, Text) VALUES (1, $t)";
            c2.Parameters.AddWithValue("$t", t);
            c2.ExecuteNonQuery();
        }
        return keeper;
    }

    private static DbContext Make(SqliteConnection keeper)
    {
        var cn = new SqliteConnection(keeper.ConnectionString);
        cn.Open();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OwPost>().OwnsMany<OwLine>(p => p.Lines, tableName: "OwLine", foreignKey: "PostId")
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static OwPost Load(DbContext ctx) =>
        ((INormQueryable<OwPost>)ctx.Query<OwPost>()).Include(p => p.Lines).ToList().Single();

    private static List<string> DbLines(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Text FROM OwLine WHERE PostId = 1";
        using var r = cmd.ExecuteReader();
        var v = new List<string>();
        while (r.Read()) v.Add(r.GetString(0));
        return v;
    }

    private static List<string> Sorted(IEnumerable<string> xs) =>
        xs.OrderBy(x => x, StringComparer.Ordinal).ToList();

    private static void AssertMatches(List<string> model, SqliteConnection keeper, string where) =>
        Assert.True(Sorted(model).SequenceEqual(Sorted(DbLines(keeper))),
            $"{where}: expected [{string.Join(",", Sorted(model))}] but child table had [{string.Join(",", Sorted(DbLines(keeper)))}]");

    // ── Keeper battery ───────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Removing_an_item_orphan_deletes_it()
    {
        using var keeper = NewKeeperWithSeed(new[] { "a", "b", "c" });
        await using var ctx = Make(keeper);
        var post = Load(ctx);

        post.Lines.RemoveAll(l => l.Text == "b");
        await ctx.SaveChangesAsync();

        AssertMatches(new List<string> { "a", "c" }, keeper, "after remove");
    }

    [Fact]
    public async Task Modifying_an_items_value_persists_the_new_value()
    {
        using var keeper = NewKeeperWithSeed(new[] { "a", "b" });
        await using var ctx = Make(keeper);
        var post = Load(ctx);

        post.Lines.Single(l => l.Text == "a").Text = "A!";
        await ctx.SaveChangesAsync();

        AssertMatches(new List<string> { "A!", "b" }, keeper, "after modify");
    }

    [Fact]
    public async Task Clearing_then_refilling_the_same_texts_persists_the_refill()
    {
        using var keeper = NewKeeperWithSeed(new[] { "a", "b" });
        await using var ctx = Make(keeper);
        var post = Load(ctx);

        post.Lines.Clear();
        post.Lines.Add(new OwLine { Text = "a" });
        post.Lines.Add(new OwLine { Text = "b" });
        await ctx.SaveChangesAsync();

        AssertMatches(new List<string> { "a", "b" }, keeper, "after clear+refill");
    }

    [Fact]
    public async Task Duplicate_texts_are_each_persisted()
    {
        using var keeper = NewKeeperWithSeed(Array.Empty<string>());
        await using var ctx = Make(keeper);
        var post = Load(ctx);

        post.Lines.Add(new OwLine { Text = "dup" });
        post.Lines.Add(new OwLine { Text = "dup" });
        post.Lines.Add(new OwLine { Text = "dup" });
        await ctx.SaveChangesAsync();

        AssertMatches(new List<string> { "dup", "dup", "dup" }, keeper, "after triple dup insert");
    }

    [Fact]
    public async Task Multiple_saves_reconcile_against_the_prior_persisted_state()
    {
        using var keeper = NewKeeperWithSeed(new[] { "a" });
        await using var ctx = Make(keeper);
        var post = Load(ctx);
        var model = new List<string> { "a" };

        post.Lines.Add(new OwLine { Text = "b" }); model.Add("b");
        await ctx.SaveChangesAsync();
        AssertMatches(model, keeper, "save 1 (add b)");

        post.Lines.RemoveAll(l => l.Text == "a"); model.Remove("a");
        await ctx.SaveChangesAsync();
        AssertMatches(model, keeper, "save 2 (remove a)");

        post.Lines.Add(new OwLine { Text = "c" }); model.Add("c");
        // A no-op-looking modify of the surviving row, to stress snapshot re-capture.
        post.Lines.Single(l => l.Text == "b").Text = "B2"; model.Remove("b"); model.Add("B2");
        await ctx.SaveChangesAsync();
        AssertMatches(model, keeper, "save 3 (add c, modify b->B2)");

        // A final save with no edits must not disturb the persisted set.
        await ctx.SaveChangesAsync();
        AssertMatches(model, keeper, "save 4 (no edits)");
    }

    // ── Coverage-guided sweep ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Owned_collection_edit_sweep_persists_exactly_the_model()
    {
        for (int trial = 0; trial < 200; trial++)
        {
            var rng = new Random(8000 + trial);
            int initCount = rng.Next(0, 4);
            var initial = Enumerable.Range(0, initCount).Select(i => $"seed-{i}").ToList();

            using var keeper = NewKeeperWithSeed(initial);
            await using var ctx = Make(keeper);
            var post = Load(ctx);
            var model = new List<string>(initial);

            int ops = rng.Next(4, 12);
            for (int o = 0; o < ops; o++)
            {
                switch (rng.Next(5))
                {
                    case 0: // add
                        var t = $"t{trial}-{o}";
                        post.Lines.Add(new OwLine { Text = t }); model.Add(t);
                        break;
                    case 1: // remove one
                        if (post.Lines.Count > 0)
                        {
                            int k = rng.Next(post.Lines.Count);
                            model.Remove(post.Lines[k].Text);
                            post.Lines.RemoveAt(k);
                        }
                        break;
                    case 2: // modify one
                        if (post.Lines.Count > 0)
                        {
                            int k = rng.Next(post.Lines.Count);
                            model.Remove(post.Lines[k].Text);
                            var nv = $"m{trial}-{o}";
                            post.Lines[k].Text = nv; model.Add(nv);
                        }
                        break;
                    case 3: // clear
                        post.Lines.Clear(); model.Clear();
                        break;
                    case 4: // save + verify
                        await ctx.SaveChangesAsync();
                        AssertMatches(model, keeper, $"trial {trial} op {o} (save)");
                        break;
                }
            }

            await ctx.SaveChangesAsync();
            AssertMatches(model, keeper, $"trial {trial} final save");
        }
    }
}
