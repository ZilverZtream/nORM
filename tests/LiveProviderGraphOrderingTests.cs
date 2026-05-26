using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Proves that SaveChanges topological-sort ordering holds under real FK-constraint
/// enforcement across all 4 providers.
///
/// Why this matters: the in-memory SQLite CascadeDeleteTests run without
/// PRAGMA foreign_keys = ON, so they never exercise real FK enforcement. Without
/// enforcement the DB accepts any insert/delete order, masking ordering bugs.
/// These tests enable FK enforcement at the connection level so that a wrong
/// insert or delete order produces an immediate FK-violation error.
///
/// Silent-wrongness risk: if the topological sort puts dependents before
/// principals during INSERT, or principals before dependents during DELETE,
/// the test throws — not just asserts. A wrong result value can hide silently;
/// a FK violation cannot.
///
/// Schema: GpoParent (Id PK) ← GpoChild (Id PK, GpoParentId FK)
///   The property name "GpoParentId" triggers nORM's convention detector:
///   strip "Id" → "GpoParent" → matched by simple-name lookup against GpoParent.
///   No [ForeignKey] attribute or fluent config needed.
///
/// Three-level chain: GpoGp (grandparent) ← GpoMid (midparent) ← GpoLeaf
///   Same convention: GpoGpId → "GpoGp"; GpoMidId → "GpoMid"
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderGraphOrderingTests
{
    // ── Entity model ─────────────────────────────────────────────────────────

    [Table("GpoParent")]
    private sealed class GpoParent
    {
        [Key] public int Id   { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("GpoChild")]
    private sealed class GpoChild
    {
        [Key] public int Id          { get; set; }
        public int    GpoParentId { get; set; } // convention: strip "Id" → "GpoParent"
        public string Tag         { get; set; } = "";
    }

    [Table("GpoGp")]
    private sealed class GpoGp   { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("GpoMid")]
    private sealed class GpoMid  { [Key] public int Id { get; set; } public int GpoGpId  { get; set; } public string Name { get; set; } = ""; }

    [Table("GpoLeaf")]
    private sealed class GpoLeaf { [Key] public int Id { get; set; } public int GpoMidId { get; set; } public string Tag  { get; set; } = ""; }

    // ── DDL helpers ───────────────────────────────────────────────────────────

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnableFkEnforcementAsync(DbContext ctx, ProviderKind kind)
    {
        if (kind == ProviderKind.Sqlite)
            await ExecuteAsync(ctx, "PRAGMA foreign_keys = ON");
    }

    private static string IdCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer ? $"NVARCHAR({len})" : $"VARCHAR({len})";

    private static string DropIfExists(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{esc}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupTwoLevelAsync(DbContext ctx, ProviderKind kind)
    {
        var ep = ctx.Provider.Escape("GpoParent");
        var ec = ctx.Provider.Escape("GpoChild");
        var intT = IdCol(kind);
        var varT = VarCol(kind, 50);
        var engine = kind == ProviderKind.MySql ? " ENGINE=InnoDB" : "";

        // Drop child before parent (FK dependency).
        await ExecuteAsync(ctx, DropIfExists(kind, ec));
        await ExecuteAsync(ctx, DropIfExists(kind, ep));

        await ExecuteAsync(ctx, $"CREATE TABLE {ep} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, {ctx.Provider.Escape("Name")} {varT} NOT NULL){engine}");
        await ExecuteAsync(ctx, $"CREATE TABLE {ec} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, {ctx.Provider.Escape("GpoParentId")} {intT} NOT NULL, {ctx.Provider.Escape("Tag")} {varT} NOT NULL, FOREIGN KEY ({ctx.Provider.Escape("GpoParentId")}) REFERENCES {ep}({ctx.Provider.Escape("Id")})){engine}");
    }

    private static async Task TeardownTwoLevelAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropIfExists(kind, ctx.Provider.Escape("GpoChild")));
            await ExecuteAsync(ctx, DropIfExists(kind, ctx.Provider.Escape("GpoParent")));
        }
        catch { /* best-effort */ }
    }

    private static async Task SetupThreeLevelAsync(DbContext ctx, ProviderKind kind)
    {
        var egp  = ctx.Provider.Escape("GpoGp");
        var emid = ctx.Provider.Escape("GpoMid");
        var elea = ctx.Provider.Escape("GpoLeaf");
        var intT = IdCol(kind);
        var varT = VarCol(kind, 50);
        var engine = kind == ProviderKind.MySql ? " ENGINE=InnoDB" : "";

        await ExecuteAsync(ctx, DropIfExists(kind, elea));
        await ExecuteAsync(ctx, DropIfExists(kind, emid));
        await ExecuteAsync(ctx, DropIfExists(kind, egp));

        await ExecuteAsync(ctx, $"CREATE TABLE {egp} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, {ctx.Provider.Escape("Name")} {varT} NOT NULL){engine}");
        await ExecuteAsync(ctx, $"CREATE TABLE {emid} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, {ctx.Provider.Escape("GpoGpId")} {intT} NOT NULL, {ctx.Provider.Escape("Name")} {varT} NOT NULL, FOREIGN KEY ({ctx.Provider.Escape("GpoGpId")}) REFERENCES {egp}({ctx.Provider.Escape("Id")})){engine}");
        await ExecuteAsync(ctx, $"CREATE TABLE {elea} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, {ctx.Provider.Escape("GpoMidId")} {intT} NOT NULL, {ctx.Provider.Escape("Tag")} {varT} NOT NULL, FOREIGN KEY ({ctx.Provider.Escape("GpoMidId")}) REFERENCES {emid}({ctx.Provider.Escape("Id")})){engine}");
    }

    private static async Task TeardownThreeLevelAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropIfExists(kind, ctx.Provider.Escape("GpoLeaf")));
            await ExecuteAsync(ctx, DropIfExists(kind, ctx.Provider.Escape("GpoMid")));
            await ExecuteAsync(ctx, DropIfExists(kind, ctx.Provider.Escape("GpoGp")));
        }
        catch { /* best-effort */ }
    }

    // ── 1: Insert parent + child in one SaveChanges — parent goes first ───────
    //
    // Silent-wrongness: if GpoChild is inserted before GpoParent, the DB raises
    // a FK violation. This test fails loudly on wrong ordering.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Insert_parent_and_child_in_one_SaveChanges_respects_FK_constraint_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupTwoLevelAsync(ctx, kind);
            await EnableFkEnforcementAsync(ctx, kind);
            try
            {
                var parent = new GpoParent { Id = 1, Name = "P1" };
                var child  = new GpoChild  { Id = 10, GpoParentId = 1, Tag = "C1" };

                // Add in reverse order to stress the sort.
                ctx.Add(child);
                ctx.Add(parent);
                await ctx.SaveChangesAsync(); // parent must be inserted first

                var pRow = await ctx.Query<GpoParent>().Where(r => r.Id == 1).FirstAsync();
                var cRow = await ctx.Query<GpoChild>().Where(r => r.Id == 10).FirstAsync();
                Assert.Equal("P1", pRow.Name);
                Assert.Equal(1, cRow.GpoParentId);
                Assert.Equal("C1", cRow.Tag);
            }
            finally { await TeardownTwoLevelAsync(ctx, kind); }
        }
    }

    // ── 2: Delete child + parent in one SaveChanges — child goes first ────────
    //
    // Without cascade, deleting the parent while the child still holds a FK
    // reference raises a FK violation. The topological sort must reverse to
    // delete dependents before principals.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Delete_parent_and_child_in_one_SaveChanges_respects_FK_constraint_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupTwoLevelAsync(ctx, kind);
            await EnableFkEnforcementAsync(ctx, kind);
            try
            {
                // Insert using nORM (correct order) so both are tracked.
                var parent = new GpoParent { Id = 2, Name = "P2" };
                var child  = new GpoChild  { Id = 20, GpoParentId = 2, Tag = "C2" };
                ctx.Add(parent);
                ctx.Add(child);
                await ctx.SaveChangesAsync();

                // Remove both — child must be deleted first.
                ctx.Remove(parent);
                ctx.Remove(child);
                await ctx.SaveChangesAsync(); // child must be deleted first

                var remaining = await ctx.Query<GpoParent>().Where(r => r.Id == 2).ToListAsync();
                Assert.Empty(remaining);
            }
            finally { await TeardownTwoLevelAsync(ctx, kind); }
        }
    }

    // ── 3: Three-level chain insert in one SaveChanges ────────────────────────
    //
    // GpoGp → GpoMid → GpoLeaf. All three added in reverse order;
    // the sort must produce GpoGp, GpoMid, GpoLeaf.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Three_level_chain_insert_in_one_SaveChanges_respects_FK_ordering_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupThreeLevelAsync(ctx, kind);
            await EnableFkEnforcementAsync(ctx, kind);
            try
            {
                var gp   = new GpoGp   { Id = 1, Name = "GP1" };
                var mid  = new GpoMid  { Id = 10, GpoGpId  = 1,  Name = "M1" };
                var leaf = new GpoLeaf { Id = 100, GpoMidId = 10, Tag  = "L1" };

                // Add in leaf-first order (worst case for sort).
                ctx.Add(leaf);
                ctx.Add(mid);
                ctx.Add(gp);
                await ctx.SaveChangesAsync(); // must insert in GpoGp → GpoMid → GpoLeaf order

                var gpRow   = await ctx.Query<GpoGp>()  .Where(r => r.Id == 1).FirstAsync();
                var midRow  = await ctx.Query<GpoMid>() .Where(r => r.Id == 10).FirstAsync();
                var leafRow = await ctx.Query<GpoLeaf>().Where(r => r.Id == 100).FirstAsync();
                Assert.Equal("GP1", gpRow.Name);
                Assert.Equal(1,   midRow.GpoGpId);
                Assert.Equal(10,  leafRow.GpoMidId);
            }
            finally { await TeardownThreeLevelAsync(ctx, kind); }
        }
    }

    // ── 4: Three-level chain delete in one SaveChanges ────────────────────────
    //
    // Delete all three in principal-first order (worst case); the reversed
    // topological sort must reorder to leaf → mid → gp.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Three_level_chain_delete_in_one_SaveChanges_respects_FK_ordering_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupThreeLevelAsync(ctx, kind);
            await EnableFkEnforcementAsync(ctx, kind);
            try
            {
                var gp   = new GpoGp   { Id = 2, Name = "GP2" };
                var mid  = new GpoMid  { Id = 20, GpoGpId  = 2,  Name = "M2" };
                var leaf = new GpoLeaf { Id = 200, GpoMidId = 20, Tag  = "L2" };
                ctx.Add(gp); ctx.Add(mid); ctx.Add(leaf);
                await ctx.SaveChangesAsync();

                // Remove in principal-first order (worst case).
                ctx.Remove(gp);
                ctx.Remove(mid);
                ctx.Remove(leaf);
                await ctx.SaveChangesAsync(); // must delete leaf → mid → gp

                var remaining = await ctx.Query<GpoGp>().Where(r => r.Id == 2).ToListAsync();
                Assert.Empty(remaining);
            }
            finally { await TeardownThreeLevelAsync(ctx, kind); }
        }
    }

    // ── 5: Multiple parents, multiple children in one SaveChanges ─────────────
    //
    // Two parents and two children each (four children total). All six added
    // in interleaved order. Sorting must place both parents before any child.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Multiple_parents_and_children_added_interleaved_all_inserted_in_one_SaveChanges_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupTwoLevelAsync(ctx, kind);
            await EnableFkEnforcementAsync(ctx, kind);
            try
            {
                // Interleave: child1, parent1, child2, parent2, child3, child4
                ctx.Add(new GpoChild  { Id = 31, GpoParentId = 3, Tag = "A" });
                ctx.Add(new GpoParent { Id = 3,  Name = "P3" });
                ctx.Add(new GpoChild  { Id = 32, GpoParentId = 3, Tag = "B" });
                ctx.Add(new GpoParent { Id = 4,  Name = "P4" });
                ctx.Add(new GpoChild  { Id = 41, GpoParentId = 4, Tag = "C" });
                ctx.Add(new GpoChild  { Id = 42, GpoParentId = 4, Tag = "D" });
                await ctx.SaveChangesAsync(); // parents must precede all children

                var childCount = await ctx.Query<GpoChild>()
                    .Where(r => r.GpoParentId == 3 || r.GpoParentId == 4)
                    .CountAsync();
                Assert.Equal(4, childCount);
            }
            finally { await TeardownTwoLevelAsync(ctx, kind); }
        }
    }
}
