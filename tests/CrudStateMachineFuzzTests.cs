using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Write-path state-machine fuzzer: drives seeded pseudo-random sequences of
/// Add / tracked-mutation / Remove / SaveChanges / context-discard against an
/// in-memory committed-state model. After every SaveChanges the full table
/// must equal the model exactly (every column), read both through the saving
/// context and — at the end and on every discard — through a FRESH context so
/// the identity map cannot mask what actually reached the database.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CrudStateMachineFuzzTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("MutRow_Test")]
    public class MutRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public bool Flag { get; set; }
    }

    private sealed record RowState(int IntVal, string Name, decimal Amount, bool Flag);

    private static readonly string[] NamePool = { "alpha", "ALPHA", "", "beta", "γ-x", "delta" };

    private static RowState RandomState(Random rng) => new(
        rng.Next(-100, 100),
        NamePool[rng.Next(NamePool.Length)],
        rng.Next(-2000, 2000) / 16m,
        rng.Next(2) == 0);

    /// <summary>
    /// Environment-directed seed sweep for building the release dry window: set
    /// NORM_CRUD_FUZZ_SWEEP to "start:count" to run that seed range through all three
    /// state machines. Unset, this fact is a no-op so the fixed seeds stay the baseline.
    /// </summary>
    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_CRUD_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
        {
            await Random_mutation_sequences_match_the_committed_model(s);
            await Random_mutation_sequences_with_generated_keys_match_the_committed_model(s);
            await Random_relationship_mutations_match_the_committed_model(s);
        }
    }

    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Random_mutation_sequences_match_the_committed_model(int seed)
    {
        var dbName = $"mutfuzz_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MutRow_Test (
                    Id INTEGER PRIMARY KEY,
                    IntVal INTEGER NOT NULL,
                    Name TEXT NOT NULL,
                    Amount TEXT NOT NULL,
                    Flag INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider());
        }

        await RunExplicitKeyMachineAsync(OpenCtx, seed, steps: 200);
    }

    /// <summary>Explicit-key machine body, shared with the live-provider variant.</summary>
    internal static async Task RunExplicitKeyMachineAsync(Func<DbContext> openCtx, int seed, int steps)
    {
        var rng = new Random(seed);
        var committed = new Dictionary<int, RowState>();
        var working = new Dictionary<int, RowState>();
        var tracked = new Dictionary<int, MutRow>();
        var deletedCommittedKeys = new List<int>();
        var nextKey = 1;

        var ctx = openCtx();
        try
        {
            for (var step = 0; step < steps; step++)
            {
                var op = rng.Next(12);
                switch (op)
                {
                    case 0 or 1 or 2: // add a new row
                    {
                        var key = nextKey++;
                        var state = RandomState(rng);
                        var entity = new MutRow { Id = key, IntVal = state.IntVal, Name = state.Name, Amount = state.Amount, Flag = state.Flag };
                        ctx.Add(entity);
                        tracked[key] = entity;
                        working[key] = state;
                        break;
                    }
                    case 3 or 4 or 5: // mutate: tracked instance, or a detached update of an untracked key
                    {
                        var untracked = working.Keys.Where(k => !tracked.ContainsKey(k)).ToList();
                        if (untracked.Count > 0 && rng.Next(2) == 0)
                        {
                            var key = untracked[rng.Next(untracked.Count)];
                            var state = RandomState(rng);
                            var entity = new MutRow { Id = key, IntVal = state.IntVal, Name = state.Name, Amount = state.Amount, Flag = state.Flag };
                            ctx.Update(entity);
                            tracked[key] = entity;
                            working[key] = state;
                        }
                        else if (tracked.Count > 0)
                        {
                            var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                            var state = RandomState(rng);
                            var entity = tracked[key];
                            entity.IntVal = state.IntVal;
                            entity.Name = state.Name;
                            entity.Amount = state.Amount;
                            entity.Flag = state.Flag;
                            working[key] = state;
                        }
                        break;
                    }
                    case 6: // remove: tracked instance, or a detached stub for an untracked key
                    {
                        var untracked = working.Keys.Where(k => !tracked.ContainsKey(k)).ToList();
                        if (untracked.Count > 0 && rng.Next(2) == 0)
                        {
                            var key = untracked[rng.Next(untracked.Count)];
                            ctx.Remove(new MutRow { Id = key });
                            working.Remove(key);
                            if (committed.ContainsKey(key))
                                deletedCommittedKeys.Add(key);
                        }
                        else if (tracked.Count > 0)
                        {
                            var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                            ctx.Remove(tracked[key]);
                            tracked.Remove(key);
                            working.Remove(key);
                            if (committed.ContainsKey(key))
                                deletedCommittedKeys.Add(key);
                        }
                        break;
                    }
                    case 7: // re-add a previously deleted committed key
                    {
                        if (deletedCommittedKeys.Count == 0) break;
                        var key = deletedCommittedKeys[rng.Next(deletedCommittedKeys.Count)];
                        if (working.ContainsKey(key)) break; // already re-added
                        var state = RandomState(rng);
                        var entity = new MutRow { Id = key, IntVal = state.IntVal, Name = state.Name, Amount = state.Amount, Flag = state.Flag };
                        ctx.Add(entity);
                        tracked[key] = entity;
                        working[key] = state;
                        break;
                    }
                    case 8 or 9 or 10: // save and verify through the same context
                    {
                        await ctx.SaveChangesAsync();
                        committed = new Dictionary<int, RowState>(working);
                        var verified = await VerifyAsync(ctx, committed, $"seed={seed} step={step} (same context after save)");
                        // The verify query tracked every row through the identity map;
                        // mirror that so the machine's tracked set matches reality.
                        foreach (var row in verified)
                            tracked[row.Id] = row;
                        break;
                    }
                    default: // discard: dispose without saving, reload through a fresh context
                    {
                        ctx.Dispose();
                        working = new Dictionary<int, RowState>(committed);
                        tracked.Clear();
                        deletedCommittedKeys.Clear();
                        ctx = openCtx();
                        // Verify through a throwaway context so the MAIN context's
                        // tracking state stays empty — the next ops then exercise the
                        // detached Update / Remove paths on a coin flip.
                        using (var verifyCtx = openCtx())
                            await VerifyAsync(verifyCtx, committed, $"seed={seed} step={step} (fresh context after discard)");
                        if (rng.Next(2) == 0)
                        {
                            foreach (var row in await ctx.Query<MutRow>().ToListAsync())
                                tracked[row.Id] = row;
                        }
                        break;
                    }
                }
            }

            await ctx.SaveChangesAsync();
            committed = new Dictionary<int, RowState>(working);
            ctx.Dispose();

            ctx = openCtx();
            await VerifyAsync(ctx, committed, $"seed={seed} final (fresh context)");
        }
        finally
        {
            ctx.Dispose();
        }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MutGen_Test")]
    public class MutGenRow
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int IntVal { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public bool Flag { get; set; }
    }

    /// <summary>
    /// DB-generated key variant: the oracle is key-agnostic — pending adds live
    /// keyed by instance until SaveChanges backfills their ids, which must come
    /// back positive, unique, and stable; values verify exactly under those ids.
    /// Pending adds share the unassigned default key (identity-map null-key
    /// handling), can be mutated before their insert, and can be removed again
    /// before saving (a net no-op).
    /// </summary>
    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    public async Task Random_mutation_sequences_with_generated_keys_match_the_committed_model(int seed)
    {
        var dbName = $"mutgenfuzz_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MutGen_Test (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    IntVal INTEGER NOT NULL,
                    Name TEXT NOT NULL,
                    Amount TEXT NOT NULL,
                    Flag INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider());
        }

        await RunGenKeyMachineAsync(OpenCtx, seed, steps: 200);
    }

    /// <summary>Generated-key machine body, shared with the live-provider variant.</summary>
    internal static async Task RunGenKeyMachineAsync(Func<DbContext> openCtx, int seed, int steps)
    {
        var rng = new Random(seed);
        var committed = new Dictionary<int, RowState>();
        var working = new Dictionary<int, RowState>();
        var tracked = new Dictionary<int, MutGenRow>();
        var pendingAdds = new List<(MutGenRow Entity, RowState State)>();

        var ctx = openCtx();
        try
        {
            for (var step = 0; step < steps; step++)
            {
                var op = rng.Next(12);
                switch (op)
                {
                    case 0 or 1 or 2: // add (key assigned by the database at save)
                    {
                        var state = RandomState(rng);
                        var entity = new MutGenRow { IntVal = state.IntVal, Name = state.Name, Amount = state.Amount, Flag = state.Flag };
                        ctx.Add(entity);
                        pendingAdds.Add((entity, state));
                        break;
                    }
                    case 3 or 4: // mutate a tracked committed row
                    {
                        if (tracked.Count == 0) break;
                        var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                        var state = RandomState(rng);
                        var entity = tracked[key];
                        entity.IntVal = state.IntVal;
                        entity.Name = state.Name;
                        entity.Amount = state.Amount;
                        entity.Flag = state.Flag;
                        working[key] = state;
                        break;
                    }
                    case 5: // mutate a pending add before its insert
                    {
                        if (pendingAdds.Count == 0) break;
                        var idx = rng.Next(pendingAdds.Count);
                        var state = RandomState(rng);
                        var entity = pendingAdds[idx].Entity;
                        entity.IntVal = state.IntVal;
                        entity.Name = state.Name;
                        entity.Amount = state.Amount;
                        entity.Flag = state.Flag;
                        pendingAdds[idx] = (entity, state);
                        break;
                    }
                    case 6: // remove a tracked committed row
                    {
                        if (tracked.Count == 0) break;
                        var key = tracked.Keys.ElementAt(rng.Next(tracked.Count));
                        ctx.Remove(tracked[key]);
                        tracked.Remove(key);
                        working.Remove(key);
                        break;
                    }
                    case 7: // remove a pending add again (net no-op)
                    {
                        if (pendingAdds.Count == 0) break;
                        var idx = rng.Next(pendingAdds.Count);
                        ctx.Remove(pendingAdds[idx].Entity);
                        pendingAdds.RemoveAt(idx);
                        break;
                    }
                    case 8 or 9 or 10: // save: backfilled keys must be positive, unique, and value-correct
                    {
                        await ctx.SaveChangesAsync();
                        foreach (var (entity, state) in pendingAdds)
                        {
                            Assert.True(entity.Id > 0,
                                $"generated key not backfilled seed={seed} step={step}: Id={entity.Id}");
                            Assert.True(!working.ContainsKey(entity.Id),
                                $"generated key collision seed={seed} step={step}: Id={entity.Id}");
                            working[entity.Id] = state;
                            tracked[entity.Id] = entity;
                        }
                        pendingAdds.Clear();
                        committed = new Dictionary<int, RowState>(working);
                        var verified = await VerifyGenAsync(ctx, committed, $"seed={seed} step={step} (same context after save)");
                        foreach (var row in verified)
                            tracked[row.Id] = row;
                        break;
                    }
                    default: // discard
                    {
                        ctx.Dispose();
                        pendingAdds.Clear();
                        working = new Dictionary<int, RowState>(committed);
                        tracked.Clear();
                        ctx = openCtx();
                        using (var verifyCtx = openCtx())
                            await VerifyGenAsync(verifyCtx, committed, $"seed={seed} step={step} (fresh context after discard)");
                        foreach (var row in await ctx.Query<MutGenRow>().ToListAsync())
                            tracked[row.Id] = row;
                        break;
                    }
                }
            }

            await ctx.SaveChangesAsync();
            foreach (var (entity, state) in pendingAdds)
            {
                Assert.True(entity.Id > 0, $"generated key not backfilled seed={seed} final: Id={entity.Id}");
                working[entity.Id] = state;
            }
            committed = new Dictionary<int, RowState>(working);
            ctx.Dispose();

            ctx = openCtx();
            await VerifyGenAsync(ctx, committed, $"seed={seed} final (fresh context)");
        }
        finally
        {
            ctx.Dispose();
        }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("RelParent_Test")]
    public class RelParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<RelChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("RelChild_Test")]
    public class RelChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public RelParent? Parent { get; set; }
    }

    /// <summary>
    /// Relationship variant: parents with child collections. Children arrive via
    /// graph adds (parent added with populated navigation), collection adds on a
    /// tracked parent (the FK comes from relationship fixup), and direct adds
    /// with an explicit FK; they move between parents through FK edits. After
    /// every save both tables verify exactly and every child's FK must point at
    /// an existing parent.
    /// </summary>
    [Theory]
    [InlineData(20260714)]
    [InlineData(42)]
    [InlineData(987654)]
    [InlineData(31337)]
    [InlineData(600023)]
    [InlineData(600332)]
    [InlineData(602775)]
    public async Task Random_relationship_mutations_match_the_committed_model(int seed)
    {
        var dbName = $"relfuzz_{seed}_{Guid.NewGuid():N}";
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RelParent_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE RelChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        DbContext OpenCtx()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider(), RelOptions());
        }

        await RunRelationshipMachineAsync(OpenCtx, seed, steps: 200);
    }

    /// <summary>Fluent relationship model shared by the SQLite and live variants.</summary>
    internal static nORM.Configuration.DbContextOptions RelOptions() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<RelParent>().HasKey(p => p.Id);
            mb.Entity<RelChild>().HasKey(c => c.Id);
            mb.Entity<RelParent>().HasMany(p => p.Children).WithOne()
                                  .HasForeignKey(c => c.ParentId, p => p.Id);
        }
    };

    /// <summary>Relationship machine body, shared with the live-provider variant.</summary>
    internal static async Task RunRelationshipMachineAsync(Func<DbContext> openCtx, int seed, int steps)
    {
        var rng = new Random(seed);
        var parents = new Dictionary<int, string>();          // working view
        var children = new Dictionary<int, (int ParentId, int Val)>();
        var committedParents = new Dictionary<int, string>();
        var committedChildren = new Dictionary<int, (int ParentId, int Val)>();
        var trackedParents = new Dictionary<int, RelParent>();
        var trackedChildren = new Dictionary<int, RelChild>();
        // Children living only in a parent's navigation until the next save: the
        // context discovers them through fixup, which sets their FK from the nav
        // parent (nav wins for new graph children). Until saved they are not
        // context-tracked, so FK edits target only tracked children, and undoing
        // a pending nav child means removing it from the parent's collection.
        var pendingNavChildren = new Dictionary<int, int>(); // childId -> parentId
        // Reference-navigation windows since the last save: FK edits and nav
        // retargets are mutually exclusive per child per window (the fixup
        // contract resolves conflicts by "deliberate FK edit outranks stale
        // nav", which the machine tests deliberately, not accidentally).
        var fkEdited = new HashSet<int>();
        var navRetargeted = new HashSet<int>();
        var pendingRefChildren = new HashSet<int>();
        var pendingPrincipals = new HashSet<int>();
        var nextParentKey = 1;
        var nextChildKey = 1;
        var trace = new List<string>();
        string Tail() => "\nops:\n" + string.Join("\n", trace);

        var ctx = openCtx();
        try
        {
            for (var step = 0; step < steps; step++)
            {
                switch (rng.Next(16))
                {
                    case 0: // add a childless parent
                    {
                        var key = nextParentKey++;
                        var name = NamePool[rng.Next(NamePool.Length)];
                        var parent = new RelParent { Id = key, Name = name };
                        ctx.Add(parent);
                        trackedParents[key] = parent;
                        parents[key] = name;
                        trace.Add($"{step}: add parent {key}");
                        break;
                    }
                    case 1 or 2: // graph add: parent with populated child navigation
                    {
                        var key = nextParentKey++;
                        var name = NamePool[rng.Next(NamePool.Length)];
                        var parent = new RelParent { Id = key, Name = name };
                        var kidCount = rng.Next(1, 4);
                        var kids = new List<int>();
                        for (var k = 0; k < kidCount; k++)
                        {
                            var ck = nextChildKey++;
                            var val = rng.Next(-50, 50);
                            var child = new RelChild { Id = ck, Val = val }; // FK comes from fixup
                            parent.Children.Add(child);
                            trackedChildren[ck] = child;
                            pendingNavChildren[ck] = key;
                            children[ck] = (key, val);
                            kids.Add(ck);
                        }
                        ctx.Add(parent);
                        trackedParents[key] = parent;
                        parents[key] = name;
                        trace.Add($"{step}: graph add parent {key} kids [{string.Join(",", kids)}]");
                        break;
                    }
                    case 3 or 4: // collection add on a tracked parent
                    {
                        if (trackedParents.Count == 0) break;
                        var pk = trackedParents.Keys.ElementAt(rng.Next(trackedParents.Count));
                        var ck = nextChildKey++;
                        var val = rng.Next(-50, 50);
                        var child = new RelChild { Id = ck, Val = val }; // FK comes from fixup
                        trackedParents[pk].Children.Add(child);
                        trackedChildren[ck] = child;
                        pendingNavChildren[ck] = pk;
                        children[ck] = (pk, val);
                        trace.Add($"{step}: nav add child {ck} -> parent {pk}");
                        break;
                    }
                    case 5: // direct child add with an explicit FK
                    {
                        if (parents.Count == 0) break;
                        var pk = parents.Keys.ElementAt(rng.Next(parents.Count));
                        var ck = nextChildKey++;
                        var val = rng.Next(-50, 50);
                        var child = new RelChild { Id = ck, ParentId = pk, Val = val };
                        ctx.Add(child);
                        trackedChildren[ck] = child;
                        children[ck] = (pk, val);
                        trace.Add($"{step}: direct add child {ck} -> parent {pk}");
                        break;
                    }
                    case 6: // FK edit: move a CONTEXT-tracked child to another parent.
                            // A stale Parent navigation may still point at the old
                            // principal — the deliberately edited FK must outrank it.
                    {
                        if (parents.Count == 0) break;
                        var editable = trackedChildren.Keys
                            .Where(ck => children.ContainsKey(ck)
                                && !pendingNavChildren.ContainsKey(ck)
                                && !pendingRefChildren.Contains(ck)
                                && !navRetargeted.Contains(ck))
                            .ToList();
                        if (editable.Count == 0) break;
                        var ckEdit = editable[rng.Next(editable.Count)];
                        var pk = parents.Keys.ElementAt(rng.Next(parents.Count));
                        trackedChildren[ckEdit].ParentId = pk;
                        children[ckEdit] = (pk, children[ckEdit].Val);
                        fkEdited.Add(ckEdit);
                        trace.Add($"{step}: fk edit child {ckEdit} -> parent {pk}");
                        break;
                    }
                    case 7: // delete a child: context-tracked via Remove, pending nav via collection removal
                    {
                        if (trackedChildren.Count == 0) break;
                        var ck = trackedChildren.Keys.ElementAt(rng.Next(trackedChildren.Count));
                        if (!children.ContainsKey(ck)) break;
                        // A pending ref-nav child carries its principal's discovery;
                        // removing it pre-save would strand the modelled principal.
                        if (pendingRefChildren.Contains(ck)) break;
                        if (pendingNavChildren.TryGetValue(ck, out var navParent))
                        {
                            if (trackedParents.TryGetValue(navParent, out var parent))
                                parent.Children.Remove(trackedChildren[ck]);
                            pendingNavChildren.Remove(ck);
                            trace.Add($"{step}: nav-remove pending child {ck} from parent {navParent}");
                        }
                        else
                        {
                            ctx.Remove(trackedChildren[ck]);
                            trace.Add($"{step}: remove child {ck}");
                        }
                        trackedChildren.Remove(ck);
                        children.Remove(ck);
                        break;
                    }
                    case 8: // delete a childless tracked parent
                    {
                        var childless = trackedParents.Keys
                            .Where(pk => parents.ContainsKey(pk)
                                && !pendingPrincipals.Contains(pk)
                                && !children.Values.Any(c => c.ParentId == pk))
                            .ToList();
                        if (childless.Count == 0) break;
                        var pk = childless[rng.Next(childless.Count)];
                        ctx.Remove(trackedParents[pk]);
                        trackedParents.Remove(pk);
                        parents.Remove(pk);
                        trace.Add($"{step}: remove parent {pk}");
                        break;
                    }
                    case 9 or 10: // save and verify both tables plus FK integrity
                    {
                        await ctx.SaveChangesAsync();
                        pendingNavChildren.Clear();
                        fkEdited.Clear();
                        navRetargeted.Clear();
                        pendingRefChildren.Clear();
                        pendingPrincipals.Clear();
                        committedParents = new Dictionary<int, string>(parents);
                        committedChildren = new Dictionary<int, (int, int)>(children);
                        trace.Add($"{step}: save");
                        // Verify through a throwaway context: the main context's
                        // identity map would return tracked instances (in-memory
                        // values), not what actually reached the database.
                        using (var verifyCtx = openCtx())
                            await VerifyRelAsync(verifyCtx, committedParents, committedChildren, $"seed={seed} step={step} (after save){Tail()}");
                        break;
                    }
                    case 11: // discard
                    {
                        ctx.Dispose();
                        parents = new Dictionary<int, string>(committedParents);
                        children = new Dictionary<int, (int, int)>(committedChildren);
                        trackedParents.Clear();
                        trackedChildren.Clear();
                        pendingNavChildren.Clear();
                        fkEdited.Clear();
                        navRetargeted.Clear();
                        pendingRefChildren.Clear();
                        pendingPrincipals.Clear();
                        ctx = openCtx();
                        trace.Add($"{step}: discard");
                        await VerifyRelAsync(ctx, committedParents, committedChildren, $"seed={seed} step={step} (fresh context after discard){Tail()}");
                        foreach (var p in await ctx.Query<RelParent>().ToListAsync())
                            trackedParents[p.Id] = p;
                        foreach (var c in await ctx.Query<RelChild>().ToListAsync())
                            trackedChildren[c.Id] = c;
                        break;
                    }
                    case 12: // reference-nav add: child linked via Parent to a tracked principal
                    {
                        var candidates = trackedParents.Keys.Where(pk => parents.ContainsKey(pk)).ToList();
                        if (candidates.Count == 0) break;
                        var pk = candidates[rng.Next(candidates.Count)];
                        var ck = nextChildKey++;
                        var val = rng.Next(-50, 50);
                        var child = new RelChild { Id = ck, Val = val, Parent = trackedParents[pk] };
                        ctx.Add(child);
                        trackedChildren[ck] = child;
                        pendingRefChildren.Add(ck);
                        children[ck] = (pk, val);
                        trace.Add($"{step}: refnav add child {ck} -> parent {pk}");
                        break;
                    }
                    case 13: // nav retarget: point a saved child's Parent at another principal
                             // (FK scalar is stale-unchanged, so the navigation drives the move)
                    {
                        var candidates = trackedChildren.Keys
                            .Where(ck => children.ContainsKey(ck)
                                && !pendingNavChildren.ContainsKey(ck)
                                && !pendingRefChildren.Contains(ck)
                                && !fkEdited.Contains(ck))
                            .ToList();
                        var targets = trackedParents.Keys
                            .Where(pk => parents.ContainsKey(pk) && !pendingPrincipals.Contains(pk))
                            .ToList();
                        if (candidates.Count == 0 || targets.Count == 0) break;
                        var ck = candidates[rng.Next(candidates.Count)];
                        var pk = targets[rng.Next(targets.Count)];
                        trackedChildren[ck].Parent = trackedParents[pk];
                        children[ck] = (pk, children[ck].Val);
                        navRetargeted.Add(ck);
                        trace.Add($"{step}: nav retarget child {ck} -> parent {pk}");
                        break;
                    }
                    case 14: // reference-nav add with an UNTRACKED new principal (graph discovery)
                    {
                        var pk = nextParentKey++;
                        var name = NamePool[rng.Next(NamePool.Length)];
                        var principal = new RelParent { Id = pk, Name = name }; // never ctx.Add'ed
                        var ck = nextChildKey++;
                        var val = rng.Next(-50, 50);
                        var child = new RelChild { Id = ck, Val = val, Parent = principal };
                        ctx.Add(child);
                        trackedParents[pk] = principal;
                        trackedChildren[ck] = child;
                        pendingPrincipals.Add(pk);
                        pendingRefChildren.Add(ck);
                        parents[pk] = name;
                        children[ck] = (pk, val);
                        trace.Add($"{step}: refnav add child {ck} -> NEW principal {pk}");
                        break;
                    }
                    default: // cascade delete: remove a parent whose children are all
                             // context-visible; tracked children delete with it, pending
                             // nav children die undiscovered (fixup skips deleted navs)
                    {
                        var eligible = trackedParents.Keys
                            .Where(pk => parents.ContainsKey(pk)
                                && !pendingPrincipals.Contains(pk)
                                && children.All(kv => kv.Value.ParentId != pk
                                    || trackedChildren.ContainsKey(kv.Key)
                                    || pendingNavChildren.TryGetValue(kv.Key, out var np) && np == pk))
                            .ToList();
                        if (eligible.Count == 0) break;
                        var pkDel = eligible[rng.Next(eligible.Count)];
                        ctx.Remove(trackedParents[pkDel]);
                        var doomed = children.Where(kv => kv.Value.ParentId == pkDel).Select(kv => kv.Key).ToList();
                        foreach (var ck in doomed)
                        {
                            children.Remove(ck);
                            trackedChildren.Remove(ck);
                            pendingNavChildren.Remove(ck);
                            pendingRefChildren.Remove(ck);
                            fkEdited.Remove(ck);
                            navRetargeted.Remove(ck);
                        }
                        trackedParents.Remove(pkDel);
                        parents.Remove(pkDel);
                        trace.Add($"{step}: cascade delete parent {pkDel} (children [{string.Join(",", doomed)}])");
                        break;
                    }
                }
            }

            await ctx.SaveChangesAsync();
            committedParents = new Dictionary<int, string>(parents);
            committedChildren = new Dictionary<int, (int, int)>(children);
            ctx.Dispose();

            ctx = openCtx();
            await VerifyRelAsync(ctx, committedParents, committedChildren, $"seed={seed} final (fresh context){Tail()}");
        }
        finally
        {
            ctx.Dispose();
        }
    }

    private static async Task VerifyRelAsync(
        DbContext ctx,
        Dictionary<int, string> expectedParents,
        Dictionary<int, (int ParentId, int Val)> expectedChildren,
        string context)
    {
        var parents = (await ctx.Query<RelParent>().ToListAsync()).OrderBy(p => p.Id).ToList();
        var children = (await ctx.Query<RelChild>().ToListAsync()).OrderBy(c => c.Id).ToList();

        Assert.True(parents.Count == expectedParents.Count,
            $"parent count mismatch {context}: db={parents.Count} model={expectedParents.Count}\n" +
            $"db: [{string.Join(",", parents.Select(p => p.Id))}]\nmodel: [{string.Join(",", expectedParents.Keys.OrderBy(k => k))}]");
        foreach (var p in parents)
        {
            Assert.True(expectedParents.TryGetValue(p.Id, out var name) && name == p.Name,
                $"parent mismatch {context} at Id={p.Id}: db Name=\"{p.Name}\" model=\"{(expectedParents.TryGetValue(p.Id, out var n) ? n : "<absent>")}\"");
        }

        Assert.True(children.Count == expectedChildren.Count,
            $"child count mismatch {context}: db={children.Count} model={expectedChildren.Count}\n" +
            $"db: [{string.Join(",", children.Select(c => c.Id))}]\nmodel: [{string.Join(",", expectedChildren.Keys.OrderBy(k => k))}]");
        foreach (var c in children)
        {
            Assert.True(expectedChildren.TryGetValue(c.Id, out var state) && state.ParentId == c.ParentId && state.Val == c.Val,
                $"child mismatch {context} at Id={c.Id}: db=({c.ParentId},{c.Val}) model={(expectedChildren.TryGetValue(c.Id, out var s) ? $"({s.ParentId},{s.Val})" : "<absent>")}");
            Assert.True(expectedParents.ContainsKey(c.ParentId),
                $"orphaned child {context}: child {c.Id} points at missing parent {c.ParentId}");
        }
    }

    private static async Task<List<MutGenRow>> VerifyGenAsync(DbContext ctx, Dictionary<int, RowState> expected, string context)
    {
        var rows = (await ctx.Query<MutGenRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
        var expectedRows = expected.OrderBy(kv => kv.Key).ToList();

        Assert.True(rows.Count == expectedRows.Count,
            $"row count mismatch {context}: db={rows.Count} model={expectedRows.Count}\n" +
            $"db keys: [{string.Join(",", rows.Select(r => r.Id))}]\nmodel keys: [{string.Join(",", expectedRows.Select(kv => kv.Key))}]");

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var (key, state) = expectedRows[i];
            Assert.True(row.Id == key
                    && row.IntVal == state.IntVal
                    && row.Name == state.Name
                    && row.Amount == state.Amount
                    && row.Flag == state.Flag,
                $"row mismatch {context} at Id={key}:\n" +
                $"db:    ({row.Id}, {row.IntVal}, \"{row.Name}\", {row.Amount}, {row.Flag})\n" +
                $"model: ({key}, {state.IntVal}, \"{state.Name}\", {state.Amount}, {state.Flag})");
        }

        return rows;
    }

    private static async Task<List<MutRow>> VerifyAsync(DbContext ctx, Dictionary<int, RowState> expected, string context)
    {
        var rows = (await ctx.Query<MutRow>().ToListAsync()).OrderBy(r => r.Id).ToList();
        var expectedRows = expected.OrderBy(kv => kv.Key).ToList();

        Assert.True(rows.Count == expectedRows.Count,
            $"row count mismatch {context}: db={rows.Count} model={expectedRows.Count}\n" +
            $"db keys: [{string.Join(",", rows.Select(r => r.Id))}]\nmodel keys: [{string.Join(",", expectedRows.Select(kv => kv.Key))}]");

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var (key, state) = expectedRows[i];
            Assert.True(row.Id == key
                    && row.IntVal == state.IntVal
                    && row.Name == state.Name
                    && row.Amount == state.Amount
                    && row.Flag == state.Flag,
                $"row mismatch {context} at Id={key}:\n" +
                $"db:    ({row.Id}, {row.IntVal}, \"{row.Name}\", {row.Amount}, {row.Flag})\n" +
                $"model: ({key}, {state.IntVal}, \"{state.Name}\", {state.Amount}, {state.Flag})");
        }

        return rows;
    }
}
