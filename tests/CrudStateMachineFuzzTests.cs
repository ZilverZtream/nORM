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

        SqliteConnection Open()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return cn;
        }

        var rng = new Random(seed);
        var committed = new Dictionary<int, RowState>();
        var working = new Dictionary<int, RowState>();
        var tracked = new Dictionary<int, MutRow>();
        var deletedCommittedKeys = new List<int>();
        var nextKey = 1;

        var ctx = new DbContext(Open(), new SqliteProvider());
        try
        {
            for (var step = 0; step < 200; step++)
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
                        ctx = new DbContext(Open(), new SqliteProvider());
                        // Verify through a throwaway context so the MAIN context's
                        // tracking state stays empty — the next ops then exercise the
                        // detached Update / Remove paths on a coin flip.
                        using (var verifyCtx = new DbContext(Open(), new SqliteProvider()))
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

            ctx = new DbContext(Open(), new SqliteProvider());
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

        SqliteConnection Open()
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return cn;
        }

        var rng = new Random(seed);
        var committed = new Dictionary<int, RowState>();
        var working = new Dictionary<int, RowState>();
        var tracked = new Dictionary<int, MutGenRow>();
        var pendingAdds = new List<(MutGenRow Entity, RowState State)>();

        var ctx = new DbContext(Open(), new SqliteProvider());
        try
        {
            for (var step = 0; step < 200; step++)
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
                        ctx = new DbContext(Open(), new SqliteProvider());
                        using (var verifyCtx = new DbContext(Open(), new SqliteProvider()))
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

            ctx = new DbContext(Open(), new SqliteProvider());
            await VerifyGenAsync(ctx, committed, $"seed={seed} final (fresh context)");
        }
        finally
        {
            ctx.Dispose();
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
