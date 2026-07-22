using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The optimistic-concurrency slice of the write-path snapshot-diff oracle. Several writers (each its own
    /// context over one shared SQLite database) interleave Load / Update / Save on rows carrying a
    /// <c>[Timestamp]</c> token. A reference model tracks, per writer, the token version observed at load and,
    /// per row, the authoritative committed version. Two invariants are checked against nORM:
    /// <list type="number">
    /// <item>a Save must throw <see cref="DbConcurrencyException"/> exactly when the model predicts a stale token —
    /// a save that should have thrown but succeeded is a silent lost update (<see cref="FuzzOutcome.WrongResult"/>);</item>
    /// <item>the authoritative persisted state (raw SQL, not the tracker) must equal the model's committed values.</item>
    /// </list>
    /// Scenarios are constrained so the oracle is provably correct: only seeded ids, Update only after Load, and each
    /// writer loads a given id at most once (so identity-map re-load semantics never arise).
    /// </summary>
    public static class OccScenarioDifferential
    {
        public const string Family = "occ";
        public const int GeneratorVersion = 1;

        private sealed class Writer
        {
            public required DbContext Ctx { get; init; }
            public readonly Dictionary<int, OccItem> Handles = new();   // rows this writer has loaded (tracked entity)
            public readonly Dictionary<int, int> LoadedVersion = new(); // model token version observed at load / last own save
            public readonly Dictionary<int, int> Pending = new();       // pending Update value not yet Saved
        }

        public static FuzzCaseResult Execute(OccScenario scenario, long seed)
        {
            var serialized = scenario.ToJson();

            // Authoritative reference model.
            var dbValue = scenario.SeededIds.ToDictionary(id => id, _ => 0);
            var dbVersion = scenario.SeededIds.ToDictionary(id => id, _ => 0);

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var create = cn.CreateCommand())
            {
                create.CommandText = "CREATE TABLE OccItem (Id INTEGER NOT NULL, Value INTEGER NOT NULL, Version BLOB NULL, CONSTRAINT PK_OccItem PRIMARY KEY (Id))";
                create.ExecuteNonQuery();
            }

            var writers = new List<Writer>();
            try
            {
                // Seed rows through nORM so each gets a properly initialized concurrency token.
                using (var seedCtx = NewCtx(cn))
                {
                    foreach (var id in scenario.SeededIds)
                        seedCtx.Add(new OccItem { Id = id, Value = 0 });
                    seedCtx.SaveChangesAsync().GetAwaiter().GetResult();
                }

                for (var w = 0; w < scenario.WriterCount; w++)
                    writers.Add(new Writer { Ctx = NewCtx(cn) });

                foreach (var op in scenario.Ops)
                {
                    var writer = writers[op.Writer];
                    switch (op.Kind)
                    {
                        case OccOpKind.Load:
                            if (!writer.Handles.ContainsKey(op.Id))
                            {
                                var loaded = writer.Ctx.Query<OccItem>().SingleAsync(r => r.Id == op.Id).GetAwaiter().GetResult();
                                writer.Handles[op.Id] = loaded;
                                writer.LoadedVersion[op.Id] = dbVersion[op.Id];
                            }
                            break;

                        case OccOpKind.Update:
                            writer.Handles[op.Id].Value = op.Value;
                            writer.Pending[op.Id] = op.Value;
                            break;

                        case OccOpKind.Save:
                            var conflictExpected = writer.Pending.Any(kv => writer.LoadedVersion[kv.Key] != dbVersion[kv.Key]);
                            bool threw = false;
                            try
                            {
                                writer.Ctx.SaveChangesAsync().GetAwaiter().GetResult();
                            }
                            catch (DbConcurrencyException)
                            {
                                threw = true;
                            }

                            if (conflictExpected && !threw)
                                return Fail(FuzzOutcome.WrongResult,
                                    $"W{op.Writer}.Save persisted a stale update (silent lost update); pending={Render(writer.Pending)}",
                                    scenario, seed, serialized);
                            if (!conflictExpected && threw)
                                return Fail(FuzzOutcome.WrongResult,
                                    $"W{op.Writer}.Save threw a false OCC conflict; pending={Render(writer.Pending)}",
                                    scenario, seed, serialized);

                            if (!threw)
                            {
                                // Commit succeeded: advance the model and refresh this writer's tokens.
                                foreach (var (id, value) in writer.Pending)
                                {
                                    dbValue[id] = value;
                                    dbVersion[id]++;
                                    writer.LoadedVersion[id] = dbVersion[id];
                                }
                                writer.Pending.Clear();
                            }
                            // On conflict the pending change stays Modified with its stale token (a later save re-throws),
                            // mirroring nORM; the model leaves it in place.
                            break;
                    }
                }
            }
            catch (NormUnsupportedFeatureException nufe)
            {
                return new FuzzCaseResult
                {
                    Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                    Outcome = FuzzOutcome.UnexpectedlyRejected,
                    ReasonCode = "occ/" + nufe.GetType().Name,
                    SerializedCase = serialized, Detail = nufe.Message, Features = ExtractFeatures(scenario),
                };
            }
            catch (Exception ex)
            {
                return Fail(FuzzOutcome.UnexpectedException, $"{ex.GetType().Name}: {ex.Message}", scenario, seed, serialized);
            }
            finally
            {
                foreach (var writer in writers)
                    writer.Ctx.Dispose();
            }

            // Compare the authoritative persisted state to the model.
            var persisted = new Dictionary<int, int>();
            using (var read = cn.CreateCommand())
            {
                read.CommandText = "SELECT Id, Value FROM OccItem";
                using var r = read.ExecuteReader();
                while (r.Read())
                    persisted[r.GetInt32(0)] = r.GetInt32(1);
            }

            if (!DictEqual(persisted, dbValue))
                return Fail(FuzzOutcome.WrongResult, $"db={Render(persisted)} model={Render(dbValue)}", scenario, seed, serialized);

            return new FuzzCaseResult
            {
                Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                Outcome = FuzzOutcome.Executed, SerializedCase = serialized, Features = ExtractFeatures(scenario),
            };
        }

        private static DbContext NewCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OccItem>().HasKey(x => x.Id)
        }, ownsConnection: false);

        private static bool DictEqual(Dictionary<int, int> a, Dictionary<int, int> b)
            => a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

        private static string Render(Dictionary<int, int> d)
            => "{" + string.Join(",", d.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}")) + "}";

        public static IReadOnlyList<string> ExtractFeatures(OccScenario s)
        {
            var f = new SortedSet<string>(StringComparer.Ordinal);
            f.Add("writers-" + s.WriterCount);
            if (s.Ops.Any(o => o.Kind == OccOpKind.Update)) f.Add("update");
            // A row loaded by >= 2 writers before either saves an update = a genuine concurrency contention.
            var loadedBy = new Dictionary<int, HashSet<int>>();
            var savedUpdate = new HashSet<(int writer, int id)>();
            foreach (var op in s.Ops)
            {
                if (op.Kind == OccOpKind.Load)
                {
                    if (!loadedBy.TryGetValue(op.Id, out var set)) loadedBy[op.Id] = set = new HashSet<int>();
                    set.Add(op.Writer);
                    if (set.Count >= 2) f.Add("contended-row");
                }
            }
            // Two writers each saving an update to the same row = the lost-update interleaving.
            var updatedThenSaved = new HashSet<int>();
            var pendingPerWriter = new Dictionary<int, HashSet<int>>();
            foreach (var op in s.Ops)
            {
                if (op.Kind == OccOpKind.Update)
                {
                    if (!pendingPerWriter.TryGetValue(op.Writer, out var p)) pendingPerWriter[op.Writer] = p = new HashSet<int>();
                    p.Add(op.Id);
                }
                else if (op.Kind == OccOpKind.Save && pendingPerWriter.TryGetValue(op.Writer, out var pend))
                {
                    foreach (var id in pend)
                    {
                        if (!updatedThenSaved.Add(id)) f.Add("competing-writes-same-row");
                    }
                    pend.Clear();
                }
            }
            return f.ToArray();
        }

        private static FuzzCaseResult Fail(FuzzOutcome outcome, string detail, OccScenario s, long seed, string serialized) => new()
        {
            Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
            Outcome = outcome, SerializedCase = serialized, Detail = detail, Features = ExtractFeatures(s),
        };
    }
}
