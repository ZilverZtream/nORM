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
    /// The write-path snapshot-diff oracle: applies a <see cref="WriteScenario"/> to nORM and to an in-memory
    /// reference model, then compares the AUTHORITATIVE persisted state (read back through raw SQL, not the
    /// change tracker) to the model. The invariant is "committed database == reference model" — a lost,
    /// duplicated, or corrupted row is a <see cref="FuzzOutcome.WrongResult"/>. This is where the round-2
    /// data-loss lived, so it is the highest-value fuzzing direction.
    /// </summary>
    public static class WriteScenarioDifferential
    {
        public const string Family = "crud";
        public const int GeneratorVersion = 1;

        public static FuzzCaseResult Execute(WriteScenario scenario, long seed)
        {
            var serialized = scenario.ToJson();

            // Reference model: the intended committed state after applying every op in order.
            var model = new Dictionary<int, int>();

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var create = cn.CreateCommand())
            {
                create.CommandText = "CREATE TABLE CrudItem (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL)";
                create.ExecuteNonQuery();
            }

            try
            {
                using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
                var tracked = new Dictionary<int, CrudItem>();

                foreach (var op in scenario.Ops)
                {
                    switch (op.Kind)
                    {
                        case WriteOpKind.Insert:
                            model[op.Id] = op.Value;
                            var e = new CrudItem { Id = op.Id, Value = op.Value };
                            ctx.Add(e);
                            tracked[op.Id] = e;
                            break;
                        case WriteOpKind.Update:
                            model[op.Id] = op.Value;
                            tracked[op.Id].Value = op.Value;
                            break;
                        case WriteOpKind.Delete:
                            model.Remove(op.Id);
                            ctx.Remove(tracked[op.Id]);
                            tracked.Remove(op.Id);
                            break;
                        case WriteOpKind.Save:
                            ctx.SaveChangesAsync().GetAwaiter().GetResult();
                            break;
                    }
                }
                ctx.SaveChangesAsync().GetAwaiter().GetResult();   // final flush of any pending ops
            }
            catch (NormUnsupportedFeatureException nufe)
            {
                return new FuzzCaseResult
                {
                    Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                    Outcome = FuzzOutcome.UnexpectedlyRejected,
                    ReasonCode = "crud/" + nufe.GetType().Name,
                    SerializedCase = serialized, Detail = nufe.Message, Features = ExtractFeatures(scenario),
                };
            }
            catch (Exception ex)
            {
                return Fail(FuzzOutcome.UnexpectedException, $"{ex.GetType().Name}: {ex.Message}", scenario, seed, serialized);
            }

            // Read the AUTHORITATIVE persisted state via raw SQL (bypassing the change tracker's view).
            var db = new Dictionary<int, int>();
            using (var read = cn.CreateCommand())
            {
                read.CommandText = "SELECT Id, Value FROM CrudItem";
                using var r = read.ExecuteReader();
                while (r.Read())
                {
                    var id = r.GetInt32(0);
                    if (db.ContainsKey(id))
                        return Fail(FuzzOutcome.WrongResult, $"duplicate row for Id={id}", scenario, seed, serialized);
                    db[id] = r.GetInt32(1);
                }
            }

            if (!DictEqual(db, model))
                return Fail(FuzzOutcome.WrongResult, $"db={Render(db)} model={Render(model)}", scenario, seed, serialized);

            return new FuzzCaseResult
            {
                Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                Outcome = FuzzOutcome.Executed, SerializedCase = serialized, Features = ExtractFeatures(scenario),
            };
        }

        private static bool DictEqual(Dictionary<int, int> a, Dictionary<int, int> b)
            => a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

        private static string Render(Dictionary<int, int> d)
            => "{" + string.Join(",", d.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}")) + "}";

        public static IReadOnlyList<string> ExtractFeatures(WriteScenario s)
        {
            var f = new SortedSet<string>(StringComparer.Ordinal);
            var kinds = s.Ops.Select(o => o.Kind).ToHashSet();
            if (kinds.Contains(WriteOpKind.Insert)) f.Add("insert");
            if (kinds.Contains(WriteOpKind.Update)) f.Add("update");
            if (kinds.Contains(WriteOpKind.Delete)) f.Add("delete");
            if (s.Ops.Count(o => o.Kind == WriteOpKind.Save) >= 2) f.Add("multi-save");
            // An Insert and a Delete of the same id with no Save between = a net no-op batch.
            for (var i = 0; i < s.Ops.Count; i++)
            {
                if (s.Ops[i].Kind != WriteOpKind.Insert) continue;
                for (var j = i + 1; j < s.Ops.Count && s.Ops[j].Kind != WriteOpKind.Save; j++)
                    if (s.Ops[j].Kind == WriteOpKind.Delete && s.Ops[j].Id == s.Ops[i].Id) { f.Add("insert-then-delete-in-batch"); break; }
            }
            // Multiple pending inserts in one batch (batched INSERT).
            var pending = 0;
            foreach (var op in s.Ops)
            {
                if (op.Kind == WriteOpKind.Insert) { pending++; if (pending >= 2) f.Add("batched-insert"); }
                else if (op.Kind == WriteOpKind.Save) pending = 0;
            }
            return f.ToArray();
        }

        private static FuzzCaseResult Fail(FuzzOutcome outcome, string detail, WriteScenario s, long seed, string serialized) => new()
        {
            Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
            Outcome = outcome, SerializedCase = serialized, Detail = detail, Features = ExtractFeatures(s),
        };
    }
}
