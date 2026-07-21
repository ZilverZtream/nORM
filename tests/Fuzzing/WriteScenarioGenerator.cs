using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Produces valid <see cref="WriteScenario"/> cases from a seed: inserts use fresh monotonic ids (so no
    /// two tracked entities ever collide on a key), and updates/deletes only target ids that are currently
    /// live (inserted and not yet deleted). Saves are interleaved so both single-op and batched-op saves occur.
    /// A seed is only metadata; the emitted scenario is the durable artifact.
    /// </summary>
    public static class WriteScenarioGenerator
    {
        public static WriteScenario Generate(int seed)
        {
            var rng = new Random(seed);
            var live = new List<int>();
            var ops = new List<WriteOp>();
            var nextId = 1;
            var opCount = rng.Next(1, 16);

            for (var i = 0; i < opCount; i++)
            {
                var choice = rng.Next(10);
                if (choice < 4 || live.Count == 0)          // Insert a fresh id (biased, forced if nothing is live)
                {
                    var id = nextId++;
                    ops.Add(WriteOp.Insert(id, rng.Next(0, 100)));
                    live.Add(id);
                }
                else if (choice < 7)                        // Update a live id
                {
                    ops.Add(WriteOp.Update(live[rng.Next(live.Count)], rng.Next(0, 100)));
                }
                else if (choice < 9)                        // Delete a live id
                {
                    var idx = rng.Next(live.Count);
                    ops.Add(WriteOp.Delete(live[idx]));
                    live.RemoveAt(idx);
                }
                else                                        // Save boundary
                {
                    ops.Add(WriteOp.Save());
                }
            }

            return new WriteScenario { Ops = ops };
        }

        /// <summary>
        /// Produces valid CRUD scenarios that interleave CALLER-OWNED TRANSACTIONS — the interaction class the
        /// implicit-per-save families cannot reach. Invariants keep the snapshot-diff oracle's reference model
        /// well-defined: at most one open transaction; a Save flushes pending work before every BeginTx (so the
        /// begin snapshot equals the committed database); inside a transaction a live row may be updated or
        /// deleted but a row INSERTED in that same transaction is never updated (that is a deliberately loud
        /// error, not a silent one); and a Rollback is terminal (nORM keeps the rolled-back edits pending to
        /// re-apply on a later save, which a reverting reference model does not track). A transaction is closed
        /// with Commit or a terminal Rollback; multiple committed transactions may occur in one scenario.
        /// </summary>
        public static WriteScenario GenerateTransactional(int seed)
        {
            var rng = new Random(seed);
            var live = new List<int>();          // all existing ids (delete targets), mirrors the differential's tracked set
            var safe = new HashSet<int>();       // ids safe to UPDATE: inserted and saved (updatable in place, even inside a tx)
            var pendingInserts = new List<int>(); // inserted ids awaiting a save to become updatable
            var ops = new List<WriteOp>();
            var nextId = 1;
            var inTx = false;
            var opCount = rng.Next(4, 22);

            // A row inserted then saved is updatable, including inside a caller transaction: the save path emits
            // an in-place UPDATE for a modified transaction-inserted row (it stays Added so a rollback re-inserts
            // it with the current values). The generator updates only rows already saved, so the update is a
            // real UPDATE rather than an edit to a not-yet-inserted pending row.
            for (var i = 0; i < opCount; i++)
            {
                var nearEnd = i >= opCount - 2;

                if (inTx && (nearEnd || rng.Next(5) == 0))
                {
                    if (rng.Next(4) == 0) { ops.Add(WriteOp.Rollback()); inTx = false; break; }  // terminal rollback
                    ops.Add(WriteOp.Commit());
                    inTx = false;
                    continue;
                }

                if (!inTx && !nearEnd && live.Count > 0 && rng.Next(4) == 0)
                {
                    EmitSave();                    // flush pending so the begin snapshot equals the committed db
                    ops.Add(WriteOp.BeginTx());
                    inTx = true;
                    continue;
                }

                switch (rng.Next(10))
                {
                    case 0 or 1 or 2 or 3:             // insert a fresh id
                        InsertFresh();
                        break;
                    case 4 or 5 or 6:                  // update a safe, still-live id
                    {
                        var candidates = safe.Where(live.Contains).ToList();
                        if (candidates.Count == 0) { InsertFresh(); break; }
                        ops.Add(WriteOp.Update(candidates[rng.Next(candidates.Count)], rng.Next(0, 100)));
                        break;
                    }
                    case 7 or 8:                       // delete a live id
                    {
                        if (live.Count == 0) { InsertFresh(); break; }
                        var idx = rng.Next(live.Count);
                        var id = live[idx];
                        ops.Add(WriteOp.Delete(id));
                        live.RemoveAt(idx);
                        safe.Remove(id);
                        pendingInserts.Remove(id);
                        break;
                    }
                    default:                           // save boundary
                        EmitSave();
                        break;
                }
            }

            if (inTx)
                ops.Add(WriteOp.Commit());

            return new WriteScenario { Ops = ops };

            void InsertFresh()
            {
                var id = nextId++;
                ops.Add(WriteOp.Insert(id, rng.Next(0, 100)));
                live.Add(id);
                pendingInserts.Add(id);
            }

            void EmitSave()
            {
                ops.Add(WriteOp.Save());
                // Once saved, a row is persisted and updatable in place (even a transaction-inserted one).
                foreach (var id in pendingInserts) safe.Add(id);
                pendingInserts.Clear();
            }
        }
    }
}
