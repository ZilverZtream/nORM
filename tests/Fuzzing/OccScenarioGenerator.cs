using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Produces valid <see cref="OccScenario"/> cases from a seed, biased toward contention (several writers
    /// loading the same small set of rows before any commits). Validity invariants keep the oracle provably
    /// correct: only seeded ids are touched, a writer Updates only a row it has Loaded, and each writer Loads a
    /// given id at most once (so identity-map re-load semantics never arise). Saves are interleaved freely so the
    /// classic lost-update race — two writers load, one commits, the other's stale save must be rejected — arises
    /// naturally. A seed is only metadata; the emitted scenario is the durable artifact.
    /// </summary>
    public static class OccScenarioGenerator
    {
        public static OccScenario Generate(int seed)
        {
            var rng = new Random(seed);
            var writerCount = rng.Next(2, 4);                 // 2 or 3 writers
            var rowCount = rng.Next(1, 3);                    // 1 or 2 rows (fewer rows => more contention)
            var seededIds = Enumerable.Range(1, rowCount).ToList();

            var loaded = new Dictionary<int, HashSet<int>>();
            for (var w = 0; w < writerCount; w++) loaded[w] = new HashSet<int>();

            var ops = new List<OccOp>();
            var opCount = rng.Next(4, 18);

            for (var i = 0; i < opCount; i++)
            {
                var w = rng.Next(writerCount);
                var unloaded = seededIds.Where(id => !loaded[w].Contains(id)).ToList();
                var loadedIds = loaded[w].ToList();
                var choice = rng.Next(10);

                // Load a not-yet-loaded row (biased, and forced when this writer holds nothing to update).
                if ((choice < 4 && unloaded.Count > 0) || loadedIds.Count == 0)
                {
                    if (unloaded.Count == 0)
                    {
                        ops.Add(OccOp.Save(w));               // nothing to load or update: a save (usually a no-op)
                        continue;
                    }
                    var id = unloaded[rng.Next(unloaded.Count)];
                    ops.Add(OccOp.Load(w, id));
                    loaded[w].Add(id);
                }
                else if (choice < 8)                          // update a loaded row
                {
                    var id = loadedIds[rng.Next(loadedIds.Count)];
                    ops.Add(OccOp.Update(w, id, rng.Next(1, 1000)));
                }
                else                                          // save boundary
                {
                    ops.Add(OccOp.Save(w));
                }
            }

            return new OccScenario { WriterCount = writerCount, SeededIds = seededIds, Ops = ops };
        }
    }
}
