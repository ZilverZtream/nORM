using System;
using System.Collections.Generic;

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
    }
}
