using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Produces valid <see cref="M2mScenario"/> cases from a seed over a small fixed set of Posts and Tags.
    /// Validity invariants keep the snapshot-diff oracle unambiguous: a post is loaded before it is edited, a
    /// Link targets a pair not currently linked, and an Unlink targets a pair that is. Random interleavings
    /// naturally produce the bug-dense shapes — a link toggled within a batch, a re-link after an unlink, and
    /// multi-post edits across several saves. A seed is only metadata; the emitted scenario is the durable artifact.
    /// </summary>
    public static class M2mScenarioGenerator
    {
        private static readonly int[] PostIds = { 1, 2 };
        private static readonly int[] TagIds = { 1, 2, 3 };

        public static M2mScenario Generate(int seed)
        {
            var rng = new Random(seed);

            var model = new HashSet<(int Post, int Tag)>();
            foreach (var p in PostIds)
                foreach (var t in TagIds)
                    if (rng.Next(3) == 0) model.Add((p, t));
            var initial = model.Select(l => new M2mLink { PostId = l.Post, TagId = l.Tag }).ToList();

            var loaded = new HashSet<int>();
            var ops = new List<M2mOp>();
            var opCount = rng.Next(3, 14);

            for (var i = 0; i < opCount; i++)
            {
                var choice = rng.Next(10);

                if (choice < 3 || loaded.Count == 0)
                {
                    var unloaded = PostIds.Where(p => !loaded.Contains(p)).ToList();
                    if (unloaded.Count == 0) { ops.Add(M2mOp.Save()); continue; }
                    var p = unloaded[rng.Next(unloaded.Count)];
                    ops.Add(M2mOp.LoadPost(p));
                    loaded.Add(p);
                }
                else if (choice < 6)
                {
                    var candidates = (from p in loaded from t in TagIds where !model.Contains((p, t)) select (p, t)).ToList();
                    if (candidates.Count == 0) { ops.Add(M2mOp.Save()); continue; }
                    var (post, tag) = candidates[rng.Next(candidates.Count)];
                    ops.Add(M2mOp.Link(post, tag));
                    model.Add((post, tag));
                }
                else if (choice < 8)
                {
                    var candidates = model.Where(l => loaded.Contains(l.Post)).ToList();
                    if (candidates.Count == 0) { ops.Add(M2mOp.Save()); continue; }
                    var (post, tag) = candidates[rng.Next(candidates.Count)];
                    ops.Add(M2mOp.Unlink(post, tag));
                    model.Remove((post, tag));
                }
                else
                {
                    ops.Add(M2mOp.Save());
                }
            }

            return new M2mScenario { PostIds = PostIds, TagIds = TagIds, InitialLinks = initial, Ops = ops };
        }
    }
}
