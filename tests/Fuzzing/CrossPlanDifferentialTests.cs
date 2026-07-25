using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using Xunit;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Cross-plan metamorphic differential: the SAME generated query, executed through nORM's DIFFERENT execution
    /// paths, must return the same rows. This closes a real blind spot — the flagship
    /// <see cref="QueryIrDifferential"/> runs nORM only via the synchronous general-translator path
    /// (<c>q.ToList()</c>), but the read FAST PATH is async-only (<c>q.ToListAsync()</c>). Several past
    /// silent-wrong bugs (the decimal fast-path range/order bug, First(predicate), chained-paging) slipped the
    /// single-path fuzzer for exactly this reason. Re-running the proven corpus through sync (general) vs async
    /// (fast path) needs NO external oracle: if two nORM paths disagree on the same query + data, one is wrong.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class CrossPlanDifferentialTests
    {
        // Canonical per-row key; sorted comparison detects wrong/missing/extra rows regardless of return order
        // (both terminals hit the same in-memory DB, so a divergence is a real cross-plan defect, not ordering noise).
        private static string Key(IrRow r) => $"{r.Id}|{r.A}|{r.B}|{r.Name}|{(r.N.HasValue ? r.N.ToString() : "null")}";

        private readonly record struct Run(bool Threw, string? Error, List<string> Keys);

        private static Run RunSync(DbContext ctx, QueryIr ir)
        {
            try
            {
                var rows = QueryIrDifferential.BuildNormQueryable(ctx, ir).ToList();
                return new Run(false, null, rows.Select(Key).OrderBy(x => x, StringComparer.Ordinal).ToList());
            }
            catch (NormUnsupportedFeatureException nufe) { return new Run(true, "unsupported: " + nufe.Message.Split('.')[0], new()); }
        }

        private static async Task<Run> RunAsync(DbContext ctx, QueryIr ir)
        {
            try
            {
                var rows = await QueryIrDifferential.BuildNormQueryable(ctx, ir).ToListAsync();
                return new Run(false, null, rows.Select(Key).OrderBy(x => x, StringComparer.Ordinal).ToList());
            }
            catch (NormUnsupportedFeatureException nufe) { return new Run(true, "unsupported: " + nufe.Message.Split('.')[0], new()); }
        }

        [Fact]
        public async Task Sync_general_path_and_async_fast_path_agree_over_the_corpus()
        {
            var failures = new List<string>();
            var executed = 0;      // both paths ran (not both-rejected)
            var fastPathEligible = 0; // simple shapes the fast path actually engages (no set-op/distinct)
            for (var seed = 0; seed < 600; seed++)
            {
                var ir = QueryIrGenerator.Generate(seed);
                if (ir.GroupBy != null || ir.Projection != null) continue; // row-returning shapes only (this builder)

                using var ctx = QueryIrDifferential.CreateSeededContext(ir.Rows);
                var sync = RunSync(ctx, ir);
                var async = await RunAsync(ctx, ir);

                // A shape rejected on BOTH paths is fine; a shape that one path accepts and the other rejects is
                // itself a cross-plan divergence (path-dependent capability).
                if (sync.Threw && async.Threw) continue;
                if (sync.Threw != async.Threw)
                {
                    failures.Add($"seed {seed} [{ir.Describe()}]: THROW divergence — sync.threw={sync.Threw} ({sync.Error}) async.threw={async.Threw} ({async.Error})");
                    continue;
                }

                executed++;
                if (ir.SetOp == null && !ir.Steps.Any(s => s.Kind == IrStepKind.Distinct)) fastPathEligible++;
                if (!sync.Keys.SequenceEqual(async.Keys, StringComparer.Ordinal))
                    failures.Add($"seed {seed} [{ir.Describe()}]: ROW divergence — sync(general)=[{string.Join(";", sync.Keys)}] async(fast)=[{string.Join(";", async.Keys)}]");
            }

            Assert.True(failures.Count == 0,
                "cross-plan divergence between the sync general-translator path and the async fast path:\n" +
                string.Join("\n", failures.Take(10)));
            Assert.True(executed > 100, $"cross-plan sweep executed too few cases: {executed}");
            Assert.True(fastPathEligible > 30, $"too few fast-path-eligible shapes exercised: {fastPathEligible}");
        }
    }
}
