using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Xunit;
using P = nORM.Tests.Fuzzing.JoinDifferential.Parent;
using C = nORM.Tests.Fuzzing.JoinDifferential.Child;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// INNER-join correctness battery: nORM's <c>Join</c> must exactly match the LINQ-to-Objects oracle across key
    /// matching, row multiplication (a parent with several children), orphan exclusion (a parent with no child, a
    /// child whose FK matches no parent), per-side filters, and empty tables. A divergence is a real join bug.
    /// </summary>
    [Trait("Category", TestCategory.Fast)]
    public class JoinDifferentialTests
    {
        // P3 (Id=3) is childless; C4 (ParentId=99) is an orphan; P1 has TWO children (multiplication).
        private static readonly P[] Parents =
        {
            new() { Id = 1, P = 10 }, new() { Id = 2, P = 20 }, new() { Id = 3, P = 30 },
        };
        private static readonly C[] Children =
        {
            new() { Id = 1, ParentId = 1, C = 1 }, new() { Id = 2, ParentId = 1, C = 2 },
            new() { Id = 3, ParentId = 2, C = 3 }, new() { Id = 4, ParentId = 99, C = 4 },
        };

        [Fact]
        public async Task Inner_join_matches_oracle_with_multiplication_and_orphans()
        {
            var (norm, oracle) = await JoinDifferential.RunAsync(Parents, Children);
            // P1×C1=1001, P1×C2=1002, P2×C3=2003; P3 (childless) and C4 (orphan) excluded.
            Assert.Equal(new[] { 1001, 1002, 2003 }, oracle);
            Assert.Equal(oracle, norm);
        }

        public static IEnumerable<object[]> FilterCases()
        {
            yield return new object[] { "parent P>15", (Expression<Func<P, bool>>)(p => p.P > 15), null! };
            yield return new object[] { "parent P==10", (Expression<Func<P, bool>>)(p => p.P == 10), null! };
            yield return new object[] { "parent none", (Expression<Func<P, bool>>)(p => p.P > 999), null! };
            yield return new object[] { "child C>1", null!, (Expression<Func<C, bool>>)(c => c.C > 1) };
            yield return new object[] { "child C==3", null!, (Expression<Func<C, bool>>)(c => c.C == 3) };
            yield return new object[] { "child none", null!, (Expression<Func<C, bool>>)(c => c.C > 999) };
        }

        [Theory]
        [MemberData(nameof(FilterCases))]
        public async Task Inner_join_with_a_side_filter_matches_oracle(string name, Expression<Func<P, bool>>? pf, Expression<Func<C, bool>>? cf)
        {
            var (norm, oracle) = await JoinDifferential.RunAsync(Parents, Children, pf, cf);
            Assert.Equal(oracle, norm);
            _ = name;
        }

        [Fact]
        public async Task Inner_join_with_both_side_filters_matches_oracle()
        {
            var (norm, oracle) = await JoinDifferential.RunAsync(
                Parents, Children, p => p.P >= 10, c => c.C <= 2);
            Assert.Equal(oracle, norm);
            Assert.NotEmpty(oracle); // teeth: a non-empty filtered join
        }

        [Fact]
        public async Task Inner_join_over_empty_child_table_is_empty()
        {
            var (norm, oracle) = await JoinDifferential.RunAsync(Parents, Array.Empty<C>());
            Assert.Empty(oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Inner_join_over_empty_parent_table_is_empty()
        {
            var (norm, oracle) = await JoinDifferential.RunAsync(Array.Empty<P>(), Children);
            Assert.Empty(oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Element_selector_groupby_parameterless_aggregate_in_computed_body_matches_oracle()
        {
            // GroupBy(key, elementSelector).Select(g => new { V = g.Key*1000 + g.Sum() }): the parameterless
            // aggregate must take the element selector (C) as its operand within the computed SQL.
            var children = new[]
            {
                new C { Id = 1, ParentId = 5, C = 1 }, new C { Id = 2, ParentId = 5, C = 2 },
                new C { Id = 3, ParentId = 8, C = 4 }, new C { Id = 4, ParentId = 8, C = 6 },
            };
            var (norm, oracle) = await JoinDifferential.RunElementSelectorComputedAsync(children);
            // group 5 → 5*1000 + Sum(1,2)=5003; group 8 → 8*1000 + Sum(4,6)=8010.
            Assert.Equal(new[] { 5003, 8010 }, oracle);
            Assert.Equal(oracle, norm);
        }

        [Theory]
        [InlineData(0)] // Key*1000 + Sum
        [InlineData(1)] // Sum*10 + Count (two aggregates)
        [InlineData(2)] // Max - Min (two distinct aggregates)
        public async Task Single_table_groupby_with_computed_aggregate_body_matches_oracle(int shape)
        {
            // GroupBy with the aggregate INSIDE a computed body (e.g. g.Key*1000 + g.Sum(c=>c.C)). Previously threw
            // NormUnsupportedFeatureException ("Method 'Sum' is not supported"); the grouped-projection fallback now
            // registers grouping keys so the embedded aggregate translates within the computed SQL.
            var children = new[]
            {
                new C { Id = 1, ParentId = 5, C = 1 }, new C { Id = 2, ParentId = 5, C = 2 },
                new C { Id = 3, ParentId = 8, C = 4 }, new C { Id = 4, ParentId = 8, C = 6 },
            };
            var (norm, oracle) = await JoinDifferential.RunGroupComputedShapeAsync(children, shape);
            Assert.NotEmpty(oracle); // teeth: a real computed result, not vacuous
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Join_then_groupby_matches_oracle()
        {
            // Two parents share P=10 (so one group aggregates children from BOTH), P3 has two children.
            var parents = new[] { new P { Id = 1, P = 10 }, new P { Id = 2, P = 10 }, new P { Id = 3, P = 20 } };
            var children = new[]
            {
                new C { Id = 1, ParentId = 1, C = 1 }, new C { Id = 2, ParentId = 2, C = 2 },
                new C { Id = 3, ParentId = 3, C = 3 }, new C { Id = 4, ParentId = 3, C = 4 },
            };
            var (norm, oracle) = await JoinDifferential.RunJoinGroupByAsync(parents, children);
            // group P=10 → sum(C1,C2)=3 → 10003; group P=20 → sum(C3,C4)=7 → 20007.
            Assert.Equal(new[] { 10003, 20007 }, oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Join_then_distinct_matches_oracle()
        {
            // Two parents share P=10 and two children share C=1, so joined pairs collide: (P=10,C=1) arises from
            // both P1×C1 and P2×C3 → must dedup to one. P3×C4 (P=20,C=1) and P1×C2 (P=10,C=2) are distinct.
            var parents = new[] { new P { Id = 1, P = 10 }, new P { Id = 2, P = 10 }, new P { Id = 3, P = 20 } };
            var children = new[]
            {
                new C { Id = 1, ParentId = 1, C = 1 }, new C { Id = 2, ParentId = 1, C = 2 },
                new C { Id = 3, ParentId = 2, C = 1 }, new C { Id = 4, ParentId = 3, C = 1 },
            };
            var (norm, oracle) = await JoinDifferential.RunJoinDistinctAsync(parents, children);
            // pairs: (10,1)×2→1001, (10,2)→1002, (20,1)→2001 → distinct {1001,1002,2001}.
            Assert.Equal(new[] { 1001, 1002, 2001 }, oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Coverage_sweep_join_then_distinct_agrees_with_oracle()
        {
            // Randomized join→Distinct; small domains make joined pairs collide. Tracks that dedup actually fires.
            var failures = new List<string>();
            var deduped = 0;
            for (var seed = 0; seed < 400; seed++)
            {
                var (parents, children, _, _) = JoinDifferential.Generate(seed);
                var (norm, oracle) = await JoinDifferential.RunJoinDistinctAsync(parents, children);
                if (!norm.SequenceEqual(oracle))
                    failures.Add($"seed {seed}: nORM=[{string.Join(",", norm)}] oracle=[{string.Join(",", oracle)}]");
                // Dedup fired when the joined pair count exceeds the distinct count.
                var joinCount = parents.Join(children, p => p.Id, c => c.ParentId, (p, c) => 1).Count();
                if (joinCount > oracle.Count) deduped++;
            }
            Assert.True(failures.Count == 0, "join→Distinct sweep divergences:\n" + string.Join("\n", failures.Take(10)));
            Assert.True(deduped > 5, $"sweep should exercise duplicate joined pairs, got {deduped}");
        }

        [Fact]
        public async Task Coverage_sweep_join_then_groupby_agrees_with_oracle()
        {
            // Randomized join→GroupBy: small P domain groups joined rows across parents; the aggregate sums the
            // inner C. Every case must match the LINQ pipeline. Tracks that a group spanning >=2 parents is hit.
            var failures = new List<string>();
            var multiParentGroup = 0;
            for (var seed = 0; seed < 400; seed++)
            {
                var (parents, children, _, _) = JoinDifferential.Generate(seed);
                var (norm, oracle) = await JoinDifferential.RunJoinGroupByAsync(parents, children);
                if (!norm.SequenceEqual(oracle))
                    failures.Add($"seed {seed}: nORM=[{string.Join(",", norm)}] oracle=[{string.Join(",", oracle)}]");
                // A group spans >=2 parents when >=2 parents share a P value AND both have >=1 child.
                var childByParent = children.GroupBy(c => c.ParentId).Select(g => g.Key).ToHashSet();
                if (parents.Where(p => childByParent.Contains(p.Id)).GroupBy(p => p.P).Any(g => g.Count() >= 2))
                    multiParentGroup++;
            }
            Assert.True(failures.Count == 0, "join→GroupBy sweep divergences:\n" + string.Join("\n", failures.Take(10)));
            Assert.True(multiParentGroup > 5, $"sweep should exercise groups spanning multiple parents, got {multiParentGroup}");
        }

        [Fact]
        public async Task GroupJoin_count_per_outer_matches_oracle()
        {
            // GroupJoin consuming the group with a per-outer Count, joined on the unique PK.
            // P1 has 2 children, P2 has 1, P3 has 0 → 1002, 2001, 3000.
            var (norm, oracle) = await JoinDifferential.RunGroupCountAsync(Parents, Children, joinOnValue: false);
            Assert.Equal(new[] { 1002, 2001, 3000 }, oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task GroupJoin_on_a_non_unique_key_does_not_merge_distinct_outers()
        {
            // The outer-row segmentation scenario: two DISTINCT parents (Id 1,2) share the join key P=5, one
            // parent has P=9. Joining on the NON-unique P means both P=5 parents match the same children. The
            // flattened stream must segment by the outer PK, not by the join key — otherwise the two P=5 parents
            // merge and one row is lost. LINQ GroupJoin gives one row per outer element → 3 rows.
            var parents = new[] { new P { Id = 1, P = 5 }, new P { Id = 2, P = 5 }, new P { Id = 3, P = 9 } };
            var children = new[]
            {
                new C { Id = 1, ParentId = 5, C = 1 }, new C { Id = 2, ParentId = 5, C = 2 },
                new C { Id = 3, ParentId = 9, C = 3 },
            };
            var (norm, oracle) = await JoinDifferential.RunGroupCountAsync(parents, children, joinOnValue: true);
            // P1(P=5)→{C1,C2}=2, P2(P=5)→{C1,C2}=2, P3(P=9)→{C3}=1 → 1002, 2002, 3001. Three distinct outer rows.
            Assert.Equal(new[] { 1002, 2002, 3001 }, oracle);
            Assert.Equal(3, norm.Count); // teeth: a merge-by-key bug would collapse the two P=5 parents to 2 rows
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Left_join_keeps_childless_parents_as_null_rows()
        {
            var (norm, oracle) = await JoinDifferential.RunLeftAsync(Parents, Children);
            // P1×C1=1001, P1×C2=1002, P2×C3=2003, and P3 (childless) → -30 (null child). Orphan C4 excluded.
            Assert.Equal(new[] { -30, 1001, 1002, 2003 }, oracle);
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Left_join_all_parents_childless_yields_a_row_per_parent()
        {
            // No children at all → every parent survives as a null-child row: -10, -20, -30.
            var (norm, oracle) = await JoinDifferential.RunLeftAsync(Parents, Array.Empty<C>());
            Assert.Equal(new[] { -30, -20, -10 }.OrderBy(x => x), oracle.OrderBy(x => x));
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Left_join_with_a_child_filter_reverts_filtered_out_parents_to_null_rows()
        {
            // Child filter C>2 keeps only C3 (ParentId 2). So P1's children are filtered out → P1 becomes a null
            // row (-10); P2 keeps C3 → 2003; P3 stays null (-30). The classic "filter demotes to null row" case.
            var (norm, oracle) = await JoinDifferential.RunLeftAsync(Parents, Children, childFilter: c => c.C > 2);
            Assert.Equal(new[] { -30, -10, 2003 }.OrderBy(x => x), oracle.OrderBy(x => x));
            Assert.Equal(oracle, norm);
        }

        [Fact]
        public async Task Coverage_sweep_inner_join_agrees_with_oracle()
        {
            // Randomized parents/children/filters across 400 seeds — orphans, shared keys, multiplication, empty
            // sides. Every case must match the LINQ oracle. Tracks that non-trivial (multiplying) joins are hit.
            var failures = new List<string>();
            var multiplicity = 0;
            for (var seed = 0; seed < 400; seed++)
            {
                var (parents, children, pf, cf) = JoinDifferential.Generate(seed);
                var (norm, oracle) = await JoinDifferential.RunAsync(parents, children, pf, cf);
                if (!norm.SequenceEqual(oracle))
                    failures.Add($"seed {seed}: nORM=[{string.Join(",", norm)}] oracle=[{string.Join(",", oracle)}]");
                // Row multiplication is exercised when a real parent has >=2 children (it then appears in >=2 result rows).
                var childCountByParent = children.GroupBy(c => c.ParentId).ToDictionary(g => g.Key, g => g.Count());
                if (parents.Any(p => childCountByParent.TryGetValue(p.Id, out var n) && n >= 2)) multiplicity++;
            }
            Assert.True(failures.Count == 0, "join sweep divergences:\n" + string.Join("\n", failures.Take(10)));
            Assert.True(multiplicity > 5, $"sweep should exercise parent row-multiplication, got {multiplicity}");
        }

        [Fact]
        public async Task Coverage_sweep_groupjoin_nonunique_key_agrees_with_oracle()
        {
            // GroupJoin on the non-unique P (small 0-5 domain, so parents frequently share a join key) consuming a
            // per-outer Count. Every distinct outer must survive with its own count. Tracks that key-sharing (the
            // segmentation trigger) is actually exercised.
            var failures = new List<string>();
            var sharedKey = 0;
            for (var seed = 0; seed < 400; seed++)
            {
                var (parents, children, _, _) = JoinDifferential.Generate(seed);
                var (norm, oracle) = await JoinDifferential.RunGroupCountAsync(parents, children, joinOnValue: true);
                if (!norm.SequenceEqual(oracle))
                    failures.Add($"seed {seed}: nORM=[{string.Join(",", norm)}] oracle=[{string.Join(",", oracle)}]");
                if (parents.GroupBy(p => p.P).Any(g => g.Count() >= 2)) sharedKey++;
            }
            Assert.True(failures.Count == 0, "GroupJoin non-unique-key sweep divergences:\n" + string.Join("\n", failures.Take(10)));
            Assert.True(sharedKey > 20, $"sweep should exercise distinct outers sharing a join key, got {sharedKey}");
        }

        [Fact]
        public async Task Coverage_sweep_left_join_agrees_with_oracle()
        {
            // Same randomized data as the INNER sweep, but LEFT-joined: every parent survives, childless parents
            // become NULL rows. Must match the LINQ oracle. Tracks that NULL-child rows are actually exercised.
            var failures = new List<string>();
            var nullRows = 0;
            for (var seed = 0; seed < 400; seed++)
            {
                var (parents, children, pf, cf) = JoinDifferential.Generate(seed);
                var (norm, oracle) = await JoinDifferential.RunLeftAsync(parents, children, pf, cf);
                if (!norm.SequenceEqual(oracle))
                    failures.Add($"seed {seed}: nORM=[{string.Join(",", norm)}] oracle=[{string.Join(",", oracle)}]");
                if (oracle.Any(v => v < 0)) nullRows++; // negative marker = a null-child (childless-parent) row
            }
            Assert.True(failures.Count == 0, "LEFT join sweep divergences:\n" + string.Join("\n", failures.Take(10)));
            Assert.True(nullRows > 20, $"LEFT sweep should exercise null-child rows, got {nullRows}");
        }

        [Fact]
        public async Task Inner_join_high_multiplicity_matches_oracle()
        {
            // One parent with FOUR children, plus a second single-child parent — heavier row multiplication.
            var parents = new[] { new P { Id = 1, P = 5 }, new P { Id = 2, P = 7 } };
            var children = new[]
            {
                new C { Id = 1, ParentId = 1, C = 1 }, new C { Id = 2, ParentId = 1, C = 2 },
                new C { Id = 3, ParentId = 1, C = 3 }, new C { Id = 4, ParentId = 1, C = 4 },
                new C { Id = 5, ParentId = 2, C = 9 },
            };
            var (norm, oracle) = await JoinDifferential.RunAsync(parents, children);
            Assert.Equal(5, oracle.Count); // 4 for parent 1, 1 for parent 2
            Assert.Equal(oracle, norm);
        }
    }
}
