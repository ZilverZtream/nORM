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
