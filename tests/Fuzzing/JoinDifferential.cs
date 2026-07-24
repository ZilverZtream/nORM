using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Differential harness for INNER equi-joins over two entities. nORM's <c>Queryable.Join</c> must return the
    /// exact same multiset as LINQ-to-Objects <c>Enumerable.Join</c> over the same seeded data — a divergence is a
    /// real join-translation bug (row multiplication, orphan exclusion, key mismatch, or per-side filter placement).
    /// The result selector projects to a scalar <c>p.P*100 + c.C</c> so the comparison is an int multiset (no
    /// anonymous-type expression construction) while still depending on BOTH joined rows.
    /// </summary>
    public static class JoinDifferential
    {
        [Table("JoinParent")]
        public sealed class Parent
        {
            [Key] public int Id { get; set; }
            public int P { get; set; }
        }

        [Table("JoinChild")]
        public sealed class Child
        {
            [Key] public int Id { get; set; }
            public int ParentId { get; set; }
            public int C { get; set; }
        }

        // Fixed filter lambdas the generator picks from — written as C# lambdas so the compiler builds the
        // Expression (no manual construction), and reused compiled for the oracle so both sides are identical.
        private static readonly Expression<Func<Parent, bool>>[] ParentFilters =
        {
            p => p.P > 2, p => p.P == 3, p => p.P <= 4, p => p.P != 0, p => p.P >= 1,
        };
        private static readonly Expression<Func<Child, bool>>[] ChildFilters =
        {
            c => c.C > 1, c => c.C == 2, c => c.C <= 3, c => c.C != 0, c => c.ParentId > 1,
        };

        /// <summary>
        /// Generates a randomized INNER-join case: 0-4 parents and 0-6 children, child FKs spanning valid parents
        /// AND out-of-range values (orphans), small value domains so keys collide and rows multiply, and optional
        /// per-side filters. Stays within the validated join+filter shape, so the oracle needs no new subtlety.
        /// </summary>
        public static (List<Parent> Parents, List<Child> Children, Expression<Func<Parent, bool>>? Pf, Expression<Func<Child, bool>>? Cf) Generate(int seed)
        {
            var rng = new Random(seed);
            var np = rng.Next(0, 5);
            var parents = Enumerable.Range(1, np).Select(i => new Parent { Id = i, P = rng.Next(0, 6) }).ToList();
            var nc = rng.Next(0, 7);
            // ParentId in 1..np+2 so some children are orphans (FK past the last parent) and parents share children.
            var children = Enumerable.Range(1, nc).Select(i => new Child { Id = i, ParentId = rng.Next(1, np + 3), C = rng.Next(0, 6) }).ToList();
            var pf = rng.Next(3) == 0 ? ParentFilters[rng.Next(ParentFilters.Length)] : null;
            var cf = rng.Next(3) == 0 ? ChildFilters[rng.Next(ChildFilters.Length)] : null;
            return (parents, children, pf, cf);
        }

        /// <summary>
        /// Runs a LEFT OUTER join (GroupJoin + SelectMany + DefaultIfEmpty) through nORM and the LINQ-to-Objects
        /// oracle. Every outer (parent) row survives: a parent with no matching child yields one row with a NULL
        /// child, projected to <c>-p.P</c> (a distinct negative marker); a matched pair projects <c>p.P*100+c.C</c>.
        /// Optional pre-join filters apply to each source identically on both sides. The null-child row and the
        /// <c>c == null</c> projection are the subtle LEFT-join surface (outer-row segmentation, NULL handling).
        /// </summary>
        public static async Task<(List<int> Norm, List<int> Oracle)> RunLeftAsync(
            IReadOnlyList<Parent> parents,
            IReadOnlyList<Child> children,
            Expression<Func<Parent, bool>>? parentFilter = null,
            Expression<Func<Child, bool>>? childFilter = null)
        {
            using var ctx = Seed(parents, children);

            IQueryable<Parent> outer = ctx.Query<Parent>();
            if (parentFilter != null) outer = outer.Where(parentFilter);
            IQueryable<Child> inner = ctx.Query<Child>();
            if (childFilter != null) inner = inner.Where(childFilter);

            var norm = (await outer
                .GroupJoin(inner, p => p.Id, c => c.ParentId, (p, cs) => new { p, cs })
                .SelectMany(x => x.cs.DefaultIfEmpty(), (x, c) => new { V = c == null ? -x.p.P : x.p.P * 100 + c.C })
                .ToListAsync())
                .Select(x => x.V).OrderBy(x => x).ToList();

            IEnumerable<Parent> oParents = parents;
            if (parentFilter != null) oParents = oParents.Where(parentFilter.Compile());
            IEnumerable<Child> oChildren = children;
            if (childFilter != null) oChildren = oChildren.Where(childFilter.Compile());
            var oracle = oParents
                .GroupJoin(oChildren, p => p.Id, c => c.ParentId, (p, cs) => new { p, cs })
                .SelectMany(x => x.cs.DefaultIfEmpty(), (x, c) => c == null ? -x.p.P : x.p.P * 100 + c.C)
                .OrderBy(x => x).ToList();

            return (norm, oracle);
        }

        /// <summary>
        /// GroupJoin that CONSUMES the group (<c>cs.Count()</c> per outer), projecting <c>p.Id*1000 + count</c>.
        /// When <paramref name="joinOnValue"/> is true the join key is the NON-unique <c>p.P</c> (vs <c>c.ParentId</c>),
        /// so two distinct outer rows can share a join key — the precise outer-row segmentation scenario: the
        /// flattened stream must be grouped back by the outer's identity (PK), not by the join key, or the two
        /// same-key outers merge and one is lost. LINQ GroupJoin groups per outer element, so it is the oracle.
        /// </summary>
        public static async Task<(List<int> Norm, List<int> Oracle)> RunGroupCountAsync(
            IReadOnlyList<Parent> parents, IReadOnlyList<Child> children, bool joinOnValue)
        {
            using var ctx = Seed(parents, children);

            var norm = (joinOnValue
                    ? await ctx.Query<Parent>().GroupJoin(ctx.Query<Child>(), p => p.P, c => c.ParentId, (p, cs) => new { V = p.Id * 1000 + cs.Count() }).ToListAsync()
                    : await ctx.Query<Parent>().GroupJoin(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, cs) => new { V = p.Id * 1000 + cs.Count() }).ToListAsync())
                .Select(x => x.V).OrderBy(x => x).ToList();

            var oracle = (joinOnValue
                    ? parents.GroupJoin(children, p => p.P, c => c.ParentId, (p, cs) => p.Id * 1000 + cs.Count())
                    : parents.GroupJoin(children, p => p.Id, c => c.ParentId, (p, cs) => p.Id * 1000 + cs.Count()))
                .OrderBy(x => x).ToList();

            return (norm, oracle);
        }

        private static DbContext Seed(IReadOnlyList<Parent> parents, IReadOnlyList<Child> children)
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE JoinParent (Id INTEGER PRIMARY KEY, P INTEGER NOT NULL);" +
                    "CREATE TABLE JoinChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, C INTEGER NOT NULL);";
                cmd.ExecuteNonQuery();
            }
            foreach (var p in parents)
            {
                using var c = cn.CreateCommand();
                c.CommandText = "INSERT INTO JoinParent (Id, P) VALUES ($id,$p)";
                c.Parameters.AddWithValue("$id", p.Id);
                c.Parameters.AddWithValue("$p", p.P);
                c.ExecuteNonQuery();
            }
            foreach (var ch in children)
            {
                using var c = cn.CreateCommand();
                c.CommandText = "INSERT INTO JoinChild (Id, ParentId, C) VALUES ($id,$pid,$c)";
                c.Parameters.AddWithValue("$id", ch.Id);
                c.Parameters.AddWithValue("$pid", ch.ParentId);
                c.Parameters.AddWithValue("$c", ch.C);
                c.ExecuteNonQuery();
            }
            return new DbContext(cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions(), ownsConnection: true);
        }

        /// <summary>
        /// Runs an INNER join (with optional per-side filters) through nORM and the LINQ-to-Objects oracle and
        /// returns both sorted int multisets. The same projection and filters drive both sides.
        /// </summary>
        public static async Task<(List<int> Norm, List<int> Oracle)> RunAsync(
            IReadOnlyList<Parent> parents,
            IReadOnlyList<Child> children,
            Expression<Func<Parent, bool>>? parentFilter = null,
            Expression<Func<Child, bool>>? childFilter = null)
        {
            using var ctx = Seed(parents, children);

            IQueryable<Parent> outer = ctx.Query<Parent>();
            if (parentFilter != null) outer = outer.Where(parentFilter);
            IQueryable<Child> inner = ctx.Query<Child>();
            if (childFilter != null) inner = inner.Where(childFilter);

            // Project to an anonymous type (reference type) so ToListAsync's class constraint is satisfied; the
            // scalar p.P*100+c.C still depends on BOTH joined rows. The C# compiler builds the anon-type Expression.
            var norm = (await outer.Join(inner, p => p.Id, c => c.ParentId, (p, c) => new { V = p.P * 100 + c.C }).ToListAsync())
                .Select(x => x.V).OrderBy(x => x).ToList();

            IEnumerable<Parent> oParents = parents;
            if (parentFilter != null) oParents = oParents.Where(parentFilter.Compile());
            IEnumerable<Child> oChildren = children;
            if (childFilter != null) oChildren = oChildren.Where(childFilter.Compile());
            var oracle = oParents.Join(oChildren, p => p.Id, c => c.ParentId, (p, c) => p.P * 100 + c.C)
                .OrderBy(x => x).ToList();

            return (norm, oracle);
        }
    }
}
