using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// Produces <see cref="QueryIr"/> cases from a seed and structure-aware mutations of existing cases.
    /// Values are drawn from a small domain so predicates actually select (and overlap, producing order ties
    /// and duplicate groups), and the generated clause set is always well-formed. A seed is only metadata; the
    /// emitted IR is the durable artifact.
    /// </summary>
    public static class QueryIrGenerator
    {
        private static readonly IrColumn[] Columns = { IrColumn.Id, IrColumn.A, IrColumn.B };
        private static readonly IrCompare[] Compares =
            { IrCompare.Eq, IrCompare.Ne, IrCompare.Lt, IrCompare.Le, IrCompare.Gt, IrCompare.Ge };

        public static QueryIr Generate(int seed)
        {
            var rng = new Random(seed);
            var rowCount = rng.Next(0, 9);
            var rows = new List<IrRow>(rowCount);
            for (var i = 1; i <= rowCount; i++)
                rows.Add(new IrRow { Id = i, A = rng.Next(0, 6), B = rng.Next(0, 6), Name = ((char)('a' + rng.Next(0, 4))).ToString() });

            var steps = new List<IrStep>();
            for (var i = rng.Next(0, 3); i > 0; i--)
                steps.Add(IrStep.Where(Pick(rng, Columns), Pick(rng, Compares), rng.Next(0, 6)));
            for (var i = rng.Next(0, 3); i > 0; i--)
                steps.Add(IrStep.OrderBy(Pick(rng, Columns), rng.Next(2) == 0));
            if (rng.Next(3) == 0)
                steps.Add(IrStep.Distinct());
            if (rng.Next(2) == 0)
                steps.Add(IrStep.Skip(rng.Next(0, rowCount + 2)));
            if (rng.Next(2) == 0)
                steps.Add(IrStep.Take(rng.Next(0, rowCount + 2)));

            return new QueryIr { Rows = rows, Steps = steps, SetOp = MaybeSetOp(rng), Projection = MaybeProjection(rng) };
        }

        private static IrSetOp? MaybeSetOp(Random rng)
        {
            if (rng.Next(3) != 0) return null;   // ~1/3 of cases carry a set operation
            var rightWheres = new List<IrStep>();
            for (var i = rng.Next(0, 3); i > 0; i--)
                rightWheres.Add(IrStep.Where(Pick(rng, Columns), Pick(rng, Compares), rng.Next(0, 6)));
            return new IrSetOp { Kind = (IrSetOpKind)rng.Next(4), RightWheres = rightWheres };
        }

        private static IrProjection? MaybeProjection(Random rng)
        {
            if (rng.Next(4) != 0) return null;   // ~1/4 of cases project a scalar
            return new IrProjection { Column = Pick(rng, Columns), Add = rng.Next(3) == 0 ? 0 : rng.Next(-5, 100) };
        }

        /// <summary>
        /// Structure-aware mutation of a valid case: add/remove a step, tweak a constant toward a boundary,
        /// or add/drop a row. Validity-preserving mutations of a corpus case reach deeper states than
        /// regenerating from scratch.
        /// </summary>
        public static QueryIr Mutate(QueryIr ir, int seed)
        {
            var rng = new Random(seed);
            var steps = ir.Steps.ToList();
            var rows = ir.Rows.ToList();
            var setOp = ir.SetOp;
            var projection = ir.Projection;
            switch (rng.Next(8))
            {
                case 0: steps.Add(IrStep.Where(Pick(rng, Columns), Pick(rng, Compares), rng.Next(0, 6))); break;
                case 1: if (steps.Count > 0) steps.RemoveAt(rng.Next(steps.Count)); break;
                case 2: steps.Add(IrStep.OrderBy(Pick(rng, Columns), rng.Next(2) == 0)); break;
                case 3: steps.Add(rng.Next(2) == 0 ? IrStep.Skip(rng.Next(0, rows.Count + 2)) : IrStep.Take(rng.Next(0, rows.Count + 2))); break;
                case 4: rows.Add(new IrRow { Id = (rows.Count == 0 ? 1 : rows.Max(r => r.Id) + 1), A = rng.Next(0, 6), B = rng.Next(0, 6), Name = ((char)('a' + rng.Next(0, 4))).ToString() }); break;
                case 5: if (rows.Count > 0) rows.RemoveAt(rng.Next(rows.Count)); break;
                case 6: // toggle / reshape the set operation
                    setOp = setOp == null
                        ? new IrSetOp { Kind = (IrSetOpKind)rng.Next(4), RightWheres = new[] { IrStep.Where(Pick(rng, Columns), Pick(rng, Compares), rng.Next(0, 6)) } }
                        : (rng.Next(3) == 0 ? null : setOp with { Kind = (IrSetOpKind)rng.Next(4) });
                    break;
                case 7: // toggle / reshape the scalar projection
                    projection = projection == null
                        ? new IrProjection { Column = Pick(rng, Columns), Add = rng.Next(-5, 100) }
                        : (rng.Next(3) == 0 ? null : projection with { Column = Pick(rng, Columns), Add = rng.Next(-5, 100) });
                    break;
            }
            return new QueryIr { Rows = rows, Steps = steps, SetOp = setOp, Projection = projection };
        }

        private static T Pick<T>(Random rng, T[] items) => items[rng.Next(items.Length)];
    }
}
