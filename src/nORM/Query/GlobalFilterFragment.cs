using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    /// <summary>
    /// Translates an entity's combined global query filters into a SQL predicate fragment for SQL that
    /// is hand-built OUTSIDE the main query pipeline — eager-load (Include / split query) child queries
    /// and translator-built correlated subqueries. The pipeline applies global filters via
    /// <c>ApplyGlobalFilters</c> on the root LINQ tree only; these secondary SQL builders must emit the
    /// same predicate themselves or they leak filtered rows (soft-deleted, or another tenant's).
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members.")]
    internal static class GlobalFilterFragment
    {
        /// <summary>
        /// Builds the global-filter SQL predicate for <paramref name="map"/>'s entity type, with column
        /// references qualified by <paramref name="qualifier"/> (a table name or alias), binding any
        /// literal parameters onto <paramref name="cmd"/>. Returns <c>null</c> when no global filter
        /// applies to the type. Parameter numbering starts after the command's existing parameters so it
        /// cannot collide with FK-IN or tenant parameters already added.
        /// </summary>
        internal static string? Build(DbContext ctx, TableMapping map, string qualifier, DbCommand cmd)
        {
            if (ctx.Options.GlobalFilters.Count == 0)
                return null;

            var combined = Combine(ctx, map.Type);
            if (combined == null)
                return null;

            var vctx = new VisitorContext(ctx, map, ctx.RawProvider, combined.Parameters[0], qualifier,
                correlated: null, compiledParams: null, paramMap: null,
                recursionDepth: 0, paramIndexStart: cmd.Parameters.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            try
            {
                var sql = visitor.Translate(combined.Body);
                foreach (var kvp in visitor.GetParameters())
                    cmd.AddParam(kvp.Key, kvp.Value);
                return sql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(visitor);
            }
        }

        /// <summary>
        /// Combines every global filter that applies to <paramref name="entityType"/> into one AND-ed
        /// lambda, mirroring NormQueryProvider.CombineGlobalFilterPredicates (including the two-parameter
        /// <c>(ctx, e) =&gt; …</c> filter form, whose context parameter is inlined as a constant). Exposed
        /// so translator-built correlated subqueries can add the same filter as a <c>Where</c> clause.
        /// Returns <c>null</c> when no filter applies.
        /// </summary>
        internal static LambdaExpression? Combine(DbContext ctx, Type entityType)
        {
            List<Expression>? bodies = null;
            ParameterExpression? param = null;
            foreach (var kvp in ctx.Options.GlobalFilters)
            {
                if (!kvp.Key.IsAssignableFrom(entityType)) continue;
                foreach (var filter in kvp.Value)
                {
                    param ??= Expression.Parameter(entityType, "gf");
                    Expression body;
                    if (filter.Parameters.Count == 2)
                    {
                        body = new ParameterReplacer(filter.Parameters[0], Expression.Constant(ctx)).Visit(filter.Body)!;
                        body = new ParameterReplacer(filter.Parameters[1], param).Visit(body)!;
                    }
                    else
                    {
                        body = new ParameterReplacer(filter.Parameters[0], param).Visit(filter.Body)!;
                    }
                    (bodies ??= new List<Expression>()).Add(body);
                }
            }

            if (bodies == null)
                return null;

            Expression combined = bodies[0];
            for (int i = 1; i < bodies.Count; i++)
                combined = Expression.AndAlso(combined, bodies[i]);
            return Expression.Lambda(combined, param!);
        }
    }
}
