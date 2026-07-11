using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AggregateExpressionTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Delegates translation of aggregate expressions used in projected form, such as <c>Sum(x =&gt; ...)</c>.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the aggregate.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleAggregateExpression(node);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class GroupByTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Converts a LINQ <c>GroupBy</c> call into the SQL <c>GROUP BY</c> clause and related projections.
            /// </summary>
            /// <param name="t">The translator responsible for query compilation.</param>
            /// <param name="node">The method call expression for <c>GroupBy</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleGroupBy(node);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class DirectAggregateTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Handles direct aggregate operators like <c>Sum</c>, <c>Min</c>, or <c>Max</c> that operate on the entire sequence.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for the aggregate.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleDirectAggregate(node);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class AllTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates the <c>All</c> predicate to its SQL equivalent, typically using <c>NOT EXISTS</c>.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>All</c>.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                // A client-tail reshaped or group-join-result source must divert before
                // any NOT EXISTS SQL is built: translate the source as the row plan and
                // evaluate All in memory. The source has been visited at this point, so
                // falling through to SQL generation is never valid — fail closed if
                // client evaluation is impossible.
                if (SourceHasClientTailReshape(node.Arguments[0])
                    || SourceHasGroupJoinResultTail(node.Arguments[0])
                    || SourceHasRawGroupByResultTail(node.Arguments[0]))
                {
                    var source = t.Visit(node.Arguments[0]);
                    if (t.TryAppendClientScalarAggregate(node))
                        return source;
                    ThrowIfClientTailReshapePending(t, node.Method.Name);
                    throw new NormUnsupportedFeatureException(
                        $"{node.Method.Name} after a client-materialized sequence operator has no in-memory equivalent overload.");
                }
                var result = t.HandleAllOperation(node);
                // NOT EXISTS SQL evaluates against server rows and would ignore a pending
                // client-tail reshape hidden deeper than the operator spine (e.g. inside
                // a joined sub-source).
                ThrowIfClientTailReshapePending(t, node.Method.Name);
                return result;
            }
        }
    }
}
