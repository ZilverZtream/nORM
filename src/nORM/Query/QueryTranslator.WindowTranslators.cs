using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private static string GetWindowAlias(LambdaExpression selector, int paramIndex, string defaultAlias)
        {
            if (selector.Body is NewExpression ne)
            {
                for (int i = 0; i < ne.Arguments.Count; i++)
                {
                    if (ne.Arguments[i] == selector.Parameters[paramIndex])
                        return ne.Members?[i].Name ?? defaultAlias;
                }
            }
            else if (selector.Body is MemberInitExpression mi)
            {
                foreach (var b in mi.Bindings)
                {
                    if (b is MemberAssignment ma && ma.Expression == selector.Parameters[paramIndex])
                        return b.Member.Name;
                }
            }
            return defaultAlias;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class RowNumberTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Adds a <c>ROW_NUMBER()</c> window function to the query and binds the result to the supplied selector.
            /// </summary>
            /// <param name="t">The translator creating the window function.</param>
            /// <param name="node">The method call expression for <c>WithRowNumber</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithRowNumber requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "RowNumber");
                var wf = new WindowFunctionInfo("ROW_NUMBER", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class RankTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Registers a <c>RANK()</c> window function and maps the result via the provided selector.
            /// </summary>
            /// <param name="t">The current translator.</param>
            /// <param name="node">The method call expression for <c>WithRank</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithRank requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "Rank");
                var wf = new WindowFunctionInfo("RANK", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class DenseRankTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Adds a <c>DENSE_RANK()</c> window function to the query.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression for <c>WithDenseRank</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var resultSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithDenseRank requires a result selector"));
                var alias = GetWindowAlias(resultSelector, 1, "DenseRank");
                var wf = new WindowFunctionInfo("DENSE_RANK", null, 0, null, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class LagTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Generates a <c>LAG</c> window function, capturing value, offset and optional default expressions.
            /// </summary>
            /// <param name="t">The translator creating the window.</param>
            /// <param name="node">The method call expression for <c>WithLag</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var valueSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLag requires a value selector"));
                int offset = TryGetIntValue(node.Arguments[2], out var off) ? off : 1;
                var resultSelector = StripQuotes(node.Arguments[3]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLag requires a result selector"));
                LambdaExpression? defaultSelector = null;
                if (node.Arguments.Count > 4)
                    defaultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
                var alias = GetWindowAlias(resultSelector, 1, "Lag");
                var wf = new WindowFunctionInfo("LAG", valueSelector, offset, defaultSelector, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class LeadTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Generates a <c>LEAD</c> window function, capturing value, offset and optional default expressions.
            /// </summary>
            /// <param name="t">The translator creating the window.</param>
            /// <param name="node">The method call expression for <c>WithLead</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                var valueSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLead requires a value selector"));
                int offset = TryGetIntValue(node.Arguments[2], out var off) ? off : 1;
                var resultSelector = StripQuotes(node.Arguments[3]) as LambdaExpression
                    ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "WithLead requires a result selector"));
                LambdaExpression? defaultSelector = null;
                if (node.Arguments.Count > 4)
                    defaultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;
                var alias = GetWindowAlias(resultSelector, 1, "Lead");
                var wf = new WindowFunctionInfo("LEAD", valueSelector, offset, defaultSelector, alias, resultSelector.Parameters[1], resultSelector);
                t._clauses.WindowFunctions.Add(wf);
                return t.Visit(node.Arguments[0]);
            }
        }
    }
}
