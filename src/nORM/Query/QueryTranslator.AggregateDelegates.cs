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
                return t.HandleAllOperation(node);
            }
        }
    }
}
