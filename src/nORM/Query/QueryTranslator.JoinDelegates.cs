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
        private sealed class JoinTranslator : IMethodCallTranslator
        {
            private readonly bool _isGroupJoin;
            public JoinTranslator(bool isGroupJoin) => _isGroupJoin = isGroupJoin;

            /// <summary>
            /// Translates <c>Join</c> and <c>GroupJoin</c> operations by delegating to the appropriate handler.
            /// </summary>
            /// <param name="t">The active translator.</param>
            /// <param name="node">The method call expression representing the join.</param>
            /// <returns>The translated expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return _isGroupJoin ? t.HandleGroupJoin(node) : t.HandleInnerJoin(node);
            }
        }

        private sealed class SelectManyTranslator : IMethodCallTranslator
        {
            /// <summary>
            /// Translates <c>SelectMany</c> calls into SQL <c>JOIN</c> operations.
            /// </summary>
            /// <param name="t">The <see cref="QueryTranslator"/> executing translation.</param>
            /// <param name="node">The method call expression for <c>SelectMany</c>.</param>
            /// <returns>The translated source expression.</returns>
            public Expression Translate(QueryTranslator t, MethodCallExpression node)
            {
                return t.HandleSelectMany(node);
            }
        }

    }
}
