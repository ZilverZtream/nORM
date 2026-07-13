using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private bool TryPrepareDistinctScalarJoinOuter(
            Expression outerQuery,
            LambdaExpression outerKeySelector,
            LambdaExpression resultSelector,
            string outerAlias,
            out DistinctScalarJoinOuter distinctOuter)
        {
            distinctOuter = default;

            if (outerQuery is not MethodCallExpression distinctCall
                || distinctCall.Arguments.Count == 0
                || distinctCall.Arguments[0] is not MethodCallExpression selectCall
                || selectCall.Method.Name != nameof(Queryable.Select)
                || selectCall.Arguments.Count < 2
                || StripQuotes(selectCall.Arguments[1]) is not LambdaExpression projectionSelector
                || projectionSelector.Parameters.Count != 1
                || outerKeySelector.Parameters.Count != 1
                || resultSelector.Parameters.Count != 2)
            {
                return false;
            }

            var projectedSource = selectCall.Arguments[0];
            var projectedSourceType = GetElementType(projectedSource);
            var projectedSourceMapping = TrackMapping(projectedSourceType);

            if (!TryGetMappedScalarProjection(projectionSelector, projectedSourceMapping, out var projectedBody))
                return false;

            if (outerKeySelector.Parameters[0].Type != projectedBody.Type
                || resultSelector.Parameters[0].Type != projectedBody.Type)
            {
                return false;
            }

            var rewrittenOuterKeyBody = new ParameterReplacer(outerKeySelector.Parameters[0], projectedBody)
                .Visit(outerKeySelector.Body)!;
            var rewrittenOuterKeySelector = Expression.Lambda(rewrittenOuterKeyBody, projectionSelector.Parameters);

            var rewrittenResultBody = new ParameterReplacer(resultSelector.Parameters[0], projectedBody)
                .Visit(resultSelector.Body)!;
            var rewrittenResultSelector = Expression.Lambda(
                rewrittenResultBody,
                projectionSelector.Parameters[0],
                resultSelector.Parameters[1]);

            var subOuter = TranslateInSubContext(
                outerQuery,
                projectedSourceMapping,
                _parameterManager.Index,
                _joinCounter,
                _recursionDepth + 1,
                out var subOuterMap);
            MergeSubPlanParameters(subOuter);

            distinctOuter = new DistinctScalarJoinOuter(
                subOuterMap,
                "(" + subOuter.Sql + ") AS " + outerAlias,
                rewrittenOuterKeySelector,
                rewrittenResultSelector);
            return true;
        }

        private static bool TryGetMappedScalarProjection(
            LambdaExpression projectionSelector,
            TableMapping mapping,
            out Expression projectedBody)
        {
            projectedBody = projectionSelector.Body;
            var mappedBody = StripConvert(projectedBody);
            if (mappedBody is not MemberExpression member
                || !TableMapping.TryGetMemberAccessRoot(member, out var root)
                || root != projectionSelector.Parameters[0]
                || !mapping.TryGetColumnForMemberAccess(member, out _))
            {
                projectedBody = null!;
                return false;
            }

            return true;
        }

        private static Expression StripConvert(Expression expression)
        {
            while (expression is UnaryExpression unary
                   && unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                expression = unary.Operand;
            }

            return expression;
        }

        private readonly struct DistinctScalarJoinOuter
        {
            public DistinctScalarJoinOuter(
                TableMapping mapping,
                string fromSql,
                LambdaExpression outerKeySelector,
                LambdaExpression resultSelector)
            {
                Mapping = mapping;
                FromSql = fromSql;
                OuterKeySelector = outerKeySelector;
                ResultSelector = resultSelector;
            }

            public TableMapping Mapping { get; }
            public string FromSql { get; }
            public LambdaExpression OuterKeySelector { get; }
            public LambdaExpression ResultSelector { get; }
        }

        private string TranslateJoinProjectionExpression(Expression expression)
        {
            if (_projection == null || _projection.Parameters.Count == 0)
                throw new NormQueryException("Join projection translation requires a result selector.");

            var parameter = _projection.Parameters[0];
            if (!_correlatedParams.TryGetValue(parameter, out var info))
                info = (_mapping, EscapeAlias("T0"));

            var vctx = new VisitorContext(
                _ctx,
                info.Mapping,
                _provider,
                parameter,
                info.Alias,
                _correlatedParams,
                _compiledParams,
                _paramConverters,
                _paramMap,
                _recursionDepth,
                _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctx);
            try
            {
                var sql = visitor.Translate(expression);
                foreach (var kvp in visitor.GetParameters())
                {
                    AddLiteralParameter(kvp.Key, kvp.Value);
                    AdvanceParameterIndexPast(kvp.Key);
                }

                return sql;
            }
            finally
            {
                FastExpressionVisitorPool.Return(visitor);
            }
        }

        private void RewritePrebuiltJoinProjectionIfNeeded()
        {
            if (_projection == null
                || _groupJoinInfo != null
                || _isAggregate
                // A grouped projection is not a join projection: its SELECT was built
                // by the GroupBy machinery (key members resolve through the composite
                // key map, not the join visitors), and the " JOIN " sniff below would
                // false-positive on an applied lateral column's CROSS JOIN LATERAL.
                || _groupBy.Count > 0
                || _sql.Length == 0)
            {
                return;
            }

            var sql = _sql.ToSqlString();
            if (!sql.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)
                || !sql.Contains(" JOIN ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" UNION ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" INTERSECT ", StringComparison.OrdinalIgnoreCase)
                || sql.Contains(" EXCEPT ", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (!TryBuildJoinProjectionSelectList(_projection.Body, out var selectList))
                return;

            var fromIndex = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIndex <= "SELECT ".Length || string.IsNullOrWhiteSpace(selectList))
                return;

            var selectPrefix = sql.StartsWith("SELECT DISTINCT ", StringComparison.OrdinalIgnoreCase)
                ? "SELECT DISTINCT "
                : "SELECT ";

            _sql.Clear();
            _sql.Append(selectPrefix).Append(selectList).Append(sql.AsSpan(fromIndex));
        }

        private bool TryBuildJoinProjectionSelectList(Expression projectionBody, out string selectList)
        {
            using var builder = new OptimizedSqlBuilder(256);
            selectList = string.Empty;

            if (projectionBody is MemberInitExpression memberInit)
            {
                var wroteAny = false;
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        continue;

                    if (ContainsWholeEntityParameter(assignment.Expression))
                        return false;

                    if (wroteAny)
                        builder.Append(", ");

                    builder.Append(TranslateJoinProjectionExpression(assignment.Expression))
                        .Append(" AS ")
                        .Append(_provider.Escape(assignment.Member.Name));
                    wroteAny = true;
                }

                if (!wroteAny)
                    return false;

                selectList = builder.ToSqlString();
                return true;
            }

            if (projectionBody is NewExpression newExpr)
            {
                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var argument = newExpr.Arguments[i];
                    if (ContainsWholeEntityParameter(argument))
                        return false;

                    if (i > 0)
                        builder.Append(", ");

                    var alias = newExpr.Members?[i].Name ?? $"Item{i + 1}";
                    builder.Append(TranslateJoinProjectionExpression(argument))
                        .Append(" AS ")
                        .Append(_provider.Escape(alias));
                }

                if (newExpr.Arguments.Count == 0)
                    return false;

                selectList = builder.ToSqlString();
                return true;
            }

            if (ContainsWholeEntityParameter(projectionBody))
                return false;

            selectList = TranslateJoinProjectionExpression(projectionBody);
            return !string.IsNullOrWhiteSpace(selectList);
        }

        private static bool ContainsWholeEntityParameter(Expression expression)
        {
            if (expression is ParameterExpression)
                return true;

            var visitor = new WholeEntityParameterVisitor();
            visitor.Visit(expression);
            return visitor.Found;
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class WholeEntityParameterVisitor : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                Found = true;
                return node;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression is ParameterExpression)
                    return node;

                return base.VisitMember(node);
            }
        }
    }
}
