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
        /// <summary>
        /// Translates the query-syntax left-join shape (GroupJoin + SelectMany + DefaultIfEmpty)
        /// directly to a LEFT JOIN, bypassing MaterializeGroupJoin. The SelectMany result
        /// selector's parameters get rewritten so its body references the outer / inner
        /// entities directly via the join's aliases.
        /// </summary>
        private void HandleQuerySyntaxLeftJoin(
            Expression outerQuery,
            Expression innerQuery,
            LambdaExpression outerKeySel,
            LambdaExpression innerKeySel,
            LambdaExpression groupJoinResultSelector,
            LambdaExpression collectionSelector,
            LambdaExpression smResultSelector,
            string outerMemberName)
        {
            Visit(outerQuery);
            if (IsPostMaterializeTailMode
                && CurrentPostMaterializeElementType == outerKeySel.Parameters[0].Type)
            {
                AppendPostMaterializeGroupJoin(innerQuery, outerKeySel, innerKeySel, groupJoinResultSelector);
                AppendPostMaterializeSelectMany(collectionSelector, smResultSelector);
                return;
            }
            outerKeySel = ExpandProjection(outerKeySel);
            var outerMapping = _mapping;
            var outerAlias = EscapeAlias("T0");
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);
            var innerAlias = EscapeAlias("T" + (++_joinCounter));

            if (!_correlatedParams.ContainsKey(outerKeySel.Parameters[0]))
                _correlatedParams[outerKeySel.Parameters[0]] = (outerMapping, outerAlias);
            // Composite (anonymous-type) keys: per-member equalities ANDed into the ON clause,
            // string members ordinal-wrapped — mirrors HandleInnerJoin / HandleGroupJoin.
            string? ljCompositeOnSql = null;
            if (outerKeySel.Body is NewExpression ljOuterComposite
                && innerKeySel.Body is NewExpression ljInnerComposite
                && ljOuterComposite.Arguments.Count == ljInnerComposite.Arguments.Count
                && ljOuterComposite.Arguments.Count > 0)
            {
                if (!_correlatedParams.ContainsKey(innerKeySel.Parameters[0]))
                    _correlatedParams[innerKeySel.Parameters[0]] = (innerMapping, innerAlias);
                var ljParts = new List<string>(ljOuterComposite.Arguments.Count);
                for (int ci = 0; ci < ljOuterComposite.Arguments.Count; ci++)
                {
                    var vctxLo = new VisitorContext(_ctx, outerMapping, _provider, outerKeySel.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var loVisitor = FastExpressionVisitorPool.Get(in vctxLo);
                    var outerMemberSql = loVisitor.Translate(ljOuterComposite.Arguments[ci]);
                    foreach (var kvp in loVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(loVisitor);

                    var vctxLi = new VisitorContext(_ctx, innerMapping, _provider, innerKeySel.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
                    var liVisitor = FastExpressionVisitorPool.Get(in vctxLi);
                    var innerMemberSql = liVisitor.Translate(ljInnerComposite.Arguments[ci]);
                    foreach (var kvp in liVisitor.GetParameters())
                        AddLiteralParameter(kvp.Key, kvp.Value);
                    FastExpressionVisitorPool.Return(liVisitor);

                    ljParts.Add(JoinBuilder.BuildOnEquality(
                        outerMemberSql, innerMemberSql, _provider, ljOuterComposite.Arguments[ci].Type));
                }
                ljCompositeOnSql = string.Join(" AND ", ljParts);
            }

            var vctxOuter = new VisitorContext(_ctx, outerMapping, _provider, outerKeySel.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var outerKeyVisitor = FastExpressionVisitorPool.Get(in vctxOuter);
            var outerKeySql = ljCompositeOnSql != null ? "1" : outerKeyVisitor.Translate(outerKeySel.Body);
            // See HandleJoin: AddLiteralParameter for inline-constant key fragments.
            foreach (var kvp in outerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(outerKeyVisitor);

            if (!_correlatedParams.ContainsKey(innerKeySel.Parameters[0]))
                _correlatedParams[innerKeySel.Parameters[0]] = (innerMapping, innerAlias);
            var vctxInner = new VisitorContext(_ctx, innerMapping, _provider, innerKeySel.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var innerKeyVisitor = FastExpressionVisitorPool.Get(in vctxInner);
            var innerKeySql = ljCompositeOnSql != null ? "1" : innerKeyVisitor.Translate(innerKeySel.Body);
            foreach (var kvp in innerKeyVisitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(innerKeyVisitor);

            // Rewrite the SelectMany result selector so its body uses fresh outer/inner entity
            // parameters that JoinBuilder can substitute for the join aliases.
            var outerEntityType = outerMapping.Type;
            var freshOuter = Expression.Parameter(outerEntityType, "__lj_outer");
            var freshInner = Expression.Parameter(innerElementType, "__lj_inner");
            var rewriter = new QuerySyntaxLeftJoinRewriter(
                smResultSelector.Parameters[0], smResultSelector.Parameters[1],
                outerMemberName, freshOuter, freshInner);
            var newBody = rewriter.Visit(smResultSelector.Body)!;
            var rewrittenResultSel = Expression.Lambda(newBody, freshOuter, freshInner);

            JoinBuilder.SetupJoinProjection(rewrittenResultSel, outerMapping, innerMapping, outerAlias, innerAlias, _correlatedParams, ref _projection);

            // Inner-source WHERE conditions (explicit filters, or the injected
            // soft-delete/tenant predicates on the inner root) belong in the JOIN ON
            // clause — ON, not WHERE, so a filtered-out inner row reads as UNMATCHED
            // and the LEFT JOIN keeps the outer row with a NULL inner instead of
            // leaking the filtered row into the flattened result.
            var ljInnerOnConditions = ExtractInnerWhereConditions(innerQuery, innerMapping, innerAlias);
            var ljAdditionalOnSql = ljInnerOnConditions.Count > 0
                ? string.Join(" AND ", ljInnerOnConditions.Select(c => "(" + c + ")"))
                : null;

            _sql.Clear();
            JoinBuilder.BuildJoinClauseInto(
                _sql,
                _projection,
                outerMapping,
                outerAlias,
                innerMapping,
                innerAlias,
                "LEFT JOIN",
                outerKeySql,
                innerKeySql,
                distinct: _isDistinct,
                additionalOnConditions: ljAdditionalOnSql,
                translateProjectionExpression: TranslateJoinProjectionExpression,
                escapeProjectionAlias: _provider.Escape,
                provider: _provider,
                keyClrType: outerKeySel.Body.Type,
                onSqlOverride: ljCompositeOnSql);
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class QuerySyntaxLeftJoinRewriter : ExpressionVisitor
        {
            private readonly ParameterExpression _oldTransparent;
            private readonly ParameterExpression _oldInner;
            private readonly string _outerMemberName;
            private readonly ParameterExpression _newOuter;
            private readonly ParameterExpression _newInner;

            public QuerySyntaxLeftJoinRewriter(
                ParameterExpression oldTransparent,
                ParameterExpression oldInner,
                string outerMemberName,
                ParameterExpression newOuter,
                ParameterExpression newInner)
            {
                _oldTransparent = oldTransparent;
                _oldInner = oldInner;
                _outerMemberName = outerMemberName;
                _newOuter = newOuter;
                _newInner = newInner;
            }

            protected override Expression VisitConditional(ConditionalExpression node)
            {
                // `c == null ? null : c.Prop` and mirrored `c != null ? c.Prop : null`:
                // Strip the null-guard entirely - LEFT JOIN already NULLs unmatched rows.
                // `c == null ? fallback : c.Prop` and mirrored `c != null ? c.Prop : fallback`
                // where fallback is a non-null constant: rewrite to COALESCE(c.Prop, fallback).
                // In SQL a LEFT JOIN null propagates through ALL right-side columns, so
                // COALESCE correctly substitutes the fallback for unmatched rows. The one
                // edge case where COALESCE differs from .NET: a matched row where c.Prop
                // happens to be NULL in the DB also gets the fallback in SQL, whereas .NET
                // would return null. That distinction is documented as an acceptable
                // approximation for non-nullable projected columns.
                if (IsInnerParam(node.Test, out bool testIsEqualParam))
                {
                    var propBranch = testIsEqualParam ? node.IfFalse : node.IfTrue;
                    var nullBranch = testIsEqualParam ? node.IfTrue  : node.IfFalse;
                    if (IsNullOrDefault(nullBranch))
                        return Visit(propBranch);
                    return BuildCoalesce(Visit(propBranch), Visit(nullBranch));
                }
                else if (IsEntityNullComparison(node.Test, out bool _, out bool eqOp))
                {
                    var propBranch = eqOp ? node.IfFalse : node.IfTrue;
                    var nullBranch = eqOp ? node.IfTrue  : node.IfFalse;
                    if (IsNullOrDefault(nullBranch))
                        return Visit(propBranch);
                    return BuildCoalesce(Visit(propBranch), Visit(nullBranch));
                }
                return base.VisitConditional(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression == _oldTransparent && node.Member.Name == _outerMemberName)
                    return _newOuter;
                return base.VisitMember(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                if (node == _oldInner) return _newInner;
                return base.VisitParameter(node);
            }

            private bool IsInnerParam(Expression e, out bool isEqual)
            {
                isEqual = false;
                return e == _oldInner || e == _newInner;
            }

            private bool IsEntityNullComparison(Expression test, out bool nullOnLeft, out bool isEqual)
            {
                nullOnLeft = false;
                isEqual = false;
                if (test is not BinaryExpression bin) return false;
                if (bin.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual)) return false;
                isEqual = bin.NodeType == ExpressionType.Equal;
                if ((bin.Left == _oldInner || bin.Left == _newInner)
                    && bin.Right is ConstantExpression { Value: null })
                { nullOnLeft = false; return true; }
                if ((bin.Right == _oldInner || bin.Right == _newInner)
                    && bin.Left is ConstantExpression { Value: null })
                { nullOnLeft = true; return true; }
                return false;
            }

            private static bool IsNullOrDefault(Expression e) =>
                e is ConstantExpression { Value: null }
                || e is DefaultExpression
                || (e is UnaryExpression ue
                    && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked)
                    && ue.Operand is ConstantExpression { Value: null });

            private static Expression BuildCoalesce(Expression valueExpression, Expression fallbackExpression)
            {
                var originalValueType = valueExpression.Type;
                var coalesceLeft = valueExpression;
                if (coalesceLeft.Type.IsValueType && Nullable.GetUnderlyingType(coalesceLeft.Type) == null)
                {
                    coalesceLeft = Expression.Convert(coalesceLeft, typeof(Nullable<>).MakeGenericType(coalesceLeft.Type));
                }

                var rightTargetType = Nullable.GetUnderlyingType(coalesceLeft.Type) ?? coalesceLeft.Type;
                var coalesceRight = ConvertIfNeeded(fallbackExpression, rightTargetType);
                var coalesce = Expression.Coalesce(coalesceLeft, coalesceRight);
                return coalesce.Type == originalValueType
                    ? coalesce
                    : ConvertIfNeeded(coalesce, originalValueType);
            }

            private static Expression ConvertIfNeeded(Expression expression, Type targetType)
            {
                if (expression.Type == targetType)
                    return expression;
                if (expression is ConstantExpression { Value: null })
                    return Expression.Constant(null, targetType);
                return Expression.Convert(expression, targetType);
            }
        }
    }
}
