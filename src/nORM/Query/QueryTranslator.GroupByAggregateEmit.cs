using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
/// <summary>Emits a SQL aggregate (SUM/AVG/MIN/MAX) over a group, honouring Where(...) filters wrapping the IGrouping source; unmatched-row branch is 0 for SUM, NULL for AVG/MIN/MAX.</summary>
        private string? EmitGroupAggregateWithOptionalFilter(
            MethodCallExpression methodCall,
            string alias,
            string sqlAgg,
            string unmatchedBranchSql,
            string? errorMessage)
        {
            var selectorArg = methodCall.Arguments.Count > 1 && StripQuotes(methodCall.Arguments[0]) is not LambdaExpression
                ? methodCall.Arguments[1]
                : methodCall.Arguments.Count > 0 ? methodCall.Arguments[0] : null;
            var selector = selectorArg != null ? StripQuotes(selectorArg) as LambdaExpression : null;
            if (selector == null)
            {
                if (errorMessage != null)
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, errorMessage));
                return null;
            }
            selector = ExpandGroupElementSelector(selector);
            // Also expand through a join / transparent-identifier or Select projection so an
            // aggregate over a JOINED member (g.Sum(x => x.innerMember)) rewrites x.innerMember
            // back to the join's inner param — which _correlatedParams maps to the inner alias.
            // No-op when neither is set (single-table grouping) or the types don't match.
            selector = ExpandProjection(selector);

            var selParam = selector.Parameters[0];
            if (!_correlatedParams.ContainsKey(selParam))
                _correlatedParams[selParam] = (_mapping, alias);
            // Same correlated-mapping preference as the group-key path (QueryTranslator.Aggregates.cs):
            // for a join/projection source the (expanded) selector param is already mapped to its own
            // side's alias; forcing (_mapping, alias) would override it with the inner alias.
            var (selMapping, selAlias) = _correlatedParams.TryGetValue(selParam, out var selCorr) ? selCorr : (_mapping, alias);

            var vctxSel = new VisitorContext(_ctx, selMapping, _provider, selParam, selAlias, _correlatedParams, _compiledParams, _paramConverters, _paramMap, _recursionDepth, _params.Count);
            var visitor = FastExpressionVisitorPool.Get(in vctxSel);
            var columnSql = visitor.Translate(selector.Body);
            foreach (var kvp in visitor.GetParameters())
                AddLiteralParameter(kvp.Key, kvp.Value);
            FastExpressionVisitorPool.Return(visitor);

            // Decimal aggregate selectors route through the provider's full-precision hook
            // (mirrors the top-level paths in QueryTranslator.Aggregates.cs): SQLite uses
            // registered decimal aggregates / collation-ordered MIN-MAX over the raw TEXT
            // operand, exact at full precision; providers with native DECIMAL emit the plain
            // aggregate. AverageAggregateOperand is identity for decimal.
            var selBodyType = Nullable.GetUnderlyingType(selector.Body.Type) ?? selector.Body.Type;
            bool decimalOperand = selBodyType == typeof(decimal);

            // C# Average over ints is a double; SQL Server's AVG(int) truncates to int, so its
            // provider hook casts integral operands to FLOAT (identity elsewhere). Mirrors the
            // top-level aggregate paths in QueryTranslator.Aggregates.cs.
            if (!decimalOperand && sqlAgg == "AVG")
                columnSql = _provider.AverageAggregateOperand(columnSql, selector.Body.Type);

            var whereFilter = ExtractAggregateSourceFilter(methodCall);
            if (whereFilter == null)
                return decimalOperand
                    ? _provider.DecimalAggregateSql(sqlAgg, columnSql)
                    : $"{sqlAgg}({columnSql})";

            // Rebind the filter lambda's parameter onto the selector's parameter so both
            // expressions reference the same group-element alias, then translate the
            // predicate against that alias and emit `AGG(CASE WHEN pred THEN sel ELSE
            // <unmatched> END)`.
            var reboundBody = new nORM.Internal.ParameterReplacer(whereFilter.Parameters[0], selector.Parameters[0]).Visit(whereFilter.Body)!;
            var reboundFilter = Expression.Lambda(reboundBody, selector.Parameters[0]);
            var predSql = TranslateGroupPredicateBody(reboundFilter, alias);
            var caseSql = $"CASE WHEN {predSql} THEN {columnSql} ELSE {unmatchedBranchSql} END";
            return decimalOperand
                ? _provider.DecimalAggregateSql(sqlAgg, caseSql)
                : $"{sqlAgg}({caseSql})";
        }

        private LambdaExpression ExpandGroupElementSelector(LambdaExpression selector)
        {
            if (_groupByElementSelector == null
                || selector.Parameters.Count != 1
                || selector.Parameters[0].Type != _groupByElementSelector.Body.Type)
            {
                return selector;
            }

            var body = new ParameterReplacer(selector.Parameters[0], _groupByElementSelector.Body).Visit(selector.Body)!;
            body = new ProjectionMemberReplacer().Visit(body)!;
            return Expression.Lambda(body, _groupByElementSelector.Parameters);
        }
    }
}
