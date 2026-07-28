using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class SelectClauseVisitor
    {
        private string RenderNavigationFilter(LambdaExpression filter, string depAlias)
            => RenderNavigationFilterBody(filter.Body, filter.Parameters[0], depAlias);

        /// <summary>
        /// Null-aware negation of a navigation-filter predicate, mirroring the WHERE-side EmitNegation so a
        /// projected All / negated filter follows SQL three-valued logic instead of a bare NOT (which stays
        /// UNKNOWN for a NULL operand and drops the row). Pushes the negation to the leaves: De Morgan for
        /// &amp;&amp;/||, flips ==/!= back through the (null-safe) positive path, and for a relational comparison
        /// emits NOT(a op b) OR &lt;nullable operand&gt; IS NULL. Uses the grammar's own RenderFilterSide so column
        /// aliasing/converters stay consistent with the positive rendering.
        /// </summary>
        private string RenderNavigationFilterBodyNegated(Expression body, ParameterExpression elementParam, string depAlias)
        {
            body = StripBoolConvertForNegation(body);
            switch (body)
            {
                case UnaryExpression { NodeType: ExpressionType.Not } inner: // !!x == x
                    return RenderNavigationFilterBody(StripBoolConvertForNegation(inner.Operand), elementParam, depAlias);
                case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                    return $"({RenderNavigationFilterBodyNegated(and.Left, elementParam, depAlias)} OR {RenderNavigationFilterBodyNegated(and.Right, elementParam, depAlias)})";
                case BinaryExpression { NodeType: ExpressionType.OrElse } orElse:
                    return $"({RenderNavigationFilterBodyNegated(orElse.Left, elementParam, depAlias)} AND {RenderNavigationFilterBodyNegated(orElse.Right, elementParam, depAlias)})";
                // Flip through the positive path: it lowers `== null`/`!= null` to IS [NOT] NULL and expands a
                // value `!=` to the null-safe 3-term form, so the negations inherit correct 3VL.
                case BinaryExpression { NodeType: ExpressionType.Equal } eq:
                    return RenderNavigationFilterBody(Expression.NotEqual(eq.Left, eq.Right), elementParam, depAlias);
                case BinaryExpression { NodeType: ExpressionType.NotEqual } ne:
                    return RenderNavigationFilterBody(Expression.Equal(ne.Left, ne.Right), elementParam, depAlias);
                case BinaryExpression
                {
                    NodeType: ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                        or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                } rel:
                    var negated = "(NOT (" + RenderNavigationFilterBody(rel, elementParam, depAlias) + ")";
                    if (NavOperandCouldBeNull(rel.Left))
                        negated += " OR " + RenderFilterSide(rel.Left, elementParam, depAlias) + " IS NULL";
                    if (NavOperandCouldBeNull(rel.Right))
                        negated += " OR " + RenderFilterSide(rel.Right, elementParam, depAlias) + " IS NULL";
                    return negated + ")";
                default:
                    return "NOT (" + RenderNavigationFilterBody(body, elementParam, depAlias) + ")";
            }
        }

        private static bool NavOperandCouldBeNull(Expression e)
        {
            if (e is ConstantExpression { Value: not null })
                return false;
            var t = e.Type;
            return !t.IsValueType || Nullable.GetUnderlyingType(t) != null;
        }

        // Renders a predicate against the dependent alias. Recursive so it covers the common global
        // filter shapes (soft-delete `!c.IsDeleted`, a bare boolean flag, and `&&`/`||` compositions)
        // in addition to the simple `c.X op constant` comparisons.
        private string RenderNavigationFilterBody(Expression body, ParameterExpression elementParam, string depAlias)
        {
            switch (body)
            {
                case UnaryExpression { NodeType: ExpressionType.Not } notExpr:
                    return RenderNavigationFilterBodyNegated(notExpr.Operand, elementParam, depAlias);

                // Bare boolean member (`c.IsActive`) → `alias.IsActive = <true>`.
                case MemberExpression boolMember when boolMember.Type == typeof(bool) && boolMember.Expression == elementParam:
                    return $"{RenderFilterSide(boolMember, elementParam, depAlias)} = {_provider.BooleanTrueLiteral}";

                // String match on a column (`c.Sku.StartsWith("A")`, `.EndsWith(...)`, `.Contains(...)`) with
                // a constant pattern — shares the provider case-sensitivity/LIKE-escaping logic with the
                // projection path so the subquery filter matches the same rows the outer query would.
                case MethodCallExpression stringMatch when TryRenderNavStringMatch(stringMatch, elementParam, depAlias, out var matchSql):
                    return matchSql;

                case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } logical:
                    var logicalOp = logical.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
                    return $"({RenderNavigationFilterBody(logical.Left, elementParam, depAlias)} {logicalOp} {RenderNavigationFilterBody(logical.Right, elementParam, depAlias)})";

                // Null comparison (`c.DeletedAt == null` / `!= null`) must lower to IS [NOT] NULL — the
                // general `col = NULL` form below is always unknown, so the whole subquery filter would match
                // nothing (silently dropping every child, e.g. for a `DeletedAt == null` soft-delete filter).
                case BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } nullCmp
                    when IsNullConstant(nullCmp.Left) || IsNullConstant(nullCmp.Right):
                    var nullOperand = IsNullConstant(nullCmp.Left) ? nullCmp.Right : nullCmp.Left;
                    var nullTest = nullCmp.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL";
                    return $"{RenderFilterSide(nullOperand, elementParam, depAlias)} {nullTest}";

                case BinaryExpression be:
                    // If one side is a value-converter column, the OTHER side's value must be converted to the
                    // provider representation, or `col = <rawModelValue>` compares against the wrong stored
                    // value and matches nothing (silent-wrong). Non-converter columns pass null → unchanged.
                    var leftColConv = ColumnConverterFor(be.Left, elementParam);
                    var rightColConv = ColumnConverterFor(be.Right, elementParam);
                    var lhs = RenderFilterSide(be.Left, elementParam, depAlias, valueConverter: rightColConv);
                    var rhs = RenderFilterSide(be.Right, elementParam, depAlias, valueConverter: leftColConv);
                    // decimal / TimeSpan / DateTimeOffset are stored as TEXT on SQLite; a raw `col op literal`
                    // then compares LEXICALLY ('9' > '50', '100' = '100.0' misses), silently including/dropping
                    // the wrong children. Coerce both operands the same way the main comparison visitors do
                    // (ExpressionToSqlVisitor.Binary / SelectClauseVisitor.Operators): numeric for relational,
                    // canonical exact-key for ==/!=. Converter columns are already handled above (the opposing
                    // operand is bound in the provider representation), so those are skipped. Identity on native
                    // providers.
                    if (leftColConv == null && rightColConv == null)
                    {
                        var lt = Nullable.GetUnderlyingType(be.Left.Type) ?? be.Left.Type;
                        var rt = Nullable.GetUnderlyingType(be.Right.Type) ?? be.Right.Type;
                        Type? navCmpType =
                            (lt == typeof(decimal) || rt == typeof(decimal)) ? typeof(decimal)
                            : (lt == typeof(TimeSpan) || rt == typeof(TimeSpan)) ? typeof(TimeSpan)
                            : (lt == typeof(DateTimeOffset) || rt == typeof(DateTimeOffset)) ? typeof(DateTimeOffset)
                            : null;
                        if (navCmpType != null)
                        {
                            bool exact = be.NodeType is ExpressionType.Equal or ExpressionType.NotEqual;
                            lhs = CoerceNavCompareOperand(lhs, navCmpType, exact);
                            rhs = CoerceNavCompareOperand(rhs, navCmpType, exact);
                        }
                    }
                    if (be.NodeType == ExpressionType.NotEqual)
                    {
                        // C# `a != b` is true when EXACTLY ONE side is NULL, or both are non-NULL and unequal —
                        // NOT the bare SQL `a <> b`, which is UNKNOWN (dropped) whenever either side is NULL, and
                        // (crucially) which must stay FALSE when BOTH are NULL. Expand to match, rescuing only the
                        // operands that can actually be NULL.
                        var neTerms = new List<string> { $"{lhs} <> {rhs}" };
                        if (NavOperandCouldBeNull(be.Left)) neTerms.Add($"({lhs} IS NULL AND {rhs} IS NOT NULL)");
                        if (NavOperandCouldBeNull(be.Right)) neTerms.Add($"({rhs} IS NULL AND {lhs} IS NOT NULL)");
                        return neTerms.Count == 1 ? neTerms[0] : "(" + string.Join(" OR ", neTerms) + ")";
                    }
                    var op = be.NodeType switch
                    {
                        ExpressionType.Equal => "=",
                        ExpressionType.NotEqual => "<>",
                        ExpressionType.GreaterThan => ">",
                        ExpressionType.GreaterThanOrEqual => ">=",
                        ExpressionType.LessThan => "<",
                        ExpressionType.LessThanOrEqual => "<=",
                        _ => throw new InvalidOperationException(
                            $"Navigation filter binary operator '{be.NodeType}' isn't yet supported in a projection subquery. " +
                            "Use a simple comparison (==, !=, <, >, <=, >=, &&, ||) or wrap with `ClientEvaluationPolicy.Allow`.")
                    };
                    return $"{lhs} {op} {rhs}";

                default:
                    throw new InvalidOperationException(
                        "Navigation filter inside a projection subquery supports comparisons (==, !=, <, >, <=, >=), " +
                        "`IS [NOT] NULL` (`== null` / `!= null`), `StartsWith`/`EndsWith`/`Contains` with a constant " +
                        "pattern, enum comparisons, bare boolean flags, `!`, and `&&`/`||` compositions. Shapes like " +
                        "`list.Contains(col)` (IN) and comparisons on value-converter columns aren't supported here " +
                        "yet — filter after materialization, or wrap with `ClientEvaluationPolicy.Allow`.");
            }
        }

        /// <summary>
        /// Renders a string <c>StartsWith</c>/<c>EndsWith</c>/<c>Contains</c> call on a column of the filter
        /// element with a CONSTANT pattern (optionally with a StringComparison) into a navigation-filter
        /// predicate, reusing <see cref="EmitStringMatch"/>. Returns false for any other shape (variable
        /// pattern, non-element receiver) so the caller falls through to the unsupported-shape error.
        /// </summary>
        /// <summary>
        /// The value converter of the filter-element column the expression accesses, or null when the
        /// expression isn't such a column, has no converter, or can't be resolved. Used to convert the
        /// opposing operand of a comparison to the provider representation.
        /// </summary>
        private IValueConverter? ColumnConverterFor(Expression expr, ParameterExpression elementParam)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            if (_ctx == null || expr is not MemberExpression me || me.Expression != elementParam
                || me.Member.DeclaringType == null)
                return null;
            try
            {
                var map = _ctx.GetMapping(me.Member.DeclaringType);
                return map.ColumnsByName.TryGetValue(me.Member.Name, out var col) ? col.Converter : null;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>True when the expression is a null literal (possibly wrapped in a nullable Convert).</summary>
        private static bool IsNullConstant(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e is ConstantExpression { Value: null };
        }

        private bool TryRenderNavStringMatch(MethodCallExpression mc, ParameterExpression elementParam, string depAlias, out string sql)
        {
            sql = string.Empty;
            if (mc.Object is not MemberExpression receiver || receiver.Expression != elementParam)
                return false;
            if (mc.Method.DeclaringType != typeof(string)
                || mc.Method.Name is not (nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains)))
                return false;
            if (!(mc.Arguments.Count == 1 || (mc.Arguments.Count == 2 && mc.Arguments[1].Type == typeof(StringComparison))))
                return false;
            if (!QueryTranslator.TryGetConstantValue(mc.Arguments[0], out var rawPattern) || !(rawPattern is string || rawPattern is char))
                return false;

            var patternStr = rawPattern as string ?? ((char)rawPattern!).ToString();
            var ignoreCase = mc.Arguments.Count == 2
                && QueryTranslator.TryGetConstantValue(mc.Arguments[1], out var cmpVal)
                && cmpVal is StringComparison.OrdinalIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase;

            sql = EmitStringMatch(RenderFilterSide(receiver, elementParam, depAlias), patternStr, mc.Method.Name, ignoreCase);
            return true;
        }

        private string RenderFilterSide(Expression expr, ParameterExpression elementParam, string depAlias, IValueConverter? valueConverter = null)
        {
            // Peel the enum→underlying Convert the compiler inserts around either operand of an enum
            // comparison (`l.EnumCol == EnumValue`, or `== capturedEnum`) so the plain column / captured
            // value is rendered. A value-converter enum column is fine now: the BinaryExpression case applies
            // the column's converter to the opposing operand, so the comparison uses the stored provider
            // representation rather than the raw underlying number.
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } enumConv
                && (Nullable.GetUnderlyingType(enumConv.Operand.Type) ?? enumConv.Operand.Type).IsEnum)
            {
                expr = enumConv.Operand;
            }

            // Member access on the element parameter → column on the dependent. Resolve the mapped
            // column through the TableMapping (like the selector path RenderElementColumnSql) so a fluent
            // HasColumnName override is honoured — not only a [Column] attribute, which is all the member's
            // own metadata carries. Otherwise a fluently renamed filter column emits `alias.<PropName>` for
            // a column that does not exist. The attribute/property-name path remains the fallback when the
            // mapping isn't reachable, and is byte-identical to the mapping's EscCol in the un-renamed case.
            if (expr is MemberExpression me && me.Expression == elementParam)
            {
                if (_ctx != null && me.Member.DeclaringType != null)
                {
                    try
                    {
                        if (_ctx.GetMapping(me.Member.DeclaringType).ColumnsByName.TryGetValue(me.Member.Name, out var mappedCol))
                            return $"{depAlias}.{mappedCol.EscCol}";
                    }
                    catch { /* fall through to attribute/property-name resolution */ }
                }
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                return $"{depAlias}.{_provider.Escape(colName)}";
            }
            // Inline constants are plan-cache-safe literals (the constant's identity is part of
            // the expression fingerprint). When compared against a value-converter column, the constant is
            // converted to the provider representation so `col = <converted>` matches the stored value.
            if (expr is ConstantExpression ce)
                return FormatLiteral(valueConverter != null ? valueConverter.ConvertToProvider(ce.Value) : ce.Value);
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand is ConstantExpression ce2)
                return FormatLiteral(valueConverter != null ? valueConverter.ConvertToProvider(ce2.Value) : ce2.Value);
            // CLOSURE captures must NOT literal-ize: plans are cached by fingerprint, so a baked
            // value would freeze the first run's filter into every later run. Emit a compiled
            // parameter through the shared channel when available (same contract as the main
            // projection closures); fall back to the literal only for channel-less fragment uses.
            if (expr is MemberExpression closureMe && QueryTranslator.TryGetConstantValue(closureMe, out var closureVal))
            {
                if (SharedParams != null && SharedCompiledParams != null)
                {
                    var reused = QueryTranslator.TryReuseClosureSlot(closureMe);
                    if (reused != null)
                        return reused;
                    var paramName = $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}";
                    SharedParams[paramName] = DBNull.Value;
                    SharedCompiledParams.Add(paramName);
                    QueryTranslator.RecordClosureSlot(closureMe, paramName);
                    // Compared against a value-converter column: record the converter so the extractor binds
                    // the provider representation of the captured value, not the raw model value.
                    if (valueConverter != null && SharedParamConverters != null)
                        SharedParamConverters[paramName] = valueConverter;
                    return paramName;
                }
                return FormatLiteral(closureVal);
            }
            throw new InvalidOperationException(
                $"Navigation filter side '{expr}' isn't a simple member access or constant — only `c.X op constant` is supported in a projection subquery.");
        }

        /// <summary>
        /// Coerces a navigation-filter comparison operand for a TEXT-stored decimal / TimeSpan /
        /// DateTimeOffset column so the comparison is numeric (relational) or canonical (equality)
        /// rather than lexical. Mirrors the main comparison visitors; identity on native providers.
        /// </summary>
        private string CoerceNavCompareOperand(string sql, Type cmpType, bool exact)
        {
            if (cmpType == typeof(decimal))
                // A decimal CONSTANT renders here as a bare numeric literal (FormatLiteral), not the
                // TEXT-bound parameter the main path canonicalizes; normalize both operands to TEXT first
                // so the canonical-text exact key compares like-for-like (column is already TEXT, so the
                // CAST is a no-op there). Relational keeps the numeric REAL coercion.
                return exact ? _provider.ExactDecimalKeySql($"CAST({sql} AS TEXT)") : _provider.NormalizeDecimalForCompare(sql);
            if (cmpType == typeof(TimeSpan))
                return exact ? (_provider.CanonicalTimeSpanTextForExactCompare(sql) ?? sql) : _provider.NormalizeTimeSpanForCompare(sql);
            if (cmpType == typeof(DateTimeOffset))
                return _provider.NormalizeDateTimeOffsetForCompare(sql);
            return sql;
        }

        private string FormatLiteral(object? value)
        {
            // Enums lower to their underlying integer so HasFlag / equality
            // projections work with closure-captured flag locals -- without this
            // the closure-fold path emits the enum boxed and FormatLiteral
            // threw "type '<EnumName>' isn't supported".
            if (value is Enum e)
                value = Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), System.Globalization.CultureInfo.InvariantCulture);
            return value switch
            {
                null => "NULL",
                bool b => b ? "1" : "0",
                string s => _provider.EscapeStringLiteral(s),
                int or long or short or byte or sbyte or uint or ulong or ushort => value.ToString()!,
                double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
                float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
                decimal m => m.ToString(System.Globalization.CultureInfo.InvariantCulture),
                // DateTime/DateTimeOffset/DateOnly/TimeOnly/TimeSpan/Guid -- emit
                // a single-quoted text literal matching the canonical format
                // Microsoft.Data.Sqlite uses for parameter binding, so the
                // result round-trips through the materializer. DateTime uses
                // 'yyyy-MM-dd HH:mm:ss.FFFFFFF' (variable trailing zeros).
                DateTime dt => $"'{dt.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", System.Globalization.CultureInfo.InvariantCulture)}'",
                DateTimeOffset dto => $"'{dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", System.Globalization.CultureInfo.InvariantCulture)}'",
                DateOnly d => $"'{d.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)}'",
                TimeOnly t => $"'{t.ToString("HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture)}'",
                TimeSpan ts => $"'{ts.ToString("c", System.Globalization.CultureInfo.InvariantCulture)}'",
                Guid g => $"'{g.ToString("D", System.Globalization.CultureInfo.InvariantCulture)}'",
                // CultureInfo / IFormatProvider arguments to ParseExact /
                // TryParse / ToString carry no SQL representation -- the
                // provider's TranslateMethodCall doesn't consume them. Emit
                // NULL so the per-arg projection visit doesn't blow up; the
                // arg never reaches the SQL output because the overload-aware
                // handler ignores it.
                System.Globalization.CultureInfo => "NULL",
                System.IFormatProvider => "NULL",
                _ => throw new InvalidOperationException(
                    $"Navigation filter literal of type '{value.GetType().Name}' isn't supported in a projection subquery. " +
                    "Use int/long/short/byte/string/bool/double/decimal/DateTime/DateTimeOffset/DateOnly/TimeOnly/TimeSpan/Guid, " +
                    "or wrap with `ClientEvaluationPolicy.Allow`.")
            };
        }

        /// <summary>
        /// Fails loud (never silent-wrong) when a Sum/Average selector over a navigation collection is a
        /// value-converter column: SUM/AVG combine the STORED provider values, and ConvertFromProvider — a
        /// per-value map — does not distribute over that combination for a non-linear converter, so there is
        /// no correct scalar result. (Min/Max ARE supported: they return one stored column value, which the
        /// materializer runs through ConvertFromProvider — same contract as the ctx.Query correlated path,
        /// with ordering/selection on the stored form; see IsNavigationScalarColumnOp.)
        /// </summary>
        private void GuardAggregateOverConverterColumn(string methodName, LambdaExpression selectorLambda, Type depType)
        {
            if (methodName is not (nameof(Queryable.Sum) or nameof(Queryable.Average))
                || _ctx == null)
                return;
            TableMapping depMap;
            try { depMap = _ctx.GetMapping(depType); }
            catch { return; }
            if (LambdaReferencesConverterColumn(selectorLambda, depMap))
                throw new NormUnsupportedFeatureException(
                    $"{methodName}(...) over a navigation collection cannot aggregate a value-converter column: " +
                    "SUM/AVG combine the stored provider values and ConvertFromProvider does not distribute over " +
                    "that combination for a non-linear converter, so there is no correct result. Materialise the " +
                    "collection and aggregate it client-side.",
                    NormUnsupportedReason.NavAggregateValueConverterColumn);
        }
    }
}
