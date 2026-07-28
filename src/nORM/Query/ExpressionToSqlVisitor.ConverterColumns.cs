using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        private static Expression StripConvert(Expression expr)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            return expr;
        }

        // Converts an enum model value to its underlying integer. Used to compare an enum-stored-as-string
        // column relationally by value (BuildStringToEnumCase maps the column's stored name back to its
        // ordinal) instead of by member name, for both baked constants and compiled/closure parameters.
        private sealed class EnumToOrdinalConverter : nORM.Mapping.IValueConverter
        {
            public static readonly EnumToOrdinalConverter Instance = new();
            public Type ModelType => typeof(Enum);
            public Type ProviderType => typeof(long);
            public object? ConvertToProvider(object? modelValue) => modelValue is null ? null : Convert.ToInt64(modelValue);
            public object? ConvertFromProvider(object? providerValue) => providerValue;
        }

        private bool TryGetConverterColumn(Expression expr, out Column column)
            => TryGetConverterColumn(expr, out column, out _);

        /// <summary>
        /// Returns the type that drives a stored-representation comparison/ordering decision for
        /// <paramref name="expr"/>. A value-converter column stores its PROVIDER type (a decimal/TimeSpan/
        /// DateTimeOffset stored as TEXT on SQLite needs numeric normalization), which is invisible to the
        /// CLR model type — so return the converter's provider type for a converter column, else the CLR type.
        /// </summary>
        internal Type EffectiveComparableType(Expression expr, Type clrType)
            => TryGetConverterColumn(expr, out var column) && column.Converter != null
                ? column.Converter.ProviderType
                : clrType;

        private bool TryGetConverterColumn(Expression expr, out Column column, out bool viaNavigation)
        {
            column = null!;
            viaNavigation = false;
            expr = StripConvert(expr);
            // A correlated subquery whose scalar result is a converter column —
            // ctx.Query<Child>()...Select(c => c.Status).First() or ...Max(c => c.Status).
            // The member-side emit (Visit) already lowers it to a scalar subquery; surfacing
            // the converter here makes the value side bind the provider representation instead
            // of the raw model value (which silently matches nothing).
            if (expr is MethodCallExpression && TryGetSubqueryConverterColumn(expr, out var subCol))
            {
                column = subCol;
                viaNavigation = true; // an empty subquery yields SQL NULL — keep != null-safe
                return true;
            }
            if (expr is not MemberExpression me)
                return false;
            // g.Key over a grouping whose key IS a single value-converter column: the synthetic "Key" member
            // does not resolve via TryGetColumnForMemberAccess, but the grouping registration carries the key's
            // column so a HAVING comparison (g.Key OP enumValue) binds the PROVIDER representation — matching the
            // WHERE path — instead of the model value against the stored text.
            if (me.Member.Name == "Key"
                && me.Expression is ParameterExpression gp
                && _groupingKeyColumns.TryGetValue(gp, out var groupKeyCol))
            {
                column = groupKeyCol;
                return true;
            }
            if (TableMapping.TryGetMemberAccessRoot(me, out var root)
                && _parameterMappings.TryGetValue(root, out var info)
                && info.Mapping.TryGetColumnForMemberAccess(me, out var col)
                && col.Converter != null)
            {
                column = col;
                return true;
            }
            // Navigation-member receiver (e.Dept.Status): the converter lives on the
            // PRINCIPAL's column. Only claim it when the chain actually resolves to a
            // scalar subquery, so the member-side emit is guaranteed to succeed.
            if (me.Expression is MemberExpression navExpr && _ctx != null)
            {
                var navType = System.Nullable.GetUnderlyingType(navExpr.Type) ?? navExpr.Type;
                if (navType.IsClass && navType != typeof(string))
                {
                    try
                    {
                        var principal = _ctx.GetMapping(navType);
                        if (principal.ColumnsByName.TryGetValue(me.Member.Name, out var navCol)
                            && navCol.Converter != null
                            && BuildReferenceNavigationScalarSql(navExpr, me.Member.Name, 0) != null)
                        {
                            column = navCol;
                            viaNavigation = true;
                            return true;
                        }
                    }
                    catch { }
                }
            }
            return false;
        }

        /// <summary>
        /// Recognizes a correlated First/FirstOrDefault/Min/Max subquery whose scalar result is a
        /// value-converter column, and returns that column. Sum/Average/Count are excluded — they
        /// yield a numeric aggregate, not the column's own converted type. Used so a comparison
        /// against such a subquery binds the other operand through the converter.
        /// </summary>
        private bool TryGetSubqueryConverterColumn(Expression expr, out Column column)
        {
            column = null!;
            if (_ctx == null || expr is not MethodCallExpression mce)
                return false;
            var isFirst = QueryTranslator.IsQueryRootedScalarFirst(mce);
            var isMinMax = mce.Method.DeclaringType == typeof(System.Linq.Queryable)
                && mce.Method.Name is nameof(System.Linq.Queryable.Min) or nameof(System.Linq.Queryable.Max)
                && QueryTranslator.IsQueryRootedScalarAggregate(mce);
            if (!isFirst && !isMinMax)
                return false;

            var (elementType, member) = ResolveSubqueryProjectedMember(mce);
            if (elementType == null || member == null)
                return false;
            try
            {
                var mapping = _ctx.GetMapping(elementType);
                if (mapping.TryGetColumnForMemberAccess(member, out var col) && col.Converter != null)
                {
                    column = col;
                    return true;
                }
            }
            catch { }
            return false;
        }

        /// <summary>
        /// Finds the single scalar member a correlated subquery projects: an explicit aggregate
        /// selector (<c>Max(c =&gt; c.Member)</c>) or the innermost <c>Select(c =&gt; c.Member)</c>
        /// in the source chain. Returns the member's declaring (element) type and the member access.
        /// </summary>
        private (Type?, MemberExpression?) ResolveSubqueryProjectedMember(MethodCallExpression mce)
        {
            if (mce.Arguments.Count > 1 && StripQuotes(mce.Arguments[1]) is LambdaExpression aggSel
                && aggSel.Parameters.Count == 1 && StripConvert(aggSel.Body) is MemberExpression aggMember)
                return (aggSel.Parameters[0].Type, aggMember);

            var current = mce.Arguments.Count > 0 ? mce.Arguments[0] : null;
            while (current is MethodCallExpression m)
            {
                if (m.Method.Name == nameof(System.Linq.Queryable.Select) && m.Arguments.Count == 2
                    && StripQuotes(m.Arguments[1]) is LambdaExpression sel
                    && sel.Parameters.Count == 1 && StripConvert(sel.Body) is MemberExpression selMember)
                    return (sel.Parameters[0].Type, selMember);
                if (m.Arguments.Count == 0) break;
                current = m.Arguments[0];
            }
            return (null, null);
        }

        private static ExpressionType FlipComparison(ExpressionType op) => op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op // Equal / NotEqual are symmetric
        };

        private bool TryEmitMappedBooleanPredicate(Expression expression, bool expectedValue)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            if (expression is UnaryExpression { NodeType: ExpressionType.Not } not)
                return TryEmitMappedBooleanPredicate(not.Operand, !expectedValue);

            if (expression.Type != typeof(bool) ||
                expression is not MemberExpression member ||
                !TableMapping.TryGetMemberAccessRoot(member, out var parameter) ||
                !_parameterMappings.TryGetValue(parameter, out var info) ||
                !info.Mapping.TryGetColumnForMemberAccess(member, out var boolColumn))
            {
                return false;
            }

            var columnSql = GetSql(member);
            if (boolColumn.Converter != null)
            {
                // A bool column with a value converter stores a non-boolean provider value (e.g. 'Y'/'N').
                // Compare against the CONVERTED representation of the expected boolean, not a raw TRUE/FALSE
                // literal, or the predicate matches nothing.
                var converted = boolColumn.Converter.ConvertToProvider(expectedValue);
                _sql.Append(columnSql).Append(" = ");
                AppendConstant(converted, converted?.GetType() ?? boolColumn.Converter.ProviderType);
                return true;
            }
            _sql.Append(_provider.FormatBooleanPredicate(columnSql, expectedValue));
            return true;
        }

        /// <summary>
        /// True when the operand is raw decimal storage -- a column member, an
        /// inline constant, or a compiled-query parameter -- whose SQL fragment
        /// renders the stored TEXT (or its parameter). Computed expressions
        /// (Truncate, arithmetic) render numeric storage classes instead, and
        /// the canonical-text equality form must not apply to them.
        /// </summary>
        private static bool IsRawStorageOperand(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e is MemberExpression or ConstantExpression or ParameterExpression;
        }
    }
}
