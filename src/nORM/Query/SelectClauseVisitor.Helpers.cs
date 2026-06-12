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
        /// <summary>
        /// Determines if the expression represents a navigation property that is a collection
        /// (e.g., <c>ICollection&lt;T&gt;</c>, <c>List&lt;T&gt;</c>) rather than a mapped scalar column.
        /// </summary>
        private bool IsNavigationCollection(Expression expr, out PropertyInfo property)
        {
            property = null!;

            // Look for member access like "b.Posts"
            if (expr is MemberExpression memberExpr &&
                memberExpr.Member is PropertyInfo propInfo &&
                memberExpr.Expression is ParameterExpression)
            {
                var propType = propInfo.PropertyType;

                // Check if it's a generic collection type (IEnumerable<T> but not string,
                // which implements IEnumerable but is a scalar column type).
                if (propType != typeof(string) &&
                    typeof(IEnumerable).IsAssignableFrom(propType) &&
                    propType.IsGenericType)
                {
                    // Verify it's actually a navigation property (not a mapped column)
                    if (!_mapping.ColumnsByName.ContainsKey(propInfo.Name))
                    {
                        property = propInfo;
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Returns the active <see cref="StringBuilder"/>, throwing if called outside
        /// a <see cref="Translate"/> invocation.
        /// </summary>
        private StringBuilder EnsureBuilder() =>
            _sb ?? throw new InvalidOperationException("Cannot visit expressions outside of a Translate() call.");

        /// <summary>
        /// Folds a <see cref="TimeSpan"/> expression to its runtime value when it
        /// is either a constant, a static/closure member, or a call to one of the
        /// side-effect-free <c>TimeSpan.From*</c> factories with a constant arg.
        /// Used by the 7-arg <c>new DateTimeOffset(...)</c> handler whose offset
        /// arg must be a compile-time constant. ExpressionValueExtractor refuses
        /// MethodCallExpression by design (RCE prevention); the TimeSpan factories
        /// have a fixed, audited surface so we admit them here explicitly.
        /// </summary>
        private static bool TryGetTimeSpanConstant(Expression e, out TimeSpan value)
        {
            value = default;
            if (ExpressionValueExtractor.TryGetConstantValue(e, out var box) && box is TimeSpan ts)
            {
                value = ts;
                return true;
            }
            if (e is MethodCallExpression mc
                && mc.Object == null
                && mc.Method.DeclaringType == typeof(TimeSpan)
                && mc.Arguments.Count == 1
                && ExpressionValueExtractor.TryGetConstantValue(mc.Arguments[0], out var argBox)
                && argBox != null)
            {
                try
                {
                    double d = Convert.ToDouble(argBox, System.Globalization.CultureInfo.InvariantCulture);
                    switch (mc.Method.Name)
                    {
                        case nameof(TimeSpan.FromDays):         value = TimeSpan.FromDays(d);         return true;
                        case nameof(TimeSpan.FromHours):        value = TimeSpan.FromHours(d);        return true;
                        case nameof(TimeSpan.FromMinutes):      value = TimeSpan.FromMinutes(d);      return true;
                        case nameof(TimeSpan.FromSeconds):      value = TimeSpan.FromSeconds(d);      return true;
                        case nameof(TimeSpan.FromMilliseconds): value = TimeSpan.FromMilliseconds(d); return true;
                        case nameof(TimeSpan.FromTicks):        value = new TimeSpan((long)d);        return true;
                    }
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }

        private static string GetTableName(Type type)
        {
            var tableAttribute = type
                .GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), inherit: false)
                .Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>()
                .FirstOrDefault();

            if (tableAttribute is null)
                return type.Name;

            return string.IsNullOrWhiteSpace(tableAttribute.Schema)
                ? tableAttribute.Name
                : tableAttribute.Schema + "." + tableAttribute.Name;
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}
