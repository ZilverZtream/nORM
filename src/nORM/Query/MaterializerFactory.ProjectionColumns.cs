using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;
using System.Globalization;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        /// <summary>
        /// Extracts the set of columns referenced by a projection expression.
        /// </summary>
        /// <remarks>
        /// <b>Limitations:</b> This method only handles two expression node types inside
        /// <see cref="NewExpression.Arguments"/>:
        /// <list type="bullet">
        ///   <item><see cref="MemberExpression"/> -- direct property access (e.g. <c>x.Name</c>).</item>
        ///   <item><see cref="ParameterExpression"/> -- the entire entity passed as a constructor arg.</item>
        /// </list>
        /// Other expression forms (method calls, conditional expressions, binary expressions, etc.)
        /// are silently skipped, causing the resulting column array to omit those members. If the
        /// projection body is not a <see cref="NewExpression"/> at all, the method falls back to
        /// returning all columns from the mapping. Callers that need richer projection support
        /// should extend this method accordingly.
        /// </remarks>
        private static Column[] ExtractColumnsFromProjection(TableMapping mapping, LambdaExpression projection, IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? projectionSubqueryConverters = null)
        {
            // MemberInit: `new TDto { A = r.A, B = r.B, ... }`. The MemberAssignment targets are
            // properties on the DTO; build a Column per assignment whose Setter binds to the DTO
            // property and whose Name matches the column we expect in the result row.
            if (projection.Body is MemberInitExpression memberInit)
            {
                var cols = new List<Column>(memberInit.Bindings.Count);
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment ma && ma.Member is PropertyInfo dtoProp)
                    {
                        // Navigation collections are populated by the dependent-query / split-query
                        // pipeline rather than read from the row, so they must not appear as
                        // projection columns. Covers bare (o.Lines) and shaped (o.Lines.Where(p).ToList())
                        // bindings alike.
                        if (IsShapedOrBareNavigationCollection(ma.Expression, mapping))
                        {
                            continue;
                        }
                        var dtoCol = new Column(dtoProp, mapping.Provider, null);
                        // Carry the source column's value converter onto the projected column so the
                        // materializer applies ConvertFromProvider — the Column(PropertyInfo,...) ctor
                        // leaves Converter null, which would silently project the raw stored value.
                        if (ma.Expression is MemberExpression srcMember
                            && mapping.TryGetColumnForMemberAccess(srcMember, out var srcCol)
                            && srcCol.Converter != null)
                        {
                            dtoCol.Converter = srcCol.Converter;
                        }
                        // A correlated subquery (First/Last/Min/Max) over a converter column: the
                        // caller resolved the element mapping's converter for this member.
                        else if (projectionSubqueryConverters != null
                            && projectionSubqueryConverters.TryGetValue(dtoProp.Name, out var subConv))
                        {
                            dtoCol.Converter = subConv;
                        }
                        cols.Add(dtoCol);
                    }
                }
                return cols.ToArray();
            }
            if (projection.Body is NewExpression newExpr)
            {
                var cols = new List<Column>(newExpr.Arguments.Count);
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is MemberExpression m)
                    {
                        // Skip navigation collections - they'll be populated by split queries
                        if (IsNavigationCollection(m, mapping))
                        {
                            continue;
                        }

                        // IGrouping<TK, TE>.Key - read-only, no setter. Project as a shadow
                        // column named after the anonymous-type member so the materializer
                        // reads the group-key column without trying to bind a setter to the
                        // IGrouping.Key property (which has none).
                        if (m.Expression is ParameterExpression pep
                            && pep.Type.IsGenericType
                            && pep.Type.GetGenericTypeDefinition() == typeof(System.Linq.IGrouping<,>)
                            && m.Member.Name == "Key")
                        {
                            var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                            var keyCol = new Column(memberName, m.Type, mapping.Type, mapping.Provider, memberName);
                            // The grouped key column stores its provider representation (e.g. an enum
                            // as its name). Without ConvertFromProvider the materializer reads the raw
                            // string as the enum's underlying integer and throws. Resolve the key's
                            // converter by its model type — safe for an enum key because a second
                            // column of the exact same enum type (and a different converter) is not a
                            // real schema, and the key type is part of the materializer cache key.
                            keyCol.Converter = ResolveGroupKeyConverter(mapping, m.Type);
                            cols.Add(keyCol);
                            continue;
                        }

                        // Try to resolve against the current mapping first
                        if (mapping.TryGetColumnForMemberAccess(m, out var col))
                        {
                            cols.Add(col);
                        }
                        else if (m.Member is PropertyInfo pi && pi.GetSetMethod() != null)
                        {
                            // Create a lightweight column for writable properties from other
                            // mappings. Read-only properties cannot be bound by the setter-based
                            // materializer; project them as shadow columns instead.
                            cols.Add(new Column(pi, mapping.Provider, null));
                        }
                        else if (m.Member is PropertyInfo pi2)
                        {
                            var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                            cols.Add(new Column(memberName, pi2.PropertyType, mapping.Type, mapping.Provider, memberName));
                        }
                        else
                        {
                            // Closure-captured local: compiler-generated DisplayClass
                            // exposes locals as FIELDS, not properties. SCV.FormatLiteral
                            // emits the canonical text for the value; reserve a column
                            // slot here so the anonymous-type ctor lookup matches arity.
                            // Covers DateTime/Guid/TimeSpan/etc constants from closures.
                            var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                            cols.Add(new Column(memberName, m.Type, mapping.Type, mapping.Provider, memberName));
                        }
                    }
                    else if (arg is ParameterExpression p)
                    {
                        // A bare key parameter from the rewritten 3-arg GroupBy `(k, gs) => new { k, ... }`
                        // stands in for the group key; apply the key column's converter (enum keys only)
                        // so the model value is materialized, mirroring the g.Key member branch above.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        var keyParamCol = new Column(memberName, p.Type, mapping.Type, mapping.Provider, memberName);
                        keyParamCol.Converter = ResolveGroupKeyConverter(mapping, p.Type);
                        cols.Add(keyParamCol);
                    }
                    else if (arg is MethodCallExpression mce)
                    {
                        // Grouping aggregates (g.Count(), g.Sum(...), etc.) and other server-side
                        // computed expressions: project as a shadow column named after the
                        // anonymous-type member, typed as the call's return type.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        var mceCol = new Column(memberName, mce.Type, mapping.Type, mapping.Provider, memberName);
                        // A correlated subquery (First/Last/Min/Max) over a converter column: apply
                        // the element mapping's converter the caller resolved, so the materializer
                        // maps the provider value back instead of blindly casting it (e.g. an enum
                        // stored as its name would otherwise throw a FormatException).
                        if (projectionSubqueryConverters != null
                            && projectionSubqueryConverters.TryGetValue(memberName, out var mceConv))
                        {
                            mceCol.Converter = mceConv;
                        }
                        cols.Add(mceCol);
                    }
                    else if (arg is ConditionalExpression ce)
                    {
                        // (cond ? a : b) translates to CASE WHEN ... END server-side.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        cols.Add(new Column(memberName, ce.Type, mapping.Type, mapping.Provider, memberName));
                    }
                    else if (arg is BinaryExpression be)
                    {
                        // Arithmetic / string concat / etc.: project the computed value.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        cols.Add(new Column(memberName, be.Type, mapping.Type, mapping.Provider, memberName));
                    }
                    else if (arg is UnaryExpression ue
                             && (ue.NodeType == ExpressionType.Convert
                                 || ue.NodeType == ExpressionType.ConvertChecked
                                 // Unary minus / boolean NOT / bitwise NOT (~) produce a
                                 // computed scalar of the operand's underlying type. SCV
                                 // emits the operator-wrapped SQL; the materializer just
                                 // needs a column slot for the result.
                                 || ue.NodeType == ExpressionType.Negate
                                 || ue.NodeType == ExpressionType.NegateChecked
                                 || ue.NodeType == ExpressionType.Not
                                 || ue.NodeType == ExpressionType.OnesComplement))
                    {
                        // Primitive/enum cast: collapses to the operand at SQL level.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        cols.Add(new Column(memberName, ue.Type, mapping.Type, mapping.Provider, memberName));
                    }
                    else if (arg is ConstantExpression || (arg is MemberExpression me2 && me2.Expression is ConstantExpression))
                    {
                        // Pure literal (true constant or closure-captured local) -- SCV.
                        // FormatLiteral emits the canonical text; reserve a column slot
                        // here so the anonymous-type ctor lookup matches arity. Covers
                        // DateTime/Guid/TimeSpan/etc closure-captured locals that the
                        // 9a7ca70 literal emit relies on.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        cols.Add(new Column(memberName, arg.Type, mapping.Type, mapping.Provider, memberName));
                    }
                    else if (arg is NewExpression ne)
                    {
                        // Embedded ctor producing a single value (e.g. `new DateTimeOffset(...)`
                        // emitted as one canonical-text column by SCV's 7-arg DateTimeOffset
                        // handler). Reserve one column slot typed as the ctor's result type.
                        // Multi-column nested anonymous shapes are handled separately by
                        // CreateNestedAnonymousProjectionMaterializer, which short-circuits
                        // ahead of this fallback.
                        var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                        cols.Add(new Column(memberName, ne.Type, mapping.Type, mapping.Provider, memberName));
                    }
                }
                return cols.ToArray();
            }
            return mapping.Columns;
        }

        /// <summary>
        /// Resolves the value converter to apply to an IGrouping.Key column. Restricted to enum key
        /// types — those are the ones that crash when the stored provider value (e.g. the enum name)
        /// is read as the enum's underlying integer. Matches by the converter's model type and
        /// declines if more than one column matches (ambiguous) so it never guesses wrong.
        /// </summary>
        private static IValueConverter? ResolveGroupKeyConverter(TableMapping mapping, Type keyType)
        {
            var t = Nullable.GetUnderlyingType(keyType) ?? keyType;
            if (!t.IsEnum) return null;
            IValueConverter? found = null;
            foreach (var c in mapping.Columns)
            {
                if (c.Converter == null) continue;
                var modelT = Nullable.GetUnderlyingType(c.Converter.ModelType) ?? c.Converter.ModelType;
                if (modelT != t) continue;
                if (found != null) return null; // ambiguous
                found = c.Converter;
            }
            return found;
        }

        /// <summary>
        /// True when <paramref name="expr"/> is a navigation-collection projection binding in either
        /// bare (<c>o.Lines</c>) or shaped (<c>o.Lines.ToList()</c>, <c>o.Lines.Where(pred).ToList()</c>)
        /// form. The split-query / dependent-query pipeline fills these, so they must be excluded from
        /// the row-column set. Mirrors the peel order in SelectClauseVisitor.TryMatchDetectedCollection.
        /// </summary>
        private static bool IsShapedOrBareNavigationCollection(Expression expr, TableMapping mapping)
        {
            var current = expr;

            // Peel a terminating ToList/ToArray/AsEnumerable over a single source.
            if (current is MethodCallExpression term
                && term.Arguments.Count == 1
                && (term.Method.DeclaringType == typeof(Enumerable) || term.Method.DeclaringType == typeof(Queryable))
                && term.Method.Name is nameof(Enumerable.ToList) or nameof(Enumerable.ToArray) or nameof(Enumerable.AsEnumerable))
            {
                current = term.Arguments[0];

                // Peel an optional single Where(source, predicate).
                if (current is MethodCallExpression whereCall
                    && whereCall.Arguments.Count == 2
                    && (whereCall.Method.DeclaringType == typeof(Enumerable) || whereCall.Method.DeclaringType == typeof(Queryable))
                    && whereCall.Method.Name == nameof(Enumerable.Where))
                {
                    current = whereCall.Arguments[0];
                }
            }

            return current is MemberExpression member && IsNavigationCollection(member, mapping);
        }

        /// <summary>
        /// Checks if a member expression represents a navigation collection property.
        /// </summary>
        private static bool IsNavigationCollection(MemberExpression memberExpr, TableMapping mapping)
        {
            if (memberExpr.Member is not PropertyInfo propInfo)
                return false;

            var propType = propInfo.PropertyType;

            // Check if it's a collection type (IEnumerable<T> but not string)
            if (propType != typeof(string) &&
                typeof(IEnumerable).IsAssignableFrom(propType) &&
                propType.IsGenericType)
            {
                // Verify it's NOT a column (meaning it's likely a navigation property)
                if (!mapping.ColumnsByName.ContainsKey(propInfo.Name))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
