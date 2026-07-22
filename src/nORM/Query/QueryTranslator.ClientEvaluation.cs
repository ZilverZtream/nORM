using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// True for Count/LongCount/Sum/Min/Max/Average/Any over a chain of
        /// Where/Select/ordering/Distinct rooted at a `ctx.Query&lt;T&gt;()` call
        /// or a captured IQueryable — the shapes SelectClauseVisitor lowers to
        /// a correlated scalar subquery inside a projection.
        /// </summary>
        internal static bool IsQueryRootedScalarAggregate(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable))
                return false;
            if (node.Method.Name is not (nameof(Queryable.Count) or nameof(Queryable.LongCount)
                or nameof(Queryable.Sum) or nameof(Queryable.Min) or nameof(Queryable.Max)
                or nameof(Queryable.Average)))
                return false;
            return HasQueryRootedSource(node.Arguments.Count > 0 ? node.Arguments[0] : null);
        }

        /// <summary>
        /// True for First/FirstOrDefault over a Where/Select/ordering/Distinct chain rooted
        /// at a <c>ctx.Query&lt;T&gt;()</c> call or a captured IQueryable — the ordered
        /// single-row shape both SelectClauseVisitor (projection) and ExpressionToSqlVisitor
        /// (predicate) lower to a correlated <c>… LIMIT 1</c> scalar subquery.
        /// </summary>
        internal static bool IsQueryRootedScalarFirst(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable))
                return false;
            if (node.Method.Name is not (nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Last) or nameof(Queryable.LastOrDefault)))
                return false;
            return HasQueryRootedSource(node.Arguments.Count > 0 ? node.Arguments[0] : null);
        }

        /// <summary>
        /// True for ElementAt/ElementAtOrDefault over a query-rooted chain — the offset single-row
        /// form the predicate translator lowers to a <c>LIMIT 1 OFFSET n</c> scalar subquery. Kept
        /// separate from <see cref="IsQueryRootedScalarFirst"/> because the projection path cannot
        /// yet apply the offset, so a projected ElementAt stays fail-closed rather than silently
        /// returning the first element.
        /// </summary>
        internal static bool IsQueryRootedScalarElementAt(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable))
                return false;
            if (node.Method.Name is not (nameof(Queryable.ElementAt) or nameof(Queryable.ElementAtOrDefault)))
                return false;
            return HasQueryRootedSource(node.Arguments.Count > 0 ? node.Arguments[0] : null);
        }

        /// <summary>
        /// True for a shaped collection projection binding — <c>o.Lines.ToList()</c>,
        /// <c>o.Lines.ToArray()</c>, <c>o.Lines.AsEnumerable()</c>, or the same wrapping a single
        /// <c>o.Lines.Where(pred)</c> — where the root is a collection navigation member off the
        /// entity parameter. SelectClauseVisitor fetches these via a split query (applying the
        /// captured predicate to the child fetch), so the analyzer must not flag the ToList/Where/nav
        /// members as client-eval. Mirrors the peel-order in SelectClauseVisitor.TryMatchDetectedCollection.
        /// </summary>
        internal static bool IsShapedCollectionBinding(MethodCallExpression node)
        {
            Expression current = node;

            // Must terminate in a materializer over a single source: ToList/ToArray/AsEnumerable.
            if (current is MethodCallExpression term
                && term.Arguments.Count == 1
                && (term.Method.DeclaringType == typeof(Enumerable) || term.Method.DeclaringType == typeof(Queryable))
                && term.Method.Name is nameof(Enumerable.ToList) or nameof(Enumerable.ToArray) or nameof(Enumerable.AsEnumerable))
            {
                current = term.Arguments[0];
            }
            else
            {
                return false;
            }

            // Optional single Select(source, elementProjection) — admitted only when the projection reads
            // solely its own element (SelectClauseVisitor.IsSafeChildProjection), matching the split-query
            // detection. A closure- or outer-referencing projection is left unpeeled so it falls through to
            // client-eval.
            if (current is MethodCallExpression selectCall
                && selectCall.Arguments.Count == 2
                && (selectCall.Method.DeclaringType == typeof(Enumerable) || selectCall.Method.DeclaringType == typeof(Queryable))
                && selectCall.Method.Name == nameof(Enumerable.Select))
            {
                var selArg = selectCall.Arguments[1];
                var selLambda = selArg as LambdaExpression
                    ?? (selArg is UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression q } ? q : null);
                if (selLambda is { Parameters.Count: 1 } && SelectClauseVisitor.IsSafeChildProjection(selLambda))
                    current = selectCall.Arguments[0];
            }

            // Optional ordering ops in LOCKSTEP with TryMatchDetectedCollection: Take → Skip → ThenBy* →
            // OrderBy. Take/Skip peeled only when constant (matching the main peeler), so a non-constant count
            // is left in place and the binding falls through to client-eval exactly as the detector does.
            if (current is MethodCallExpression takeCall
                && takeCall.Arguments.Count == 2
                && (takeCall.Method.DeclaringType == typeof(Enumerable) || takeCall.Method.DeclaringType == typeof(Queryable))
                && takeCall.Method.Name == nameof(Enumerable.Take)
                && TryGetConstantValue(takeCall.Arguments[1], out var tv) && tv is int)
            {
                current = takeCall.Arguments[0];
            }
            if (current is MethodCallExpression skipCall
                && skipCall.Arguments.Count == 2
                && (skipCall.Method.DeclaringType == typeof(Enumerable) || skipCall.Method.DeclaringType == typeof(Queryable))
                && skipCall.Method.Name == nameof(Enumerable.Skip)
                && TryGetConstantValue(skipCall.Arguments[1], out var sv) && sv is int)
            {
                current = skipCall.Arguments[0];
            }
            while (current is MethodCallExpression thenCall
                && thenCall.Arguments.Count == 2
                && (thenCall.Method.DeclaringType == typeof(Enumerable) || thenCall.Method.DeclaringType == typeof(Queryable))
                && thenCall.Method.Name is nameof(Enumerable.ThenBy) or nameof(Enumerable.ThenByDescending))
            {
                current = thenCall.Arguments[0];
            }
            if (current is MethodCallExpression orderCall
                && orderCall.Arguments.Count == 2
                && (orderCall.Method.DeclaringType == typeof(Enumerable) || orderCall.Method.DeclaringType == typeof(Queryable))
                && orderCall.Method.Name is nameof(Enumerable.OrderBy) or nameof(Enumerable.OrderByDescending))
            {
                current = orderCall.Arguments[0];
            }

            // Optional single Where(source, predicate).
            if (current is MethodCallExpression whereCall
                && whereCall.Arguments.Count == 2
                && (whereCall.Method.DeclaringType == typeof(Enumerable) || whereCall.Method.DeclaringType == typeof(Queryable))
                && whereCall.Method.Name == nameof(Enumerable.Where))
            {
                current = whereCall.Arguments[0];
            }

            return IsCollectionNavigationMember(current);
        }

        /// <summary>
        /// True when <paramref name="expr"/> is a generic collection navigation member accessed
        /// directly off a lambda parameter (e.g. <c>o.Lines</c> where Lines is a List&lt;Line&gt;).
        /// String is excluded so <c>o.Name</c> is never mistaken for a collection.
        /// </summary>
        private static bool IsCollectionNavigationMember(Expression expr)
        {
            if (expr is not MemberExpression { Expression: ParameterExpression } m)
                return false;
            if (m.Member is not PropertyInfo p)
                return false;
            var t = p.PropertyType;
            if (t == typeof(string) || !t.IsGenericType)
                return false;
            if (t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return true;
            return t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        }

        /// <summary>
        /// Reserved map key under which <see cref="ComputeProjectionSubqueryConverters"/> registers the
        /// converter for a BARE scalar projection whose body is itself the correlated subquery op
        /// (<c>Select(p =&gt; ctx.Query&lt;C&gt;()....First())</c>) — no anonymous-type member name exists. Empty
        /// string is impossible as a real C# member name, so it never collides with a New/MemberInit member.
        /// The scalar-projection materializer path reads this key to apply ConvertFromProvider.
        /// </summary>
        internal const string BareScalarSubqueryConverterKey = "";

        /// <summary>
        /// For a projection lambda, resolves the value converter of each member whose value comes
        /// from a correlated First/Last/Min/Max subquery over a converter column — keyed by the
        /// projection member name (matching the shadow-column naming in ExtractColumnsFromProjection),
        /// or by <see cref="BareScalarSubqueryConverterKey"/> when the whole projection body is the subquery.
        /// Returns null when no such member exists, so the materializer path is unchanged for every
        /// ordinary projection. Requires the context to resolve the subquery element's mapping.
        /// </summary>
        internal static IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? ComputeProjectionSubqueryConverters(LambdaExpression projection, DbContext ctx)
        {
            Dictionary<string, nORM.Mapping.IValueConverter>? result = null;
            void Consider(string name, Expression arg)
            {
                var e = StripConvert(arg);
                // Direct member of an entity parameter (Select(w => new { w.Col }) / Select(w => w.Col)):
                // resolve its converter from the PARAMETER's entity mapping. Needed for set operations, where
                // the outer materializer runs with the element (anon/scalar) mapping, not the entity, so
                // ExtractColumnsFromProjection can't resolve the column and would read the raw stored value.
                if (e is MemberExpression dm && dm.Expression is ParameterExpression dp)
                {
                    try
                    {
                        var dmap = ctx.GetMapping(dp.Type);
                        if (dmap.TryGetColumnForMemberAccess(dm, out var dcol) && dcol.Converter != null)
                            (result ??= new Dictionary<string, nORM.Mapping.IValueConverter>(StringComparer.Ordinal))[name] = dcol.Converter;
                    }
                    catch { }
                    return;
                }
                if (e is not MethodCallExpression mce || !(IsSubqueryScalarColumnOp(mce) || IsNavigationScalarColumnOp(mce)))
                    return;
                if (!TryResolveSubqueryProjectedMember(mce, out var elementType, out var member)
                    || elementType == null || member == null)
                    return;
                try
                {
                    var mapping = ctx.GetMapping(elementType);
                    if (mapping.TryGetColumnForMemberAccess(member, out var col) && col.Converter != null)
                        (result ??= new Dictionary<string, nORM.Mapping.IValueConverter>(StringComparer.Ordinal))[name] = col.Converter;
                }
                catch { }
            }

            var body = projection.Body;
            if (body is NewExpression ne)
            {
                for (int i = 0; i < ne.Arguments.Count; i++)
                    Consider(ne.Members?[i]?.Name ?? $"Item{i + 1}", ne.Arguments[i]);
            }
            else if (body is MemberInitExpression mi)
            {
                // Braces are REQUIRED: without them the trailing `else` below binds to this inner
                // `if (b is MemberAssignment ma)` (dangling-else), so a bare projection never registers.
                foreach (var b in mi.Bindings)
                    if (b is MemberAssignment ma)
                        Consider(ma.Member.Name, ma.Expression);
            }
            else
            {
                // Bare scalar projection under the reserved empty-name key. Consider registers either a
                // correlated subquery op (Select(p => sub....First())) or a DIRECT member (Select(w => w.Col)).
                // The direct-member case matters for set operations, where the outer materializer runs with the
                // scalar ELEMENT mapping (e.g. int), not the entity, so the value would otherwise come back as
                // its raw stored representation.
                Consider(BareScalarSubqueryConverterKey, body);
            }
            return result;
        }

        /// <summary>A correlated subquery op whose scalar result carries the source column's type
        /// (and therefore its converter): First/Last/FirstOrDefault/LastOrDefault, or Min/Max.
        /// Sum/Average/Count produce a numeric aggregate, not the column's converted type.</summary>
        private static bool IsSubqueryScalarColumnOp(MethodCallExpression mce)
            => IsQueryRootedScalarFirst(mce)
               || (mce.Method.DeclaringType == typeof(Queryable)
                   && mce.Method.Name is nameof(Queryable.Min) or nameof(Queryable.Max)
                   && IsQueryRootedScalarAggregate(mce));

        /// <summary>
        /// The navigation-collection analogue of <see cref="IsSubqueryScalarColumnOp"/>: a scalar
        /// First/Last or Min/Max whose source chain roots at a navigation-collection member of the
        /// projection parameter (e.g. <c>p.Children.OrderBy(..).Select(c =&gt; c.Col).First()</c> or
        /// <c>p.Children.Max(c =&gt; c.Col)</c>). The nav emit selects/aggregates the STORED column, so the
        /// scalar result carries the column's converter and must run through ConvertFromProvider — the same
        /// contract as the ctx.Query correlated path. Structural only (no mapping): the element type comes
        /// from <see cref="TryResolveSubqueryProjectedMember"/> and a non-entity root resolves no converter.
        /// </summary>
        private static bool IsNavigationScalarColumnOp(MethodCallExpression mce)
        {
            var name = mce.Method.Name;
            var isFirst = name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Last) or nameof(Queryable.LastOrDefault);
            var isMinMax = name is nameof(Queryable.Min) or nameof(Queryable.Max) && mce.Arguments.Count == 2;
            if (!isFirst && !isMinMax)
                return false;
            // Walk the source chain to its root. A nav collection roots at a member-on-parameter whose type
            // is a generic IEnumerable<T> (Enumerable/Queryable operators over p.Children); ctx.Query() roots
            // at a zero-argument method call and a captured local roots at a member on a constant closure.
            Expression cur = mce.Arguments.Count > 0 ? mce.Arguments[0] : mce;
            while (cur is MethodCallExpression m && m.Arguments.Count > 0)
                cur = m.Arguments[0];
            return cur is MemberExpression { Expression: ParameterExpression } nm
                && nm.Type != typeof(string)
                && nm.Type.IsGenericType
                && typeof(System.Collections.IEnumerable).IsAssignableFrom(nm.Type);
        }

        /// <summary>
        /// Finds the single scalar member a correlated subquery projects: an aggregate selector
        /// (<c>Max(c =&gt; c.Member)</c>) or the innermost <c>Select(c =&gt; c.Member)</c> in the chain.
        /// </summary>
        private static bool TryResolveSubqueryProjectedMember(MethodCallExpression mce, out Type? elementType, out MemberExpression? member)
        {
            elementType = null;
            member = null;
            if (mce.Arguments.Count > 1 && StripQuotes(mce.Arguments[1]) is LambdaExpression aggSel
                && aggSel.Parameters.Count == 1 && StripConvert(aggSel.Body) is MemberExpression aggMember)
            {
                elementType = aggSel.Parameters[0].Type;
                member = aggMember;
                return true;
            }
            var current = mce.Arguments.Count > 0 ? mce.Arguments[0] : null;
            while (current is MethodCallExpression m)
            {
                if (m.Method.Name == nameof(Queryable.Select) && m.Arguments.Count == 2
                    && StripQuotes(m.Arguments[1]) is LambdaExpression sel
                    && sel.Parameters.Count == 1 && StripConvert(sel.Body) is MemberExpression selMember)
                {
                    elementType = sel.Parameters[0].Type;
                    member = selMember;
                    return true;
                }
                if (m.Arguments.Count == 0) break;
                current = m.Arguments[0];
            }
            return false;
        }

        /// <summary>Stable-within-process fingerprint of a projection subquery-converter map for the
        /// materializer cache key; 0 for null/empty so ordinary projections keep their existing key.</summary>
        internal static long ProjectionSubqueryConverterFingerprint(IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? map)
        {
            if (map == null || map.Count == 0) return 0L;
            long h = 17;
            foreach (var kvp in map.OrderBy(k => k.Key, StringComparer.Ordinal))
            {
                h = h * 31 + kvp.Key.GetHashCode();
                h = h * 31 + kvp.Value.GetType().GetHashCode();
            }
            return h;
        }

        /// <summary>True when a queryable chain contains any OrderBy/OrderByDescending/ThenBy ordering.</summary>
        internal static bool HasQueryableOrdering(Expression? source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                if (mce.Method.DeclaringType == typeof(Queryable)
                    && mce.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending)
                        or nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending))
                    return true;
                if (mce.Arguments.Count == 0) break;
                current = mce.Arguments[0];
            }
            return false;
        }

        /// <summary>
        /// Rewrites a queryable chain so every ordering flips direction (OrderBy↔OrderByDescending,
        /// ThenBy↔ThenByDescending) — turning "the last ordered row" into "the first" so Last can be
        /// served by the same single-row subquery as First. <paramref name="hadOrdering"/> reports
        /// whether any ordering was present; without one, "last" is undefined and callers fail closed.
        /// </summary>
        internal static Expression ReverseQueryableOrderings(Expression source, out bool hadOrdering)
        {
            var had = false;
            var result = ReverseOrderingsCore(source, ref had);
            hadOrdering = had;
            return result;
        }

        private static Expression ReverseOrderingsCore(Expression source, ref bool hadOrdering)
        {
            if (source is not MethodCallExpression mce || mce.Arguments.Count == 0)
                return source;
            var newSource = ReverseOrderingsCore(mce.Arguments[0], ref hadOrdering);
            string? flipped = mce.Method.DeclaringType == typeof(Queryable)
                ? mce.Method.Name switch
                {
                    nameof(Queryable.OrderBy) => nameof(Queryable.OrderByDescending),
                    nameof(Queryable.OrderByDescending) => nameof(Queryable.OrderBy),
                    nameof(Queryable.ThenBy) => nameof(Queryable.ThenByDescending),
                    nameof(Queryable.ThenByDescending) => nameof(Queryable.ThenBy),
                    _ => null
                }
                : null;
            var args = new Expression[mce.Arguments.Count];
            args[0] = newSource;
            for (int i = 1; i < mce.Arguments.Count; i++) args[i] = mce.Arguments[i];
            if (flipped != null)
            {
                hadOrdering = true;
                return Expression.Call(typeof(Queryable), flipped, mce.Method.GetGenericArguments(), args);
            }
            if (ReferenceEquals(newSource, mce.Arguments[0]))
                return mce;
            return Expression.Call(mce.Object, mce.Method, args);
        }

        /// <summary>
        /// A Query-rooted subquery TERMINAL — the scalar aggregates, the ordered single-row
        /// First/FirstOrDefault, plus the boolean terminals Any/All/Contains — over an
        /// explicit <c>ctx.Query&lt;T&gt;()</c> chain.
        /// Even when such a terminal has no outer-row reference (its predicate touches
        /// only closures/constants) it must NOT be constant-folded: folding executes a
        /// database query during translation, bakes the result as a literal, and caches
        /// it, so a later execution with a different closure replays the first value.
        /// These must stay server-side subqueries whose closures re-bind per execution.
        /// </summary>
        internal static bool IsQueryRootedSubqueryTerminal(MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable))
                return false;
            if (node.Method.Name is nameof(Queryable.Any) or nameof(Queryable.All)
                or nameof(Queryable.Contains))
                return HasQueryRootedSource(node.Arguments.Count > 0 ? node.Arguments[0] : null);
            return IsQueryRootedScalarAggregate(node) || IsQueryRootedScalarFirst(node)
                || IsQueryRootedScalarElementAt(node);
        }

        private static bool HasQueryRootedSource(Expression? source)
        {
            var current = source;
            while (current is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case "Where":
                    case "Select":
                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                    case "Distinct":
                    case "AsNoTrackingWithIdentityResolution":
                    case "AsNoTracking":
                        break;
                    case "Query": // the `ctx.Query<T>()` chain root
                        return true;
                    default:
                        return false;
                }
                if (mce.Arguments.Count == 0) return false;
                current = mce.Arguments[0];
            }
            return current is ConstantExpression { Value: System.Linq.IQueryable }
                || (current is MemberExpression && typeof(System.Linq.IQueryable).IsAssignableFrom(current.Type));
        }

        /// <summary>
        /// Analyzes a projection expression to determine if it contains untranslatable operations
        /// that require client-side evaluation.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class TranslatabilityAnalyzer : ExpressionVisitor
        {
            private readonly DatabaseProvider _provider;
            private bool _hasUntranslatableExpression;
            // Tracks whether ALL untranslatable expressions encountered are
            // "auto-route-safe" -- pure LINQ-to-Objects leaves that the
            // SelectTranslator can transparently run client-side after
            // materializing the raw columns. The narrow allowlist (string.Split,
            // string.ToCharArray, etc.) lets common shapes "just work" under the
            // default Throw policy without users having to opt into Warn/Allow.
            // Complex untranslatables (correlated subqueries, multi-hop nav,
            // arbitrary helper methods) still surface the Throw with the full
            // diagnostic.
            private bool _allUntranslatableAreAutoRouteSafe = true;
            private static readonly HashSet<string> _autoRouteSafeMethodNames = new(StringComparer.Ordinal)
            {
                // Methods that return a non-SQL type from a column input but are
                // pure / side-effect-free and trivially executable client-side.
                nameof(string.Split),
                nameof(string.ToCharArray),
            };
            private readonly HashSet<string> _sqlTranslatableMethods = new()
            {
                // String methods
                nameof(string.ToUpper),
                nameof(string.ToLower),
                nameof(string.ToUpperInvariant),
                nameof(string.ToLowerInvariant),
                nameof(string.Contains),
                nameof(string.StartsWith),
                nameof(string.EndsWith),
                nameof(string.Substring),
                nameof(string.Trim),
                nameof(string.TrimStart),
                nameof(string.TrimEnd),
                nameof(string.Format),
                nameof(string.Concat),
                nameof(string.Join),
                nameof(string.Compare),
                nameof(string.CompareTo),
                // CompareTo instance-form covered above; nameof(string.CompareTo)
                // is the same string for all primitive receivers.
                nameof(string.IsNullOrEmpty),
                nameof(string.IsNullOrWhiteSpace),
                nameof(string.Equals),
                nameof(string.Replace),
                nameof(string.IndexOf),
                nameof(string.PadLeft),
                nameof(string.PadRight),
                nameof(string.Remove),
                nameof(string.Insert),
                "get_Chars",
                nameof(char.IsDigit),
                nameof(char.IsLetter),
                nameof(char.IsWhiteSpace),
                nameof(char.IsUpper),
                nameof(char.IsLower),
                nameof(char.IsPunctuation),
                nameof(char.IsSymbol),
                nameof(char.IsControl),
                nameof(char.GetNumericValue),
                nameof(int.Parse),
                nameof(Enum.HasFlag),
                nameof(Enum.Parse),
                nameof(Enum.TryParse),
                // Convert.* from-string overloads -- sister to X.Parse(string).
                nameof(Convert.ToInt32),
                nameof(Convert.ToInt64),
                nameof(Convert.ToDouble),
                nameof(Convert.ToDecimal),
                nameof(Convert.ToBoolean),
                nameof(Convert.ChangeType),

                // Math methods
                nameof(Math.Abs),
                nameof(Math.Ceiling),
                nameof(Math.Floor),
                nameof(Math.Round),
                nameof(Math.Log2),
                nameof(Math.Cbrt),
                nameof(Math.Sinh),
                nameof(Math.Cosh),
                nameof(Math.Tanh),
                nameof(Math.Atan2),
                nameof(Math.Asinh),
                nameof(Math.Acosh),
                nameof(Math.Atanh),
                nameof(Math.MaxMagnitude),
                nameof(Math.MinMagnitude),
                nameof(Math.IEEERemainder),
                nameof(Math.ScaleB),
                nameof(Math.BigMul),
                nameof(Math.Clamp),
                // decimal.Round shares its name with Math.Round but has its
                // own static (and analyzer is name-based not type-based, so
                // Math.Round's entry doesn't cover it).
                nameof(decimal.Round),
                // decimal sister statics for the Math.* math primitives.
                nameof(decimal.Truncate),
                nameof(decimal.Floor),
                nameof(decimal.Ceiling),
                nameof(decimal.Abs),
                // Static method-form arithmetic.
                nameof(decimal.Add),
                nameof(decimal.Subtract),
                nameof(decimal.Multiply),
                nameof(decimal.Divide),
                nameof(decimal.Remainder),
                nameof(decimal.Negate),
                nameof(decimal.Compare),
                // IEEE 754 predicates on double/float (same name on both).
                nameof(double.IsNaN),
                nameof(double.IsInfinity),
                nameof(double.IsFinite),
                nameof(double.IsNegativeInfinity),
                nameof(double.IsPositiveInfinity),

                // DateTime properties
                nameof(DateTime.Year),
                nameof(DateTime.Month),
                nameof(DateTime.Day),
                nameof(DateTime.Hour),
                nameof(DateTime.Minute),
                nameof(DateTime.Second),
                nameof(DateTime.Date),
                nameof(DateTime.DayOfYear),
                nameof(DateTime.DayOfWeek),
                nameof(DateTime.TimeOfDay),
                nameof(DateTime.Millisecond),
                nameof(DateTime.Ticks),
                nameof(DateTime.Compare),
                nameof(DateTime.ParseExact),
                nameof(DateTime.IsLeapYear),
                nameof(DateTime.DaysInMonth),
                nameof(DateOnly.DayNumber),
                nameof(DateOnly.AddDays),
                nameof(DateOnly.AddMonths),
                nameof(DateOnly.AddYears),
                nameof(TimeOnly.Add),
                // TimeOnly.AddHours/AddMinutes wrap around midnight; the SCV branch
                // and the provider AddSecondsToTimeOnlySql hook implement that wrap.
                // (Name-based list: these entries also open DateTime.AddHours /
                // AddMinutes, whose handlers exist in both visitors.)
                nameof(TimeOnly.AddHours),
                nameof(TimeOnly.AddMinutes),
                // DateTime/DateTimeOffset.Add / .Subtract(TimeSpan) instance forms.
                // The method names are also shared with several other types whose
                // semantics differ; the SCV handler keys off the receiver type so
                // adding them here only opens the translatable gate.
                nameof(DateTime.Subtract),
                nameof(DateOnly.FromDayNumber),
                nameof(DateOnly.FromDateTime),
                nameof(DateOnly.ToDateTime),
                nameof(TimeOnly.FromDateTime),
                nameof(TimeOnly.FromTimeSpan),
                nameof(TimeOnly.IsBetween),
                nameof(Nullable<int>.GetValueOrDefault),
                nameof(DateTimeOffset.UtcDateTime),
                nameof(DateTimeOffset.LocalDateTime),
                nameof(DateTimeOffset.DateTime),
                nameof(DateTimeOffset.Offset),
                nameof(DateTimeOffset.ToOffset),

                // TimeSpan component properties -- sub-day spans only; see
                // SqliteProvider.TranslateFunction(TimeSpan) for the SUBSTR
                // string-slice emission.
                nameof(TimeSpan.Hours),
                nameof(TimeSpan.Minutes),
                nameof(TimeSpan.Seconds),
                nameof(TimeSpan.TotalHours),
                nameof(TimeSpan.TotalMinutes),
                nameof(TimeSpan.TotalSeconds),
                nameof(TimeSpan.TotalMilliseconds),
                nameof(TimeSpan.Compare),
                // TimeSpan factory methods -- per-row column args lowered to
                // SQL printf('%02d:%02d:%02d.%07d', ...) by SqliteProvider.TranslateMethodCall.
                // The materializer reads the resulting canonical 'HH:mm:ss.fffffff' text
                // via TimeSpan.Parse. All factories share a total-ticks reduction so the
                // sub-second precision flows through uniformly.
                nameof(TimeSpan.FromHours),
                nameof(TimeSpan.FromMinutes),
                nameof(TimeSpan.FromSeconds),
                nameof(TimeSpan.FromMilliseconds),
                nameof(TimeSpan.FromTicks),
                nameof(TimeSpan.FromDays),
                // TimeSpan.Negate() (no-arg instance) and TimeSpan.Duration() (abs).
                // Both lower to (-1.0 * seconds_expr) / ABS(seconds_expr) per SCV.
                nameof(TimeSpan.Negate),
                nameof(TimeSpan.Duration),

                // LINQ aggregate methods (when used in proper context)
                nameof(Enumerable.Count),
                nameof(Enumerable.LongCount),
                nameof(Enumerable.Sum),
                nameof(Enumerable.Average),
                nameof(Enumerable.Min),
                nameof(Enumerable.Max),
                nameof(Enumerable.Any),
                nameof(Enumerable.All),
                // LINQ projection / filter — translatable when sitting between a navigation
                // collection and an aggregate (e.g. `parent.Children.Select(c => c.X).Sum()`
                // → SCV emits a correlated subquery). Without these, the analyzer flags the
                // whole projection as client-eval and SelectTranslator throws the dad1fec
                // message even though SCV can actually emit valid SQL.
                nameof(Enumerable.Select),
                nameof(Enumerable.SelectMany),
                nameof(Enumerable.Where),
                // Distinct between a navigation Select and Count — `parent.Children.Select(c => c.X)
                // .Distinct().Count()` → SCV emits COUNT(DISTINCT x). Admitting the name lets the
                // analyzer pass the chain; SCV either emits it or fails loud for an unsupported shape.
                nameof(Enumerable.Distinct)
            };

            public TranslatabilityAnalyzer(DatabaseProvider provider)
            {
                _provider = provider;
            }

            public bool HasUntranslatableExpression => _hasUntranslatableExpression;

            /// <summary>
            /// True when every untranslatable expression encountered is in the
            /// narrow auto-route-safe allowlist (pure LINQ-to-Objects leaves).
            /// SelectTranslator uses this to bypass the Throw policy for safe
            /// shapes like `Select(p =&gt; p.Csv.Split(','))`.
            /// </summary>
            public bool AllUntranslatableAreAutoRouteSafe => _allUntranslatableAreAutoRouteSafe;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                // A Queryable scalar aggregate whose chain roots at a Query call or a
                // captured IQueryable translates as a correlated scalar subquery in the
                // projection (SelectClauseVisitor emits it) — admit WITHOUT descending
                // so the subtree's Query() call and inner lambdas are not re-analyzed
                // as client-eval leaves.
                if (IsQueryRootedScalarAggregate(node) || IsQueryRootedScalarFirst(node))
                    return node;

                // Deterministic "top related value" over a navigation collection —
                // `nav.OrderBy(key).Select(scalar).First()` — which SelectClauseVisitor emits as a
                // LIMIT-1 correlated subquery. Admit WITHOUT descending so the nav member / Select /
                // OrderBy aren't flagged as client-eval; only this exact shape is admitted.
                if (SelectClauseVisitor.IsNavigationOrderedFirstShape(node))
                    return node;

                // A shaped collection binding — `o.Lines.ToList()` or
                // `o.Lines.Where(pred).ToList()` — is fetched via a split query by
                // SelectClauseVisitor, not materialized client-side. Admit it WITHOUT
                // descending so its ToList/Where/nav members are not flagged as client-eval.
                if (IsShapedCollectionBinding(node))
                    return node;

                // Check if this is a method that can be translated to SQL
                var declaringType = node.Method.DeclaringType;

                // entity.GetType() folds in SCV.VisitMember when wrapped by a
                // Type.<member> access (Name/FullName/Namespace/...) -- accept
                // it here so the analyzer doesn't pre-flag the whole projection
                // as client-eval before the per-visitor folder runs.
                if (node.Method.Name == "GetType"
                    && node.Arguments.Count == 0
                    && node.Object != null)
                {
                    return base.VisitMethodCall(node);
                }

                // No-arg ToString() on a non-string receiver lowers to the provider's
                // CAST AS TEXT (primitives) or a CASE-WHEN-per-name expansion (enums) --
                // both handled by SelectClauseVisitor and ExpressionToSqlVisitor. Admit
                // here so the analyzer doesn't pre-flag the whole projection as client-
                // eval before the per-visitor handler runs.
                if (node.Method.Name == nameof(object.ToString)
                    && node.Arguments.Count == 0
                    && node.Object != null
                    && node.Object.Type != typeof(string))
                {
                    return base.VisitMethodCall(node);
                }
                // numeric.ToString(formatString) -- admit so the per-visitor handler
                // can map fixed-decimal "F<N>" formats to printf('%.<N>f', col).
                // Other format strings still throw inside the visitor with the
                // supported-subset hint.
                if (node.Method.Name == nameof(object.ToString)
                    && node.Arguments.Count == 1
                    && node.Object != null
                    && node.Object.Type != typeof(string)
                    && node.Arguments[0].Type == typeof(string))
                {
                    return base.VisitMethodCall(node);
                }

                // Check if provider can translate this function. Build placeholder args of
                // the correct arity so the provider's arity-guarded switches (e.g.
                // `nameof(Math.Sqrt) when args.Length == 1`) can probe safely. Without this,
                // the no-args call falls into 1-arg arms like `nameof(Math.Abs) => $"ABS({args[0]})"`
                // and throws IndexOutOfRangeException when the analyzer first visits a
                // projection containing `Math.Abs(col)`. The probe only cares about whether
                // a name+arity pair is translatable; real args bind at SQL emit time.
                if (declaringType != null)
                {
                    var arity = node.Arguments.Count + (node.Object != null ? 1 : 0);
                    var probeArgs = new string[arity];
                    for (int i = 0; i < arity; i++) probeArgs[i] = string.Empty;
                    var translated = _provider.TranslateFunction(node.Method.Name, declaringType, probeArgs);
                    if (translated == null && !_sqlTranslatableMethods.Contains(node.Method.Name))
                    {
                        // This method cannot be translated to SQL
                        _hasUntranslatableExpression = true;
                        if (!_autoRouteSafeMethodNames.Contains(node.Method.Name))
                            _allUntranslatableAreAutoRouteSafe = false;
                    }
                }
                else
                {
                    // Unknown declaring type - assume untranslatable AND unsafe
                    // (we can't reason about side-effects without a type).
                    _hasUntranslatableExpression = true;
                    _allUntranslatableAreAutoRouteSafe = false;
                }

                return base.VisitMethodCall(node);
            }

            protected override Expression VisitInvocation(InvocationExpression node)
            {
                // Lambda invocations cannot be translated to SQL and aren't
                // in the auto-route-safe allowlist (caller-supplied lambdas
                // may have side effects).
                _hasUntranslatableExpression = true;
                _allUntranslatableAreAutoRouteSafe = false;
                return base.VisitInvocation(node);
            }

            protected override Expression VisitNew(NewExpression node)
            {
                // Anonymous types and simple constructors are OK
                if (node.Type.Name.StartsWith("<>", StringComparison.Ordinal))
                {
                    // Anonymous type - this is fine
                    return base.VisitNew(node);
                }

                // Check if this is a simple value type constructor
                if (node.Type.IsValueType && node.Arguments.Count <= 1)
                {
                    return base.VisitNew(node);
                }

                // Other constructors may require client evaluation
                // but we'll allow them if they only use translatable sub-expressions
                return base.VisitNew(node);
            }
        }

        /// <summary>
        /// Extracts all member accesses from an expression that reference the parameter.
        /// These are the columns we need to fetch from the database.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class MemberAccessExtractor : ExpressionVisitor
        {
            private readonly ParameterExpression _parameter;
            private readonly HashSet<MemberInfo> _accessedMembers = new();

            public MemberAccessExtractor(ParameterExpression parameter)
            {
                _parameter = parameter;
            }

            public IReadOnlySet<MemberInfo> AccessedMembers => _accessedMembers;

            protected override Expression VisitMember(MemberExpression node)
            {
                // Check if this member access is directly on our parameter
                if (node.Expression == _parameter)
                {
                    _accessedMembers.Add(node.Member);
                }

                return base.VisitMember(node);
            }
        }

        /// <summary>
        /// Attempts to split a projection into a server-side (SQL) projection and a client-side
        /// projection when the original contains untranslatable expressions.
        /// </summary>
        /// <param name="originalProjection">The original projection lambda.</param>
        /// <param name="serverProjection">Output: Lambda that selects only required columns from DB.</param>
        /// <param name="clientProjection">Output: Delegate that applies remaining logic client-side.</param>
        /// <returns>True if the projection was successfully split; false if it can be fully translated to SQL.</returns>
        private bool TrySplitProjection(
            LambdaExpression originalProjection,
            out LambdaExpression? serverProjection,
            out Func<object, object>? clientProjection)
            => TrySplitProjection(originalProjection, out serverProjection, out clientProjection, out _);

        private bool TrySplitProjection(
            LambdaExpression originalProjection,
            out LambdaExpression? serverProjection,
            out Func<object, object>? clientProjection,
            out bool allUntranslatableAreAutoRouteSafe)
        {
            serverProjection = null;
            clientProjection = null;
            allUntranslatableAreAutoRouteSafe = false;

            // Analyze if the projection contains untranslatable expressions
            var analyzer = new TranslatabilityAnalyzer(_provider);
            analyzer.Visit(originalProjection.Body);

            if (!analyzer.HasUntranslatableExpression)
            {
                // Everything can be translated to SQL
                return false;
            }
            allUntranslatableAreAutoRouteSafe = analyzer.AllUntranslatableAreAutoRouteSafe;

            // Extract all member accesses we need from the database
            var extractor = new MemberAccessExtractor(originalProjection.Parameters[0]);
            extractor.Visit(originalProjection.Body);

            if (extractor.AccessedMembers.Count == 0)
            {
                // No database columns needed - this is a computed expression
                // We still need to fetch something, so we'll fetch all columns
                return false;
            }

            try
            {
                // Build server-side projection: select only the columns we need
                var parameter = originalProjection.Parameters[0];

                // Create a tuple or simple structure to hold the intermediate values
                // We'll use the actual entity type as intermediate, just fetching specific columns
                // This is simpler than dynamic type creation

                // For now, we'll create a member init expression that only initializes the needed members
                var memberInit = Expression.MemberInit(
                    Expression.New(parameter.Type),
                    extractor.AccessedMembers.Select(m =>
                        Expression.Bind(m, Expression.MakeMemberAccess(parameter, m)))
                );

                serverProjection = Expression.Lambda(memberInit, parameter);

                // Build client-side projection: take the intermediate object and apply original logic
                var intermediateParam = Expression.Parameter(typeof(object), "intermediate");
                var castIntermediate = Expression.Convert(intermediateParam, parameter.Type);

                // Replace the parameter in the original body with the cast intermediate
                var replacer = new ParameterReplacer(parameter, castIntermediate);
                var clientBody = replacer.Visit(originalProjection.Body);

                // Convert result to object
                var clientBodyAsObject = Expression.Convert(clientBody, typeof(object));
                var clientLambda = Expression.Lambda<Func<object, object>>(clientBodyAsObject, intermediateParam);

                // Compile the client-side projection
                clientProjection = ExpressionUtils.CompileWithFallback(clientLambda, default);

                return true;
            }
            catch (Exception ex) when (ex is InvalidOperationException or ArgumentException or NotSupportedException or MemberAccessException)
            {
                // If we can't split the projection, fall back to letting the SQL translator
                // try (and likely fail with a better error message)
                System.Diagnostics.Debug.WriteLine($"Failed to split projection for client-side evaluation: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Replaces a parameter with a different expression throughout an expression tree.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression _oldParameter;
            private readonly Expression _newExpression;

            public ParameterReplacer(ParameterExpression oldParameter, Expression newExpression)
            {
                _oldParameter = oldParameter;
                _newExpression = newExpression;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == _oldParameter ? _newExpression : base.VisitParameter(node);
            }
        }
    }
}
