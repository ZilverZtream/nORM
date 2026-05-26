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
        /// Analyzes a projection expression to determine if it contains untranslatable operations
        /// that require client-side evaluation.
        /// </summary>
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
                nameof(Enumerable.Where)
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
                if (node.Type.Name.StartsWith("<>"))
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
