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
    internal sealed class ExpressionToSqlVisitor : ExpressionVisitor, nORM.Internal.IResettable, IDisposable
    {
        private DbContext _ctx = null!;
        private TableMapping _mapping = null!;
        private DatabaseProvider _provider = null!;
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _parameterMappings = new();
        private ParameterExpression _parameter = null!;
        private string _tableAlias = string.Empty;
        private OptimizedSqlBuilder _sql = null!;
        private readonly Dictionary<string, object> _params = new();
        // Parameter sink (can be redirected to a shared dictionary)
        private Dictionary<string, object> _paramSink = null!;
        private int _paramIndex = 0;
        internal int ParamIndex => _paramIndex;
        private readonly List<string> _ownedCompiledParams = new();
        private readonly Dictionary<ParameterExpression, string> _ownedParamMap = new();
        private List<string> _compiledParams = null!;
        private Dictionary<ParameterExpression, string> _paramMap = null!;
        private bool _suppressNullCheck = false;
        // Outer translator's recursion depth so BuildExists/BuildIn pass depth+1 to sub-translators.
        private int _recursionDepth = 0;
        private const int ConstParamMapLimit = 1024;
        /// <summary>Maximum allowed length for a JSON path in <c>Json.Value()</c>.</summary>
        private const int MaxJsonPathLength = 500;
        /// <summary>SQL false literal used when a local collection is empty.</summary>
        private const string SqlFalseLiteral = "(1=0)";
        /// <summary>
        /// Maximum length (in characters or bytes) for inline string/binary parameters.
        /// Based on SQL Server's non-MAX varchar/varbinary threshold.
        /// </summary>
        private const int MaxInlineParameterLength = 8000;
        private readonly Dictionary<ConstKey, string> _constParamMap = new();
        private readonly Dictionary<(ParameterExpression Param, string Member), string> _memberParamMap = new();
        private static readonly Expression s_emptyExpression = Expression.Empty();
        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();
        // String methods (Contains, StartsWith, EndsWith) are handled via _fastMethodHandlers
        // for better performance, avoiding a redundant _translators dictionary lookup.
        private static readonly Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> _fastMethodHandlers =
            BuildFastMethodHandlers();

        private static Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> BuildFastMethodHandlers()
        {
            var dict = new Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>>
            {
                { typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!, HandleStringContains },
                { typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) })!, HandleStringStartsWith },
                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };
            // 2-arg StringComparison overloads — common in user predicates. We honour
            // ordinal-ignore-case / invariant-ignore-case / current-ignore-case by wrapping
            // both sides of the LIKE / equality compare in LOWER(); culture-specific
            // collation isn't reachable from SQL but case-folding to lower is the
            // de-facto cross-provider approximation.
            var containsCi = typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string), typeof(StringComparison) });
            if (containsCi != null) dict.Add(containsCi, HandleStringContainsWithComparison);
            var startsCi = typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string), typeof(StringComparison) });
            if (startsCi != null) dict.Add(startsCi, HandleStringStartsWithComparison);
            var endsCi = typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string), typeof(StringComparison) });
            if (endsCi != null) dict.Add(endsCi, HandleStringEndsWithComparison);
            var equalsInstanceCi = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(StringComparison) });
            if (equalsInstanceCi != null) dict.Add(equalsInstanceCi, HandleStringEqualsInstanceWithComparison);
            var equalsStaticCi = typeof(string).GetMethod(nameof(string.Equals), new[] { typeof(string), typeof(string), typeof(StringComparison) });
            if (equalsStaticCi != null) dict.Add(equalsStaticCi, HandleStringEqualsStaticWithComparison);
            return dict;
        }

        private static bool IsIgnoreCase(Expression comparisonArg)
        {
            if (TryGetConstantValue(comparisonArg, out var v) && v is StringComparison sc)
            {
                return sc is StringComparison.OrdinalIgnoreCase
                          or StringComparison.InvariantCultureIgnoreCase
                          or StringComparison.CurrentCultureIgnoreCase;
            }
            return false;
        }
        internal ExpressionToSqlVisitor() { }
        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                                      List<string>? compiledParams = null,
                                      Dictionary<ParameterExpression, string>? paramMap = null)
        {
            var context = new VisitorContext(ctx ?? throw new ArgumentNullException(nameof(ctx)), mapping ?? throw new ArgumentNullException(nameof(mapping)), provider ?? throw new ArgumentNullException(nameof(provider)), parameter ?? throw new ArgumentNullException(nameof(parameter)), tableAlias ?? throw new ArgumentNullException(nameof(tableAlias)), correlated, compiledParams, paramMap);
            Initialize(in context);
        }
        /// <summary>
        /// Initializes the visitor with all context required to translate a LINQ expression into
        /// SQL. This method can be called multiple times to reuse the same instance for different
        /// translations.
        /// </summary>
        /// <param name="context">A structure containing the current <see cref="DbContext"/>,
        /// table mapping, provider and parameter information.</param>
        public void Initialize(in VisitorContext context)
        {
            _paramSink = _params;
            _ctx = context.Context;
            _mapping = context.Mapping;
            _provider = context.Provider;
            _parameter = context.Parameter;
            _tableAlias = context.TableAlias;
            _parameterMappings.Clear();
            if (context.Correlated != null)
            {
                foreach (var kvp in context.Correlated)
                    _parameterMappings[kvp.Key] = kvp.Value;
            }
            _parameterMappings[context.Parameter] = (context.Mapping, context.TableAlias);
            _compiledParams = context.CompiledParams ?? _ownedCompiledParams;
            if (context.CompiledParams == null)
                _ownedCompiledParams.Clear();
            _paramMap = context.ParamMap ?? _ownedParamMap;
            if (context.ParamMap == null)
                _ownedParamMap.Clear();
            _constParamMap.Clear();
            // Start numbering from the caller-supplied offset so that a visitor sharing
            // _compiledParams/_paramMap with a previous visitor does not reuse parameter names
            // (e.g. both inner Where and global-filter Where getting @p0).
            _paramIndex = context.ParamIndexStart;
            _suppressNullCheck = false;
            _recursionDepth = context.RecursionDepth;
            _memberParamMap.Clear();
            _groupingKeys.Clear();
        }
        /// <summary>
        /// Resets the internal state so that the visitor can be returned to an object pool and
        /// reused for subsequent translations.
        /// </summary>
        public void Reset()
        {
            _sql = null!;
            _params.Clear();
            _paramSink = null!;
            _paramIndex = 0;
            _parameterMappings.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
            _compiledParams = null!;
            _paramMap = null!;
            _ctx = null!;
            _mapping = null!;
            _provider = null!;
            _parameter = null!;
            _tableAlias = string.Empty;
            _suppressNullCheck = false;
            _recursionDepth = 0;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _groupingKeys.Clear();
        }
        /// <summary>
        /// Releases resources by resetting the visitor's state. The instance can be reused after
        /// calling this method.
        /// </summary>
        public void Dispose()
        {
            Reset();
        }
        /// <summary>
        /// Translates the supplied LINQ expression tree into an SQL string using the configured
        /// provider and mapping information.
        /// </summary>
        /// <param name="expression">The expression to translate.</param>
        /// <returns>The SQL text corresponding to the expression.</returns>
        public string Translate(Expression expression)
        {
            using var builder = new OptimizedSqlBuilder();
            _sql = builder;
            if (!TryEmitMappedBooleanPredicate(expression, expectedValue: true))
                Visit(expression);
            return builder.ToSqlString();
        }

        private readonly struct FormatSegment
        {
            public readonly bool IsLiteral;
            public readonly string? Literal;
            public readonly int ArgIndex;
            public FormatSegment(string literal) { IsLiteral = true; Literal = literal; ArgIndex = -1; }
            public FormatSegment(int argIndex) { IsLiteral = false; Literal = null; ArgIndex = argIndex; }
        }

        /// <summary>
        /// Splits a <c>string.Format</c> template into literal/placeholder segments.
        /// Returns null if the template contains alignment (<c>{0,5}</c>) or format
        /// specifiers (<c>{0:N2}</c>) — those have no portable SQL equivalent and
        /// belong on the client-evaluation path. Escaped braces (<c>{{</c>, <c>}}</c>)
        /// are recognized and emitted as literal single braces.
        /// </summary>
        private static List<FormatSegment>? TryParseSimpleFormatSegments(string template)
        {
            var segments = new List<FormatSegment>();
            var literal = new System.Text.StringBuilder();
            int i = 0;
            while (i < template.Length)
            {
                char c = template[i];
                if (c == '{')
                {
                    if (i + 1 < template.Length && template[i + 1] == '{') { literal.Append('{'); i += 2; continue; }
                    if (literal.Length > 0) { segments.Add(new FormatSegment(literal.ToString())); literal.Clear(); }
                    int end = template.IndexOf('}', i + 1);
                    if (end < 0) return null;
                    var inner = template.Substring(i + 1, end - i - 1);
                    if (inner.Length == 0 || inner.IndexOfAny(new[] { ',', ':' }) >= 0) return null;
                    if (!int.TryParse(inner, out var argIdx) || argIdx < 0) return null;
                    segments.Add(new FormatSegment(argIdx));
                    i = end + 1;
                }
                else if (c == '}')
                {
                    if (i + 1 < template.Length && template[i + 1] == '}') { literal.Append('}'); i += 2; continue; }
                    return null; // unmatched closing brace
                }
                else
                {
                    literal.Append(c);
                    i++;
                }
            }
            if (literal.Length > 0) segments.Add(new FormatSegment(literal.ToString()));
            return segments;
        }

        /// <summary>
        /// C# emits `charExpr OP charExpr` as `intExpr OP intExpr` via implicit `char→int`
        /// promotions. When at least one side is `Convert(charExpr, int)`, recover the char
        /// comparison so the SQL emit doesn't compare a string SUBSTR result against an int
        /// parameter (which never matches at the database). Returns null when no rewrite
        /// is needed.
        /// </summary>
        private static BinaryExpression? TryRewriteLiftedCharComparison(BinaryExpression node)
        {
            bool leftIsLifted = node.Left is UnaryExpression { NodeType: ExpressionType.Convert } lu
                                && lu.Operand.Type == typeof(char);
            bool rightIsLifted = node.Right is UnaryExpression { NodeType: ExpressionType.Convert } ru
                                 && ru.Operand.Type == typeof(char);
            if (!leftIsLifted && !rightIsLifted) return null;

            Expression Demote(Expression side)
            {
                if (side is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand.Type == typeof(char))
                    return u.Operand;
                if (side is ConstantExpression { Value: not null } intLit && intLit.Type == typeof(int))
                    return Expression.Constant((char)(int)intLit.Value, typeof(char));
                if (side is ConstantExpression { Value: null }) return Expression.Constant(null, typeof(char?));
                return side;
            }

            var newLeft = Demote(node.Left);
            var newRight = Demote(node.Right);
            if (ReferenceEquals(newLeft, node.Left) && ReferenceEquals(newRight, node.Right)) return null;
            return Expression.MakeBinary(node.NodeType, newLeft, newRight);
        }
        protected override Expression VisitBinary(BinaryExpression node)
        {
            // C# lifts char comparisons to int — `r.Name[0] == 'A'` becomes
            // `Equal(Convert(get_Chars(...), int), Constant(65, int))`. If we let the
            // generic comparison path emit it, the int parameter (65) would never match
            // the char-shaped result of SUBSTR(...). Detect the lift on one side and
            // re-fold both operands back to char so the SQL compares string vs string.
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual)
            {
                var rewritten = TryRewriteLiftedCharComparison(node);
                if (rewritten != null) node = rewritten;
            }
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                bool leftNull  = IsNullExpression(node.Left);
                bool rightNull = IsNullExpression(node.Right);
                if (leftNull || rightNull)
                {
                    _sql.Append("(");
                    Visit(leftNull ? node.Right : node.Left);
                    _sql.Append(node.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                    _sql.Append(")");
                    return node;
                }

                // Inline boolean literals (true/false) as SQL literals instead of parameterizing.
                // Parameterized booleans (WHERE col = @p0 with @p0=1) deprive query planners of
                // column selectivity statistics. Providers that prefer bare boolean predicates get
                // WHERE col / WHERE NOT col for non-nullable bools instead.
                if (TryInlineBoolLiteral(node))
                    return node;

                // Nullable column-vs-column comparison needs three-valued logic.
                // A plain = or <> is incorrect when either side can be NULL at runtime.
                // For Nullable<T> value types: always expand (runtime null possible).
                // For reference types (string, class): only expand when BOTH sides could be null
                // at runtime (i.e., neither side is a known non-null constant or parameter).
                // Comparing a string column to a literal "ABC" does not need expansion since "ABC" is never null.
                if (NeedsNullSafeExpansion(node.Left, node.Right, node.NodeType))
                {
                    int ls = _sql.Length;
                    Visit(node.Left);
                    string lf = _sql.ToString(ls, _sql.Length - ls);
                    _sql.TruncateTo(ls);

                    int rs = _sql.Length;
                    Visit(node.Right);
                    string rf = _sql.ToString(rs, _sql.Length - rs);
                    _sql.TruncateTo(rs);

                    if (node.NodeType == ExpressionType.Equal)
                    {
                        _sql.Append(_provider.NullSafeEqual(lf, rf));
                    }
                    else
                    {
                        // For NotEqual, use asymmetric simplified form when the right side is a
                        // known non-null value (e.g. a literal or closure-captured non-null constant).
                        // In that case, "rf IS NULL" can never happen, so the full 3-way expansion
                        // reduces to: (lf IS NULL OR lf <> rf)
                        bool rightCouldBeNull = CouldBeNull(node.Right);
                        if (!rightCouldBeNull)
                            _sql.Append($"({lf} IS NULL OR {lf} <> {rf})");
                        else
                            _sql.Append(_provider.NullSafeNotEqual(lf, rf));
                    }
                    return node;
                }
            }

            if (node.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
            {
                _sql.Append("(");
                if (!TryEmitMappedBooleanPredicate(node.Left, expectedValue: true))
                    Visit(node.Left);
                _sql.Append(node.NodeType == ExpressionType.AndAlso ? " AND " : " OR ");
                if (!TryEmitMappedBooleanPredicate(node.Right, expectedValue: true))
                    Visit(node.Right);
                _sql.Append(")");
                return node;
            }

            // `a ?? b` lowers to COALESCE(a, b). The expression tree uses Coalesce as a
            // BinaryExpression node; sit it before the generic switch below so it doesn't
            // hit the "operator not supported" throw. Composes with arithmetic / comparison
            // so `(col ?? 0) > 5` becomes `(COALESCE(col, 0)) > 5`.
            if (node.NodeType == ExpressionType.Coalesce)
            {
                _sql.Append("COALESCE(");
                Visit(node.Left);
                _sql.Append(", ");
                Visit(node.Right);
                _sql.Append(")");
                return node;
            }

            // Bitwise XOR needs provider-specific syntax — SQLite has no `^` operator and
            // PostgreSQL uses `#`. Route through DatabaseProvider.GetBitwiseXorSql which
            // hands back the right token (or a synthesised `(a|b) - (a&b)` on SQLite).
            if (node.NodeType == ExpressionType.ExclusiveOr)
            {
                var leftSql = GetSql(node.Left);
                var rightSql = GetSql(node.Right);
                _sql.Append(_provider.GetBitwiseXorSql(leftSql, rightSql));
                return node;
            }

            _sql.Append("(");
            Visit(node.Left);
            _sql.Append(node.NodeType switch
            {
                ExpressionType.Equal => " = ",
                ExpressionType.NotEqual => " <> ",
                ExpressionType.GreaterThan => " > ",
                ExpressionType.GreaterThanOrEqual => " >= ",
                ExpressionType.LessThan => " < ",
                ExpressionType.LessThanOrEqual => " <= ",
                ExpressionType.AndAlso => " AND ",
                ExpressionType.OrElse => " OR ",
                // Arithmetic operators flow through to SQL directly; the column type drives the
                // server's semantics (integer vs decimal vs double).
                ExpressionType.Add => " + ",
                ExpressionType.Subtract => " - ",
                ExpressionType.Multiply => " * ",
                ExpressionType.Divide => " / ",
                ExpressionType.Modulo => " % ",
                // Bitwise operators on integer columns: the And/Or/ExclusiveOr node types
                // overload as both boolean (no short-circuit) and bitwise depending on operand
                // type. Lower to SQL bit operators so flag-mask predicates like
                // `(Flags & 4) == 4` translate server-side. All four supported providers
                // (SQLite, SQL Server, Postgres, MySQL) accept & | ^ for integer columns.
                ExpressionType.And => " & ",
                ExpressionType.Or => " | ",
                // ExclusiveOr is handled above via DatabaseProvider.GetBitwiseXorSql
                _ => throw new NormUnsupportedFeatureException(
                    $"Binary operator '{node.NodeType}' has no portable SQL equivalent. " +
                    "For LeftShift / RightShift on integer columns, rewrite as multiply or " +
                    "divide by a power of 2 (`x * 2` instead of `x << 1`, `x / 4` instead of " +
                    "`x >> 2`) — the SQL planner produces the same execution plan and the " +
                    "rewrite works on every provider. For Power, use `Math.Pow(x, n)` which " +
                    "lowers to the provider's POWER / POW function.")
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }

        /// <summary>
        /// Checks if an expression is a compile-time boolean constant (true/false).
        /// Handles ConstantExpression and Convert(ConstantExpression) wrappers.
        /// </summary>
        private static bool TryGetBoolConstant(Expression expr, out bool value)
        {
            if (expr is ConstantExpression { Value: bool b })
            {
                value = b;
                return true;
            }
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ue)
                return TryGetBoolConstant(ue.Operand, out value);
            value = default;
            return false;
        }

        /// <summary>
        /// Optimizes boolean literal comparisons by emitting SQL literals instead of parameters.
        /// Returns true if the optimization was applied (caller should return node immediately).
        /// </summary>
        private bool TryInlineBoolLiteral(BinaryExpression node)
        {
            if (TryGetBoolConstant(node.Left, out bool boolVal))
            {
                EmitBoolComparison(node.Right, boolVal, node.NodeType);
                return true;
            }
            if (TryGetBoolConstant(node.Right, out boolVal))
            {
                EmitBoolComparison(node.Left, boolVal, node.NodeType);
                return true;
            }
            return false;
        }

        private void EmitBoolComparison(Expression memberSide, bool boolVal, ExpressionType op)
        {
            if (_provider.PrefersBareBooleanPredicates && memberSide.Type == typeof(bool))
            {
                var emitPositivePredicate =
                    op == ExpressionType.Equal && boolVal ||
                    op == ExpressionType.NotEqual && !boolVal;

                _sql.Append("(");
                if (!emitPositivePredicate)
                    _sql.Append("NOT ");
                Visit(memberSide);
                _sql.Append(")");
                return;
            }

            var literal = boolVal ? _provider.BooleanTrueLiteral : _provider.BooleanFalseLiteral;
            _sql.Append("(");
            Visit(memberSide);
            _sql.Append(op == ExpressionType.Equal ? " = " : " <> ");
            _sql.Append(literal);
            _sql.Append(")");
        }

        private static bool IsNullExpression(Expression e)
        {
            if (e is ConstantExpression ce)
                return ce.Value is null;
            if (e is UnaryExpression ue && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked))
                return IsNullExpression(ue.Operand);
            if (e is MemberExpression me && me.Expression is ConstantExpression closure)
            {
                try
                {
                    var val = closure.Value == null ? null :
                        me.Member is FieldInfo fi ? fi.GetValue(closure.Value) :
                        me.Member is PropertyInfo pi ? pi.GetValue(closure.Value) : null;
                    return val is null;
                }
                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures (getter throws, access denied, etc.) — conservatively
                    // report as non-null so the caller does not emit an incorrect IS NULL predicate.
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// Returns <c>true</c> when <paramref name="t"/> is <c>Nullable&lt;T&gt;</c>.
        /// Used to detect when column-vs-column comparisons need three-valued logic.
        /// </summary>
        private static bool IsNullableValueType(Type t) =>
            t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);

        /// <summary>
        /// Returns <c>true</c> when <paramref name="t"/> is either a reference type (string, class, etc.)
        /// or a <c>Nullable&lt;T&gt;</c> value type. Both can be NULL at runtime and require
        /// three-valued SQL logic (IS NULL guards) for equality/inequality comparisons.
        /// </summary>
        private static bool IsNullableOrReferenceType(Type t) =>
            !t.IsValueType ||  // reference types (string, class, etc.)
            (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>));

        /// <summary>
        /// Determines whether an equality/inequality comparison between two expressions needs
        /// three-valued SQL logic (IS NULL expansion). The expansion is required when:
        /// - Either side is <c>Nullable&lt;T&gt;</c>, or
        /// - For <c>Equal</c>: both sides are reference types that could be null (no change needed;
        ///   <c>NULL = 'Alice'</c> is UNKNOWN in SQL and <c>null == "Alice"</c> is false in C#, so plain = is correct).
        /// - For <c>NotEqual</c>: the LEFT side could be null (column reference) even when the right
        ///   side is a known non-null constant. SQL 3-valued logic makes <c>NULL &lt;&gt; 'Alice'</c>
        ///   UNKNOWN (excluded), but C# semantics say <c>null != "Alice"</c> is true (included).
        ///   Fix: emit <c>(col IS NULL OR col &lt;&gt; @p)</c> whenever left could be null.
        /// </summary>
        private bool NeedsNullSafeExpansion(Expression left, Expression right, ExpressionType nodeType = ExpressionType.Equal)
        {
            // Nullable<T> value types always need expansion (runtime null is possible on either side)
            if (IsNullableValueType(left.Type) || IsNullableValueType(right.Type))
                return true;

            // Reference types: asymmetric rule for Equal vs NotEqual.
            // At least one side is a reference type (the condition below is true when
            // either left or right is not a value type, via De Morgan on the negation).
            // A "known non-null" side is one that is:
            //   - A non-null compile-time constant (ConstantExpression with non-null value)
            //   - A closure capture whose value we can verify is non-null at expression-build time
            if (!left.Type.IsValueType || !right.Type.IsValueType)
            {
                bool leftCouldBeNull  = CouldBeNull(left);
                bool rightCouldBeNull = CouldBeNull(right);

                if (nodeType == ExpressionType.NotEqual)
                    // For NotEqual: expand when EITHER side could be null.
                    // Specifically, a nullable column vs a non-null constant still needs
                    // (col IS NULL OR col <> @p) to preserve C# null != "literal" → true semantics.
                    return leftCouldBeNull || rightCouldBeNull;
                else
                    // For Equal: expand only when BOTH sides could be null.
                    // null == "Alice" is false in both SQL (UNKNOWN→false) and C#, so no expansion needed.
                    return leftCouldBeNull && rightCouldBeNull;
            }

            return false;
        }

        /// <summary>
        /// Returns <c>true</c> when <paramref name="expr"/> might evaluate to SQL NULL at runtime.
        /// Constants and member accesses on closures whose current value is non-null are "safe"
        /// (they will never produce SQL NULL on the right-hand side of a comparison).
        /// Column references (member accesses on query parameters) are always potentially nullable.
        /// </summary>
        /// <remarks>
        /// This method does not distinguish C# nullable reference types (NRTs) from non-nullable
        /// reference types. The NRT annotation (e.g., <c>string?</c> vs <c>string</c>) is erased
        /// at runtime and is not present in expression trees. At the CLR level all reference types
        /// can be null, so this method conservatively returns <c>true</c> for any reference-typed
        /// column or method-call expression.
        /// </remarks>
        private bool CouldBeNull(Expression expr)
        {
            // Non-null compile-time constant: cannot be null
            if (expr is ConstantExpression ce)
                return ce.Value is null;

            // Unwrap casts/conversions
            if (expr is UnaryExpression ue && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked))
                return CouldBeNull(ue.Operand);

            if (expr is MemberExpression columnMember &&
                columnMember.Expression is ParameterExpression columnParameter &&
                _parameterMappings.TryGetValue(columnParameter, out var mappedParameter) &&
                mappedParameter.Mapping.ColumnsByName.TryGetValue(columnMember.Member.Name, out var column))
            {
                return column.IsNullable;
            }

            // Closure-captured member whose value is non-null at expression-build time
            if (expr is MemberExpression me && me.Expression is ConstantExpression closure)
            {
                try
                {
                    var val = closure.Value == null ? null :
                        me.Member is FieldInfo fi ? fi.GetValue(closure.Value) :
                        me.Member is PropertyInfo pi ? pi.GetValue(closure.Value) : null;
                    return val is null;  // non-null captured value → not nullable
                }
                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures (getter throws, access denied, etc.) — assume nullable
                    // to preserve correctness.
                    return true;
                }
            }

            // Everything else (column references, method calls, etc.) could be null
            return true;
        }

        private bool TryEmitMappedBooleanPredicate(Expression expression, bool expectedValue)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            if (expression is UnaryExpression { NodeType: ExpressionType.Not } not)
                return TryEmitMappedBooleanPredicate(not.Operand, !expectedValue);

            if (expression.Type != typeof(bool) ||
                expression is not MemberExpression member ||
                member.Expression is not ParameterExpression parameter ||
                !_parameterMappings.TryGetValue(parameter, out var info) ||
                !info.Mapping.ColumnsByName.ContainsKey(member.Member.Name))
            {
                return false;
            }

            var columnSql = GetSql(member);
            _sql.Append(_provider.FormatBooleanPredicate(columnSql, expectedValue));
            return true;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // TimeSpan member access whose receiver is a DateTime subtraction lowers to a
            // fractional-seconds scalar via the provider, then a unit-conversion divide.
            // Examples: (end - start).TotalHours, .TotalMinutes, .TotalSeconds, .TotalDays,
            // .Days, .Hours, .Minutes, .Seconds. Both nullable and non-nullable receivers.
            if (node.Expression is BinaryExpression timeSpanBinary
                && timeSpanBinary.NodeType == ExpressionType.Subtract
                && IsDateTimeLike(timeSpanBinary.Left.Type)
                && IsDateTimeLike(timeSpanBinary.Right.Type)
                && node.Expression.Type == typeof(TimeSpan)
                && TryEmitTimeSpanMember(node.Member.Name, GetSql(timeSpanBinary.Left), GetSql(timeSpanBinary.Right)))
            {
                return node;
            }

            // Nullable<T> structural members: HasValue -> IS NOT NULL, Value -> operand itself.
            // GetValueOrDefault is a method, handled in VisitMethodCall.
            if (node.Expression != null
                && node.Expression.Type.IsGenericType
                && node.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                if (node.Member.Name == "HasValue")
                {
                    _sql.Append('(');
                    Visit(node.Expression);
                    _sql.Append(" IS NOT NULL)");
                    return node;
                }
                if (node.Member.Name == "Value")
                {
                    Visit(node.Expression);
                    return node;
                }
            }
            if (node.Expression is ParameterExpression pe && _parameterMappings.TryGetValue(pe, out var info))
            {
                if (_groupingKeys.TryGetValue(pe, out var groupKey) && node.Member.Name == "Key")
                {
                    _sql.Append(groupKey);
                    return node;
                }
                if (info.Mapping.ColumnsByName.TryGetValue(node.Member.Name, out var column))
                {
                    // Table aliases are generated internally and escaped when created,
                    // allowing them to be used safely without additional validation.
                    _sql.Append($"{info.Alias}.{column.EscCol}");
                    return node;
                }
            }
            if (node.Expression is ParameterExpression p && !_parameterMappings.ContainsKey(p))
            {
                var key = (p, node.Member.Name);
                if (!_memberParamMap.TryGetValue(key, out var paramName))
                {
                    paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                    _params[paramName] = DBNull.Value;
                    _compiledParams.Add(paramName);
                    _memberParamMap[key] = paramName;
                }
                _sql.Append(paramName);
                return node;
            }
            if (TryGetConstantValue(node, out var value))
            {
                // Closure-captured variable: emit a compiled parameter so the live value is
                // re-extracted from the expression tree on every plan-cache hit.  Baking the
                // value into _params at translation time causes stale values to be used when
                // the captured variable changes between calls that share the same cached plan.
                //
                // Use the current size of the SHARED _compiledParams list as the index so that
                // parameter names are globally unique across all visitor instances within one
                // query translation.  The "cp" prefix prevents collisions with inline-constant
                // parameters which use the "p" prefix (visitor-local _paramIndex).
                var paramName = $"{_provider.ParamPrefix}cp{_compiledParams.Count}";
                _params[paramName] = DBNull.Value; // placeholder; actual value supplied at execution time
                _compiledParams.Add(paramName);
                _sql.Append(paramName);
                return node;
            }
            if (node.Expression != null)
            {
                var exprSql = GetSql(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType!, exprSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
            }
            throw new NormUnsupportedFeatureException($"Member '{node.Member.Name}' is not supported in this context.");
        }
        protected override Expression VisitConstant(ConstantExpression node)
        {
            AppendConstant(node.Value, node.Type);
            return node;
        }
        protected override Expression VisitParameter(ParameterExpression node)
        {
            // SelectTranslator rewrites `GroupBy(k).Select(g => proj)` into a 3-arg
            // `GroupBy(k, (k, gs) => proj)`, so the projection's key parameter `k`
            // stands in directly for the group key (not via `g.Key`). When a downstream
            // Where / OrderBy references `k` as a standalone parameter
            // (`.Where(x => x.Category.StartsWith("A"))` → expands to `k.StartsWith(…)`)
            // we must emit the group-by SQL here rather than letting the parameter
            // fall through to the entity-column path (which emits nothing and produces
            // SQL like ` LIKE 'A%'` → SQLite syntax error) or the closure-binding path
            // (which would bind it as `@p0 LIKE 'A%'` and never set the value).
            if (_groupingKeys.TryGetValue(node, out var groupKeySql))
            {
                _sql.Append(groupKeySql);
                return node;
            }
            if (_parameterMappings.ContainsKey(node))
                return base.VisitParameter(node);
            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                if (TryEmitMappedBooleanPredicate(node.Operand, expectedValue: false))
                    return node;

                _sql.Append("(NOT(");
                Visit(node.Operand);
                _sql.Append("))");
                return node;
            }
            // Numeric / enum conversions in projections: (int)entity.Status, (long)e.Count, etc.
            // SQL columns are already typed, so just emit the operand. Reference-type Convert
            // (interface casts, base→derived) has no SQL meaning and falls through to default.
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                var operandType = node.Operand.Type;
                var targetType = node.Type;
                var operandUnderlying = Nullable.GetUnderlyingType(operandType) ?? operandType;
                var targetUnderlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
                bool operandIsPrimitive = operandUnderlying.IsPrimitive || operandUnderlying.IsEnum
                    || operandUnderlying == typeof(decimal) || operandUnderlying == typeof(string);
                bool targetIsPrimitive = targetUnderlying.IsPrimitive || targetUnderlying.IsEnum
                    || targetUnderlying == typeof(decimal) || targetUnderlying == typeof(string);
                if (operandIsPrimitive && targetIsPrimitive)
                {
                    Visit(node.Operand);
                    return node;
                }
            }
            return base.VisitUnary(node);
        }
        protected override Expression VisitConditional(ConditionalExpression node)
        {
            // x ? a : b -> (CASE WHEN x THEN a ELSE b END). Nested conditionals naturally
            // recurse, producing CASE WHEN ... WHEN ... ELSE ... END.
            _sql.Append("(CASE WHEN ");
            Visit(node.Test);
            _sql.Append(" THEN ");
            Visit(node.IfTrue);
            _sql.Append(" ELSE ");
            Visit(node.IfFalse);
            _sql.Append(" END)");
            return node;
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Fast path: common string methods (Contains, StartsWith, EndsWith) are handled
            // directly via pre-built delegates, bypassing the general method translation pipeline.
            if (_fastMethodHandlers.TryGetValue(node.Method, out var handler))
            {
                handler(this, node);
                return node;
            }
            // Nullable<T>.GetValueOrDefault() / GetValueOrDefault(fallback) — COALESCE.
            if (node.Object != null
                && node.Method.Name == nameof(Nullable<int>.GetValueOrDefault)
                && node.Object.Type.IsGenericType
                && node.Object.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var inner = GetSql(node.Object);
                if (node.Arguments.Count == 0)
                {
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type)!;
                    var fallback = underlying == typeof(string) ? "''"
                        : underlying.IsValueType ? "0"
                        : "NULL";
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fallback).Append(')');
                }
                else
                {
                    var fbSql = GetSql(node.Arguments[0]);
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fbSql).Append(')');
                }
                return node;
            }
            // No-arg ToString() on a column/expression — lower to provider-specific CAST.
            // Receiver type must NOT already be string (identity, would emit a redundant
            // CAST) and must NOT be an enum (CAST(StatusInt AS TEXT) returns "0"/"1"/"2"
            // rather than the enum names users actually expect — that's a client-eval shape).
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && node.Object.Type != typeof(string)
                && !(Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type).IsEnum)
            {
                var inner = GetSql(node.Object);
                _sql.Append(_provider.GetToStringSql(inner));
                return node;
            }

            // char.IsDigit(c) / char.IsLetter(c) / char.IsWhiteSpace(c) — static methods on
            // System.Char. ASCII-only ranges via portable BETWEEN / OR / IN comparisons; matches
            // the common "is the first char a digit/letter/space" validation pattern. We don't
            // try to match the full Unicode definition (System.Char.IsLetter accepts every
            // Unicode L* category) because no provider has a portable way to express that —
            // this is the de-facto SQL parity behaviour EF Core also implements.
            if (node.Method.DeclaringType == typeof(char)
                && node.Object == null
                && node.Arguments.Count == 1
                && (node.Method.Name == nameof(char.IsDigit)
                    || node.Method.Name == nameof(char.IsLetter)
                    || node.Method.Name == nameof(char.IsWhiteSpace)))
            {
                var charSql = GetSql(node.Arguments[0]);
                switch (node.Method.Name)
                {
                    case nameof(char.IsDigit):
                        _sql.Append('(').Append(charSql).Append(" BETWEEN '0' AND '9')");
                        return node;
                    case nameof(char.IsLetter):
                        _sql.Append("((").Append(charSql).Append(" BETWEEN 'A' AND 'Z') OR (")
                            .Append(charSql).Append(" BETWEEN 'a' AND 'z'))");
                        return node;
                    case nameof(char.IsWhiteSpace):
                        // ASCII whitespace: space, tab, LF, CR. Matches CLR IsWhiteSpace for the
                        // characters that actually appear in textual database content.
                        _sql.Append('(').Append(charSql).Append(" = ' ' OR ")
                            .Append(charSql).Append(" = CHAR(9) OR ")
                            .Append(charSql).Append(" = CHAR(10) OR ")
                            .Append(charSql).Append(" = CHAR(13))");
                        return node;
                }
            }

            // char.ToUpper(c) / char.ToLower(c) — route through the provider's existing
            // UPPER / LOWER string functions. The argument expression is typically a
            // SUBSTR(...) from a string-indexer, so the SQL becomes
            // `LOWER(SUBSTR("T0"."Code", @p0 + 1, 1)) = @p1` (the lifted-char comparison
            // fix in 86d7ac1 keeps the right side bound as a single-char string).
            if (node.Method.DeclaringType == typeof(char)
                && node.Object == null
                && node.Arguments.Count == 1
                && (node.Method.Name == nameof(char.ToUpper) || node.Method.Name == nameof(char.ToLower)))
            {
                var charSql = GetSql(node.Arguments[0]);
                var fnName = node.Method.Name == nameof(char.ToUpper)
                    ? nameof(string.ToUpper)
                    : nameof(string.ToLower);
                var fn = _provider.TranslateFunction(fnName, typeof(string), charSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
            }

            // int.Parse(s) / long.Parse(s) / decimal.Parse(s) / double.Parse(s) — lower to a
            // per-provider numeric CAST. Single-arg overload AND the 2-arg overload that takes
            // an IFormatProvider (e.g. CultureInfo.InvariantCulture) are both supported — the
            // format provider only affects culture-specific parsing of the literal string, which
            // doesn't apply to a SQL cast. NumberStyles / signed-binary overloads still fall
            // through to unsupported (no SQL equivalent for those).
            if (node.Object == null
                && node.Method.Name == "Parse"
                && node.Arguments.Count is 1 or 2
                && (node.Method.DeclaringType == typeof(int) || node.Method.DeclaringType == typeof(long)
                    || node.Method.DeclaringType == typeof(decimal) || node.Method.DeclaringType == typeof(double))
                && node.Arguments[0].Type == typeof(string)
                && (node.Arguments.Count == 1 || typeof(IFormatProvider).IsAssignableFrom(node.Arguments[1].Type)))
            {
                var inner = GetSql(node.Arguments[0]);
                var dt = node.Method.DeclaringType!;
                string castSql;
                if (dt == typeof(int) || dt == typeof(long))
                    castSql = _provider.GetIntCastSql(inner, asLong: dt == typeof(long));
                else
                    castSql = _provider.GetRealCastSql(inner, asDecimal: dt == typeof(decimal));
                _sql.Append(castSql);
                return node;
            }

            // Guid.Parse(s) — identity pass-through. The receiver expression is already a string
            // (the column), and the comparison side (a Guid constant) binds as DbType.Guid which
            // every supported provider converts to its textual representation before binding.
            // So `WHERE Guid.Parse(TextCol) == constGuid` reduces to `WHERE TextCol = @p0` — no
            // CAST needed. ParseExact / TryParse have richer semantics that don't apply.
            if (node.Object == null
                && node.Method.Name == "Parse"
                && node.Arguments.Count == 1
                && node.Method.DeclaringType == typeof(Guid)
                && node.Arguments[0].Type == typeof(string))
            {
                Visit(node.Arguments[0]);
                return node;
            }

            // enum.HasFlag(other) — lower to `(col & other) = other`. Preserves .NET semantics
            // (true only when every bit of `other` is set in receiver). The receiver must be
            // an enum instance; we don't filter by [Flags] attribute because the runtime
            // doesn't either — HasFlag works on any Enum and is the natural shape for
            // bit-tested permission columns.
            if (node.Method.Name == nameof(Enum.HasFlag)
                && node.Arguments.Count == 1
                && node.Object != null
                && node.Object.Type.IsEnum)
            {
                var receiverSql = GetSql(node.Object);
                var flagSql = GetSql(node.Arguments[0]);
                _sql.Append('(').Append(receiverSql).Append(" & ").Append(flagSql).Append(") = ").Append(flagSql);
                return node;
            }
            if (!IsTranslatableMethod(node.Method))
                // ErrorMessages.QueryTranslationFailed is "Failed to translate LINQ query to SQL: {0}".
                // The {0} argument below is a detail message, not a duplicate prefix.
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Method '{node.Method.Name}' cannot be translated to SQL"));
            if (!_suppressNullCheck && RequiresNullCheck(node))
            {
                return TranslateWithNullCheck(node);
            }
            if (TryGetConstantValueSafe(node, out var constVal))
            {
                return CreateSafeParameter(constVal);
            }
            if (node.Method.DeclaringType == typeof(Json) && node.Method.Name == nameof(Json.Value))
            {
                var columnSql = GetSql(node.Arguments[0]);
                if (TryGetConstantValue(node.Arguments[1], out var path) && path is string jsonPath)
                {
                    // TYPE SAFETY FIX: Validate JSON path format to prevent SQL injection and runtime errors
                    if (string.IsNullOrWhiteSpace(jsonPath))
                    {
                        throw new NormQueryException("JSON path cannot be null or whitespace.");
                    }

                    // Q1 fix: validate JSON path for SQL-injection-capable chars only.
                    // Chars that can break out of the enclosing SQL string literal:
                    //   ' → closes string literal
                    //   " → identifier-quote escape
                    //   ; → statement terminator
                    //   \ → SQL/escape sequence in some dialects
                    // Chars that are VALID in JSON property names and must be allowed:
                    //   - (hyphen) → e.g. $.order-items[0].id (RFC 7159 §7 allows in key names)
                    //   * (wildcard) → e.g. $.* for JSONPath wildcard selection
                    //   / (path separator) → e.g. JSON Pointer RFC 6901 paths
                    if (jsonPath.IndexOfAny(new[] { '\'', '"', ';', '\\' }) >= 0)
                    {
                        throw new NormQueryException(
                            $"JSON path '{jsonPath}' contains invalid characters. " +
                            "JSON paths must not contain single-quote, double-quote, semicolon, or backslash.");
                    }

                    // Limit path length to prevent potential DoS via pathological JSON path strings.
                    if (jsonPath.Length > MaxJsonPathLength)
                    {
                        throw new NormQueryException(
                            $"JSON path exceeds maximum length of {MaxJsonPathLength} characters (actual: {jsonPath.Length}).");
                    }

                    var jsonSql = _provider.TranslateJsonPathAccess(columnSql, jsonPath);
                    _sql.Append(jsonSql);
                    return node;
                }
                else
                {
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "JSONPath argument in Json.Value must be a constant string."));
                }
            }
            // String methods are handled by _fastMethodHandlers (see above).
            if (node.Method.DeclaringType == typeof(string))
            {
                // String indexer s[i] compiles to String.get_Chars(i) — lower to
                // SUBSTR(col, i+1, 1) via the provider's existing 3-arg Substring shape.
                // The right-hand char literal binds as a single-char parameter, so the
                // SQL comparison ends up `SUBSTR(col, i+1, 1) = 'A'`.
                if (node.Method.Name == "get_Chars"
                    && node.Object != null
                    && node.Arguments.Count == 1)
                {
                    var receiver = GetSql(node.Object);
                    var index = GetSql(node.Arguments[0]);
                    var indexerSql = _provider.TranslateFunction(nameof(string.Substring), typeof(string), receiver, index, "1");
                    if (indexerSql != null)
                    {
                        _sql.Append(indexerSql);
                        return node;
                    }
                }
                // static string.IsNullOrEmpty(x) -> (x IS NULL OR x = '')
                // static string.IsNullOrWhiteSpace(x) -> (x IS NULL OR LTRIM(RTRIM(x)) = '')
                if (node.Object == null && node.Arguments.Count == 1 &&
                    (node.Method.Name == nameof(string.IsNullOrEmpty) || node.Method.Name == nameof(string.IsNullOrWhiteSpace)))
                {
                    var inner = GetSql(node.Arguments[0]);
                    var trimmed = _provider.TranslateFunction(nameof(string.Trim), typeof(string), inner) ?? inner;
                    var target = node.Method.Name == nameof(string.IsNullOrWhiteSpace) ? trimmed : inner;
                    _sql.Append('(').Append(inner).Append(" IS NULL OR ").Append(target).Append(" = '')");
                    return node;
                }
                // static string.Compare(a, b) and instance a.CompareTo(b) -> CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END
                if (node.Method.Name == nameof(string.Compare) || node.Method.Name == nameof(string.CompareTo))
                {
                    string lhs, rhs;
                    if (node.Object == null && node.Arguments.Count == 2)
                    {
                        lhs = GetSql(node.Arguments[0]);
                        rhs = GetSql(node.Arguments[1]);
                    }
                    else if (node.Object != null && node.Arguments.Count == 1)
                    {
                        lhs = GetSql(node.Object);
                        rhs = GetSql(node.Arguments[0]);
                    }
                    else
                    {
                        throw new NormUnsupportedFeatureException(
                            $"Overload of {node.Method.Name} with {node.Arguments.Count} arguments is not supported.");
                    }
                    _sql.Append("(CASE WHEN ").Append(lhs).Append(" < ").Append(rhs)
                        .Append(" THEN -1 WHEN ").Append(lhs).Append(" > ").Append(rhs)
                        .Append(" THEN 1 ELSE 0 END)");
                    return node;
                }
                // static string.Format("template", args...) where the template uses only
                // simple positional placeholders (`{0}`, `{1}`, ...). Lowers to a provider
                // concat that interleaves literal pieces and argument SQL. Format specifiers
                // (`{0:N2}` / `{0,5}`) get rejected so client-evaluation can take over for
                // those — no provider has a portable equivalent of .NET format strings.
                if (node.Object == null
                    && node.Method.Name == nameof(string.Format)
                    && node.Arguments.Count >= 2
                    && TryGetConstantValue(node.Arguments[0], out var fmtRaw) && fmtRaw is string template)
                {
                    var segments = TryParseSimpleFormatSegments(template);
                    if (segments != null)
                    {
                        // Collect remaining args (one per argument expression). string.Format
                        // accepts `params object[]`, so the second argument may be a
                        // NewArrayExpression wrapping the parts — unwrap if so.
                        var argExprs = new List<Expression>();
                        for (int i = 1; i < node.Arguments.Count; i++)
                        {
                            var a = node.Arguments[i];
                            if (a is NewArrayExpression arr) argExprs.AddRange(arr.Expressions);
                            else argExprs.Add(a);
                        }
                        var parts = new List<string>();
                        foreach (var seg in segments)
                        {
                            if (seg.IsLiteral)
                            {
                                if (seg.Literal!.Length > 0)
                                    parts.Add($"'{seg.Literal.Replace("'", "''")}'");
                            }
                            else
                            {
                                if (seg.ArgIndex >= argExprs.Count)
                                    return base.VisitMethodCall(node); // bad template, defer
                                var argSql = GetSql(argExprs[seg.ArgIndex]);
                                var argType = argExprs[seg.ArgIndex].Type;
                                if (argType != typeof(string))
                                    argSql = _provider.GetToStringSql(argSql);
                                parts.Add(argSql);
                            }
                        }
                        if (parts.Count == 0)
                        {
                            _sql.Append("''");
                            return node;
                        }
                        var concatSql = parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                        _sql.Append(concatSql);
                        return node;
                    }
                }
                // static string.Concat(a, b, ...) -> provider-specific concat
                if (node.Object == null && node.Method.Name == nameof(string.Concat) && node.Arguments.Count >= 1)
                {
                    var parts = new List<string>();
                    foreach (var a in node.Arguments)
                    {
                        // string.Concat(params object[]) - skip the array wrapper if present
                        if (a is NewArrayExpression arr)
                        {
                            foreach (var item in arr.Expressions) parts.Add(GetSql(item));
                        }
                        else
                        {
                            parts.Add(GetSql(a));
                        }
                    }
                    var concatSql = parts.Count == 1
                        ? parts[0]
                        : parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                    _sql.Append(concatSql);
                    return node;
                }
                var strArgs = new List<string>();
                if (node.Object != null)
                    strArgs.Add(GetSql(node.Object));
                foreach (var a in node.Arguments)
                    strArgs.Add(GetSql(a));
                var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, strArgs.ToArray());
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
                throw new NormUnsupportedFeatureException($"String method '{node.Method.Name}' is not supported.");
            }
            if (node.Method.DeclaringType == typeof(Convert) && node.Arguments.Count == 1)
            {
                var inner = GetSql(node.Arguments[0]);
                var sqlType = node.Method.Name switch
                {
                    nameof(Convert.ToInt32) or nameof(Convert.ToInt16) or nameof(Convert.ToByte) or nameof(Convert.ToSByte) => "INTEGER",
                    nameof(Convert.ToInt64) => "BIGINT",
                    nameof(Convert.ToString) => "TEXT",
                    nameof(Convert.ToDouble) or nameof(Convert.ToSingle) => "REAL",
                    nameof(Convert.ToDecimal) => "DECIMAL",
                    nameof(Convert.ToBoolean) => "BOOLEAN",
                    _ => null
                };
                if (sqlType != null)
                {
                    _sql.Append("CAST(").Append(inner).Append(" AS ").Append(sqlType).Append(')');
                    return node;
                }
            }
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                // Detect nested aggregates on a mapped navigation collection in a predicate context,
                // e.g. `parent.Children.Any(c => c.Foo > x)`. The receiver `parent.Children` is a
                // CLR List/IEnumerable, not an IQueryable, so it normally cannot translate. We
                // rewrite it into a correlated subquery on the dependent table joined by the
                // relation's FK, then let the existing Queryable.Any/All/Count handlers run.
                if (node.Arguments.Count >= 1 &&
                    node.Arguments[0] is MemberExpression navMember &&
                    navMember.Expression is ParameterExpression navParent &&
                    _parameterMappings.TryGetValue(navParent, out var navParentInfo) &&
                    navParentInfo.Mapping.Relations.TryGetValue(navMember.Member.Name, out var relation) &&
                    (node.Method.Name is nameof(Queryable.Any)
                                      or nameof(Queryable.All)
                                      or nameof(Queryable.Count)
                                      or nameof(Queryable.LongCount)))
                {
                    var rewritten = RewriteNavigationAggregate(node, navParent, relation);
                    Visit(rewritten);
                    return node;
                }

                // `parent.Children.Select(c => c.Value).Sum/Min/Max/Average()` — the Select
                // projection sits between the navigation and the aggregate. Emit as a
                // correlated scalar subquery directly:
                //   `(SELECT AGG(selector_sql) FROM ChildTable T1 WHERE T1.FK = parent.PK)`
                if (node.Arguments.Count == 1
                    && node.Method.Name is nameof(Queryable.Sum)
                                       or nameof(Queryable.Min)
                                       or nameof(Queryable.Max)
                                       or nameof(Queryable.Average)
                    && node.Arguments[0] is MethodCallExpression selectCall
                    && selectCall.Method.Name == nameof(Queryable.Select)
                    && selectCall.Arguments.Count == 2
                    && selectCall.Arguments[0] is MemberExpression selNav
                    && selNav.Expression is ParameterExpression selNavParent
                    && _parameterMappings.TryGetValue(selNavParent, out var selNavInfo)
                    && selNavInfo.Mapping.Relations.TryGetValue(selNav.Member.Name, out var selRel)
                    && StripQuotes(selectCall.Arguments[1]) is LambdaExpression selectorLambda)
                {
                    EmitNavigationScalarAggregateSubquery(
                        node.Method.Name, selNavParent, selNavInfo, selRel, selectorLambda);
                    return node;
                }
                switch (node.Method.Name)
                {
                    case nameof(Queryable.GroupBy):
                        HandleGroupByMethod(node);
                        return node;
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1
                            && node.Arguments[0] is ParameterExpression cp
                            && (_parameterMappings.ContainsKey(cp) || _groupingKeys.ContainsKey(cp)))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                (TableMapping Mapping, string Alias) info = _parameterMappings.TryGetValue(cp, out var existing)
                                    ? existing
                                    : (_mapping, _tableAlias);
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var predSql = visitor.Translate(countSelector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _sql.Append($"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)");
                                FastExpressionVisitorPool.Return(visitor);
                            }
                            else
                            {
                                _sql.Append("COUNT(*)");
                            }
                            return node;
                        }
                        // Count/LongCount over a query source in a predicate context: emit a
                        // correlated scalar subquery (SELECT COUNT(*) FROM child WHERE ...).
                        if (node.Arguments.Count >= 1)
                        {
                            var countLambda = node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null;
                            BuildScalarCountSubquery(node.Arguments[0], countLambda);
                            return node;
                        }
                        break;
                    case "Sum":
                    case "Average":
                    case "Min":
                    case "Max":
                        if (node.Arguments.Count >= 2
                            && node.Arguments[0] is ParameterExpression gp
                            && (_parameterMappings.ContainsKey(gp) || _groupingKeys.ContainsKey(gp)))
                        {
                            var selector = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (selector != null)
                            {
                                // GroupBy aggregate inside HAVING: the IGrouping parameter is in
                                // _groupingKeys, not _parameterMappings. The selector's element
                                // parameter belongs to the same row source as the outer query
                                // (the GroupBy hasn't introduced a new table), so reuse this
                                // visitor's mapping + alias.
                                (TableMapping Mapping, string Alias) info = _parameterMappings.TryGetValue(gp, out var existing)
                                    ? existing
                                    : (_mapping, _tableAlias);
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var colSql = visitor.Translate(selector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                var fn = node.Method.Name switch
                                {
                                    "Sum" => "SUM",
                                    "Average" => "AVG",
                                    "Min" => "MIN",
                                    "Max" => "MAX",
                                    _ => "",
                                };
                                _sql.Append($"{fn}({colSql})");
                                FastExpressionVisitorPool.Return(visitor);
                                return node;
                            }
                        }
                        break;
                }
            }
            if (node.Method.Name == nameof(List<int>.Contains))
            {
                Expression? collectionExpr = null;
                Expression? valueExpr = null;
                if (node.Method.DeclaringType == typeof(Enumerable))
                {
                    if (node.Arguments.Count == 2)
                    {
                        collectionExpr = node.Arguments[0];
                        valueExpr = node.Arguments[1];
                    }
                }
                else if (node.Object != null && node.Arguments.Count == 1)
                {
                    collectionExpr = node.Object;
                    valueExpr = node.Arguments[0];
                }
                if (collectionExpr != null && valueExpr != null && TryGetConstantValue(collectionExpr, out var colVal) && colVal is IEnumerable en && colVal is not string)
                {
                    var items = new List<object?>();
                    foreach (var item in en)
                        items.Add(item);
                    if (items.Count == 0)
                    {
                        _sql.Append(SqlFalseLiteral);
                        return node;
                    }

                    // Separate nulls from non-nulls: SQL `col IN (NULL, @p1)` never matches null
                    // rows — only `col IS NULL` does. Emit (col IN (...) OR col IS NULL) when needed.
                    bool hasNulls = items.Any(x => x is null);
                    var nonNullItems = hasNulls ? items.Where(x => x != null).ToList() : items;

                    // All-nulls case: emit col IS NULL with no parameters (IN () is invalid SQL).
                    if (nonNullItems.Count == 0)
                    {
                        Visit(valueExpr);
                        _sql.Append(" IS NULL");
                        return node;
                    }

                    // Exact accounting: _paramIndex tracks all params added so far.
                    // Nulls cost no parameters, so use nonNullItems.Count for the budget check.
                    var remainingParams = _provider.MaxParameters - _paramIndex;
                    if (nonNullItems.Count > remainingParams)
                        throw new NormQueryException(
                            $"IN clause with {nonNullItems.Count} items exceeds remaining parameter budget " +
                            $"({remainingParams} available, {_paramIndex} already used, limit {_provider.MaxParameters}). " +
                            "Consider using a temporary table or reducing the number of items.");

                    // Optimizer batching (1000 items per IN clause for DB plan efficiency).
                    // This is decoupled from parameter limits.
                    // NOTE: When the collection exceeds MaxInClauseItems, each batch re-visits
                    // valueExpr to emit the column reference (e.g., "T0.[Col] IN (...) OR T0.[Col] IN (...)").
                    // This means the SQL string grows linearly with the number of batches (one column
                    // reference per batch). For very large collections this is a deliberate tradeoff:
                    // multiple smaller IN clauses let the query optimizer produce better plans than a
                    // single massive IN list, at the cost of a slightly larger SQL string and plan
                    // cache variance (different collection sizes produce different SQL shapes).
                    const int MaxInClauseItems = 1000;
                    if (nonNullItems.Count > MaxInClauseItems)
                    {
                        if (hasNulls) _sql.Append("(");
                        _sql.Append("(");
                        for (int batch = 0; batch < nonNullItems.Count; batch += MaxInClauseItems)
                        {
                            if (batch > 0) _sql.Append(" OR ");
                            var batchItems = nonNullItems.Skip(batch).Take(MaxInClauseItems);
                            Visit(valueExpr);
                            _sql.Append(" IN (");
                            bool first = true;
                            foreach (var item in batchItems)
                            {
                                if (!first) _sql.Append(", ");
                                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                                _sql.AppendParameterizedValue(paramName, item, _paramSink);
                                first = false;
                            }
                            _sql.Append(")");
                        }
                        _sql.Append(")");
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    else
                    {
                        if (hasNulls) _sql.Append("(");
                        Visit(valueExpr);
                        _sql.Append(" IN (");
                        for (int i = 0; i < nonNullItems.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _sql.AppendParameterizedValue(paramName, nonNullItems[i], _paramSink);
                        }
                        _sql.Append(")");
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    return node;
                }
            }
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Any):
                        BuildExists(node.Arguments[0], node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null, negate: false);
                        return node;
                    case nameof(Queryable.All):
                        if (node.Arguments.Count < 2)
                            throw new NormQueryException("All() requires a predicate argument.");
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new NormQueryException("All() requires a predicate lambda expression.");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate: true);
                        return node;
                    case nameof(Queryable.Contains):
                        BuildIn(node.Arguments[0], node.Arguments[1]);
                        return node;
                    default:
                        throw new NormUnsupportedFeatureException($"Queryable method '{node.Method.Name}' is not supported.");
                }
            }
            var args = new List<string>();
            if (node.Object != null)
                args.Add(GetSql(node.Object));
            foreach (var a in node.Arguments)
                args.Add(GetSql(a));
            var fnSql = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, args.ToArray());
            if (fnSql != null)
            {
                _sql.Append(fnSql);
                return node;
            }
            var custom = node.Method.GetCustomAttribute<SqlFunctionAttribute>();
            if (custom != null)
            {
                var formatted = string.Format(custom.Format, args.ToArray());
                _sql.Append(formatted);
                return node;
            }
            throw new NormUnsupportedFeatureException($"Method '{node.Method.Name}' is not supported.");
        }
        /// <summary>
        /// Rewrites a navigation-aware aggregate call (e.g. <c>parent.Children.Any(...)</c>)
        /// into the equivalent <c>Queryable</c> shape that the existing translator can consume:
        /// <c>NormQueryable.Query&lt;Child&gt;(ctx).Where(c => c.FK == parent.PK).Any(...)</c>.
        /// The outer parameter reference inside the FK join condition is preserved so the
        /// translator emits a correlated subquery instead of an independent SELECT.
        /// </summary>
        /// <summary>
        /// Emits a correlated scalar subquery for the
        /// `parent.Children.Select(c =&gt; c.Value).Sum/Min/Max/Average()` shape:
        /// <code>(SELECT AGG(selector_sql) FROM ChildTable T_n WHERE T_n.FK = T_outer.PK)</code>
        /// The selector lambda is translated against the child mapping in a sub-visitor so
        /// arbitrary projection expressions (computed selectors, COALESCE chains, etc.) flow
        /// through naturally.
        /// </summary>
        private void EmitNavigationScalarAggregateSubquery(
            string aggregateName,
            ParameterExpression parentParam,
            (TableMapping Mapping, string Alias) parentInfo,
            TableMapping.Relation relation,
            LambdaExpression selectorLambda)
        {
            var sqlAgg = aggregateName switch
            {
                nameof(Queryable.Sum)     => "SUM",
                nameof(Queryable.Min)     => "MIN",
                nameof(Queryable.Max)     => "MAX",
                nameof(Queryable.Average) => "AVG",
                _ => throw new NormQueryException($"Unexpected navigation aggregate '{aggregateName}'.")
            };

            var childMapping = _ctx.GetMapping(relation.DependentType);
            var subAlias = $"T_nav_{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Translate the selector lambda against the child mapping. Routing through a
            // dedicated child-parameter binding lets nested member access / arithmetic /
            // COALESCE all flow through the regular expression-to-SQL pipeline.
            using var subVisitor = new ExpressionToSqlVisitor();
            var subCtx = new VisitorContext(
                _ctx, childMapping, _provider,
                selectorLambda.Parameters[0], subAlias,
                _parameterMappings, _compiledParams, _paramMap, _recursionDepth + 1, _paramIndex);
            subVisitor.Initialize(in subCtx);
            subVisitor.UseSharedParameterDictionary(_paramSink);
            var selectorSql = subVisitor.Translate(selectorLambda.Body);
            _paramIndex = subVisitor.ParamIndex;

            var fkCol = relation.ForeignKey.EscCol;
            var pkCol = relation.PrincipalKey.EscCol;
            _sql.Append("(SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
                .Append(" FROM ").Append(childMapping.EscTable).Append(' ').Append(_provider.Escape(subAlias))
                .Append(" WHERE ").Append(_provider.Escape(subAlias)).Append('.').Append(fkCol)
                .Append(" = ").Append(parentInfo.Alias).Append('.').Append(pkCol)
                .Append(')');
        }

        private MethodCallExpression RewriteNavigationAggregate(
            MethodCallExpression originalCall,
            ParameterExpression parentParam,
            TableMapping.Relation relation)
        {
            var depType = relation.DependentType;

            // Materialize the dependent IQueryable upfront so the sub-translator sees a
            // ConstantExpression<IQueryable<Child>> at the root - that is the shape its
            // VisitConstant recognizes as the query source. Building the expression as
            // `Expression.Call(NormQueryable.Query, ctxConstant)` would emit the DbContext
            // as a SQL parameter instead.
            var queryMethod = typeof(NormQueryable).GetMethod(nameof(NormQueryable.Query))!
                .MakeGenericMethod(depType);
            var dependentQuery = queryMethod.Invoke(null, new object[] { _ctx })!;
            var sourceExpr = (Expression)Expression.Constant(dependentQuery, typeof(IQueryable<>).MakeGenericType(depType));

            // Build the FK = PK predicate against the dependent's property.
            var childParam = Expression.Parameter(depType, "__nav_" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Expression fkAccess = Expression.Property(childParam, relation.ForeignKey.Prop);
            Expression pkAccess = Expression.Property(parentParam, relation.PrincipalKey.Prop);
            // Promote nullable mismatches so Expression.Equal type-checks (e.g. nullable FK
            // referring to a non-nullable PK).
            if (fkAccess.Type != pkAccess.Type)
            {
                var common = Nullable.GetUnderlyingType(fkAccess.Type) ?? fkAccess.Type;
                if (fkAccess.Type != common) fkAccess = Expression.Convert(fkAccess, common);
                if (pkAccess.Type != common) pkAccess = Expression.Convert(pkAccess, common);
            }
            var fkPredicate = Expression.Lambda(Expression.Equal(fkAccess, pkAccess), childParam);

            sourceExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                new[] { depType }, sourceExpr, Expression.Quote(fkPredicate));

            // Re-emit the aggregate call (Any/All/Count/LongCount) against the synthesized
            // Queryable source. The original predicate (if any) is the second arg.
            var methodName = originalCall.Method.Name;
            if (originalCall.Arguments.Count > 1)
            {
                var innerPredicate = StripQuotes(originalCall.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(
                        $"{methodName}() on a navigation collection requires a lambda predicate argument.");
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 2)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr, Expression.Quote(innerPredicate));
            }
            else
            {
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 1)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr);
            }
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            // Materialize any `NormQueryable.Query<T>(ctx)` calls inside the source into
            // ConstantExpression(IQueryable) so the sub-translator's VisitConstant picks up
            // the query as the entity source instead of emitting `@p<n>` for the ctx instance.
            source = QueryCallMaterializer.Materialize(source);
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            // Both Parameters and CompiledParameters use SEPARATE dicts/lists for the sub-
            // translator. QueryTranslator.Dispose() calls ParameterManager.Reset() which
            // Clear()s these collections — sharing them with the outer would wipe the
            // outer's accumulated params and compiled-param registrations the moment the
            // sub-translator goes out of `using`. Copy both back before dispose.
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), tempCompiled, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }
        private void BuildScalarCountSubquery(Expression source, LambdaExpression? predicate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), tempCompiled, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }

            // Rewrite the entity SELECT into SELECT COUNT(*) by replacing everything before the
            // first ` FROM `. Strip any trailing ORDER BY which is meaningless inside a scalar
            // subquery and disallowed by some providers.
            var sql = subPlan.Sql;
            var fromIdx = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIdx < 0)
                throw new NormQueryException("Could not rewrite Count() subquery: no FROM clause found.");
            var tail = sql.Substring(fromIdx);
            var orderIdx = tail.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderIdx >= 0)
                tail = tail.Substring(0, orderIdx);
            _sql.Append("(SELECT COUNT(*)");
            _sql.Append(tail);
            _sql.Append(")");
        }
        private void BuildIn(Expression source, Expression value)
        {
            // Extract expression from closure-captured IQueryable.
            if (TryGetConstantValue(source, out var srcConstValue) && srcConstValue is System.Linq.IQueryable iqSrc)
                source = iqSrc.Expression!;
            // Mirror BuildExists (line ~1584): materialize any `NormQueryable.Query<T>(ctx)`
            // calls inside the source into ConstantExpression(IQueryable) so the sub-translator's
            // VisitConstant picks up the query as the entity source instead of trying to bind
            // the DbContext as a SQL parameter (which throws "No mapping exists from object
            // type nORM.Core.DbContext"). Without this, `Where(p => ctx.Query<O>().Select(o
            // => o.Id).Contains(p.Id))` — the semi-join-via-Contains idiom — silently
            // fails to translate.
            source = QueryCallMaterializer.Materialize(source);

            // Compile-time null: ConstantExpression{null} OR Convert(null, T?) (UnaryExpression).
            bool isNullValue = (value is ConstantExpression { Value: null })
                || (value is UnaryExpression { NodeType: ExpressionType.Convert } ueNull
                    && ueNull.Operand is ConstantExpression { Value: null });
            if (isNullValue)
            {
                BuildNullExistsForContains(source);
                return;
            }

            // Build the IN (subquery) with a fresh correlated dict so inner lambda params get T0
            // regardless of what the outer _parameterMappings contains.
            // Use a separate tempParams dict: QueryTranslator.Dispose() clears its Parameters dict —
            // if shared with _params, collected params are lost.
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var freshCorrelatedForIn = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var tempParams = new Dictionary<string, object>();
            var tempCompiled = new List<string>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex,
                freshCorrelatedForIn, new HashSet<string>(), tempCompiled, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            // Copy collected params and compiled-param registrations to outer BEFORE
            // subTranslator.Dispose() clears those collections.
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in tempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }

            // SQL NULL IN (...) is UNKNOWN (not TRUE); emit null-safe OR pattern for nullable value types.
            bool isNullable = !value.Type.IsValueType || Nullable.GetUnderlyingType(value.Type) != null;

            if (!isNullable)
            {
                Visit(value);
                _sql.Append(" IN (");
                _sql.Append(subPlan.Sql);
                _sql.Append(")");
                return;
            }

            // Runtime nullable: (val IN (subq) OR (val IS NULL AND EXISTS(null-filtered subq)))
            var valueSql = GetSql(value);
            _sql.Append("(");
            _sql.Append(valueSql);
            _sql.Append(" IN (");
            _sql.Append(subPlan.Sql);
            _sql.Append(") OR (");
            _sql.Append(valueSql);
            _sql.Append(" IS NULL AND ");
            BuildNullExistsForContains(source);
            _sql.Append("))");
        }

        // Emits EXISTS(SELECT ... FROM source WHERE col IS NULL).
        // Uses a fresh correlated dict so the EXISTS translator starts clean with T0 for all params.
        private void BuildNullExistsForContains(Expression source)
        {
            // Walk back through Select calls to find the entity-level query and selector.
            LambdaExpression? innerSelector = null;
            var cursor = source;
            while (cursor is MethodCallExpression mce && mce.Method.Name == "Select")
            {
                innerSelector = StripQuotes(mce.Arguments[1]) as LambdaExpression;
                cursor = mce.Arguments[0];
            }

            // Append a WHERE col IS NULL filter at the entity level.
            Expression filteredSource;
            Type entityType;
            if (innerSelector == null)
            {
                // No Select: the source IS the entity query; filter entities where they are null.
                entityType = GetElementType(source);
                var p = Expression.Parameter(entityType, "__nc");
                var isNull = Expression.Equal(p, Expression.Constant(null, entityType));
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, source, Expression.Quote(Expression.Lambda(isNull, p)));
            }
            else
            {
                // Has Select: add a null-check on the projected column at the entity level.
                // WhereTranslator does not increment _joinCounter, so innerSelector.Parameters[0]
                // and cursor's lambda params all get T0 in the fresh-dict EXISTS translator.
                entityType = innerSelector.Parameters[0].Type;
                var nullCheck = Expression.Lambda(
                    Expression.Equal(innerSelector.Body, Expression.Constant(null, innerSelector.ReturnType)),
                    innerSelector.Parameters[0]);
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, cursor, Expression.Quote(nullCheck));
            }

            // Fresh correlated dict and separate tempParams so EXISTS translator never sees
            // stale aliases, and Dispose() clearing its Parameters dict doesn't affect _params.
            var freshCorrelated = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var rootType = GetRootElementType(filteredSource);
            var mapping = _ctx.GetMapping(rootType);
            var existsTempParams = new Dictionary<string, object>();
            var existsTempCompiled = new List<string>();
            using var existsTranslator = QueryTranslator.Create(
                _ctx, mapping, existsTempParams, _paramIndex,
                freshCorrelated, new HashSet<string>(),
                existsTempCompiled, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var existsPlan = existsTranslator.Translate(filteredSource);
            _paramIndex = existsTranslator.ParameterIndex;
            // Copy collected params and compiled-param registrations to outer BEFORE
            // existsTranslator.Dispose() clears those collections.
            foreach (var kvp in existsTempParams)
                _params[kvp.Key] = kvp.Value;
            foreach (var compiled in existsTempCompiled)
            {
                if (!_compiledParams.Contains(compiled))
                    _compiledParams.Add(compiled);
            }
            _sql.Append("EXISTS(");
            _sql.Append(existsPlan.Sql);
            _sql.Append(")");
        }

        /// <summary>
        /// Declaring types whose methods can be translated to SQL. Frozen at startup
        /// to avoid per-call allocation and to enable O(1) lookup.
        /// </summary>
        private static readonly FrozenSet<Type> s_safeDeclaringTypes = new HashSet<Type>
        {
            typeof(string), typeof(Math), typeof(DateTime), typeof(Convert),
            typeof(Enumerable), typeof(Queryable), typeof(Json),
            typeof(NormFunctions)
        }.ToFrozenSet();

        /// <summary>
        /// Replaces `NormQueryable.Query&lt;T&gt;(ctxConstant)` MethodCall nodes inside a
        /// sub-expression with a `ConstantExpression(IQueryable&lt;T&gt;)` so the
        /// QueryTranslator recognizes them as the query source. Used by BuildExists +
        /// related sub-translator entry points; the outer compiled-query path does the same
        /// materialization in ExpressionCompiler.QueryCallEvaluator.
        /// </summary>
        private sealed class QueryCallMaterializer : ExpressionVisitor
        {
            public static Expression Materialize(Expression e) => new QueryCallMaterializer().Visit(e)!;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType == typeof(NormQueryable) && node.Method.Name == nameof(NormQueryable.Query))
                {
                    var compiled = Expression.Lambda(node).Compile();
                    var queryable = compiled.DynamicInvoke();
                    return Expression.Constant(queryable, node.Type);
                }
                return base.VisitMethodCall(node);
            }
        }

        private static bool IsDateTimeLike(Type t)
        {
            var underlying = Nullable.GetUnderlyingType(t) ?? t;
            return underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset);
        }

        /// <summary>
        /// Emits the SQL for a TimeSpan member access on a `(end - start)` subtraction.
        /// Returns true when the member name maps to a unit conversion; false otherwise
        /// (the caller falls through to the normal member-resolution path).
        /// </summary>
        private bool TryEmitTimeSpanMember(string memberName, string endSql, string startSql)
        {
            var secondsSql = _provider.GetDateTimeDifferenceSecondsSql(endSql, startSql);
            // Total* return fractional values; Days/Hours/Minutes/Seconds are the integer
            // component matching System.TimeSpan's semantics (truncate toward zero).
            switch (memberName)
            {
                case nameof(TimeSpan.TotalSeconds):
                    _sql.Append(secondsSql);
                    return true;
                case nameof(TimeSpan.TotalMinutes):
                    _sql.Append('(').Append(secondsSql).Append(" / 60.0)");
                    return true;
                case nameof(TimeSpan.TotalHours):
                    _sql.Append('(').Append(secondsSql).Append(" / 3600.0)");
                    return true;
                case nameof(TimeSpan.TotalDays):
                    _sql.Append('(').Append(secondsSql).Append(" / 86400.0)");
                    return true;
                case nameof(TimeSpan.TotalMilliseconds):
                    _sql.Append('(').Append(secondsSql).Append(" * 1000.0)");
                    return true;
                case nameof(TimeSpan.Days):
                    _sql.Append("CAST(").Append(secondsSql).Append(" / 86400 AS INTEGER)");
                    return true;
                case nameof(TimeSpan.Hours):
                    _sql.Append("(CAST(").Append(secondsSql).Append(" / 3600 AS INTEGER) % 24)");
                    return true;
                case nameof(TimeSpan.Minutes):
                    _sql.Append("(CAST(").Append(secondsSql).Append(" / 60 AS INTEGER) % 60)");
                    return true;
                case nameof(TimeSpan.Seconds):
                    _sql.Append("(CAST(").Append(secondsSql).Append(" AS INTEGER) % 60)");
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Object-identity methods that must never be translated to SQL because they
        /// rely on CLR runtime semantics with no SQL equivalent.
        /// </summary>
        private static readonly FrozenSet<string> s_untranslatableMethods = new HashSet<string>
        {
            nameof(object.GetType), nameof(object.ToString), nameof(object.GetHashCode)
        }.ToFrozenSet();

        private static bool IsTranslatableMethod(MethodInfo method)
        {
            if (method.GetCustomAttribute<SqlFunctionAttribute>() != null)
                return true;
            if (method.DeclaringType == null || !s_safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            // System.Convert.ToString is intentionally translatable as CAST(... AS TEXT);
            // it's only the default object.ToString that has no SQL equivalent.
            if (method.DeclaringType == typeof(Convert) && method.Name == nameof(Convert.ToString))
                return true;
            return !s_untranslatableMethods.Contains(method.Name);
        }
        private Expression TranslateWithNullCheck(MethodCallExpression node)
        {
            if (node.Object == null) return base.VisitMethodCall(node);
            _sql.Append("(CASE WHEN ");
            Visit(node.Object);
            _sql.Append(" IS NULL THEN NULL ELSE ");
            _suppressNullCheck = true;
            var result = VisitMethodCall(node);
            _suppressNullCheck = false;
            _sql.Append(" END)");
            return result;
        }
        private bool RequiresNullCheck(MethodCallExpression node)
        {
            if (node.Object == null)
                return false;
            if (node.Method.DeclaringType == typeof(string))
                return false;
            return !node.Object.Type.IsValueType || Nullable.GetUnderlyingType(node.Object.Type) != null;
        }
        /// <summary>
        /// Attempts to extract a compile-time constant from the expression, catching
        /// expected reflection failures without propagating them to the caller.
        /// </summary>
        private static bool TryGetConstantValueSafe(Expression expr, out object? value)
        {
            try
            {
                return TryGetConstantValue(expr, out value);
            }
            catch (Exception ex) when (ex is TargetInvocationException or ArgumentException)
            {
                value = null;
                return false;
            }
        }
        private void AppendConstant(object? value, Type type)
        {
            var key = new ConstKey(value, type);
            if (_constParamMap.TryGetValue(key, out var existing))
            {
                _sql.Append(existing);
                return;
            }
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _sql.AppendParameterizedValue(paramName, value, _paramSink);
            if (_constParamMap.Count >= ConstParamMapLimit)
                _constParamMap.Clear();
            _constParamMap[key] = paramName;
        }
        private Expression CreateSafeParameter(object? value)
        {
            if (value is string str && str.Length > MaxInlineParameterLength)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"String parameter exceeds maximum length of {MaxInlineParameterLength} characters"));
            if (value is byte[] bytes && bytes.Length > MaxInlineParameterLength)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"Binary parameter exceeds maximum length of {MaxInlineParameterLength} bytes"));
            AppendConstant(value, value?.GetType() ?? typeof(object));
            // Returning a cached empty expression avoids allocating a new
            // Expression instance for each constant value translated. The
            // actual value has already been written directly to the
            // parameter collection in AppendConstant, so no further
            // expression tree representation is required here.
            return s_emptyExpression;
        }
        private enum LikeOperation
        {
            Contains,
            StartsWith,
            EndsWith
        }
        private readonly struct ConstKey : IEquatable<ConstKey>
        {
            public readonly object? Value;
            public readonly Type? Type;
            public ConstKey(object? value, Type? type)
            {
                Value = value;
                Type = type;
            }

            /// <summary>
            /// Determines equality with another <see cref="ConstKey"/> based on both the value and
            /// the associated type.
            /// </summary>
            public bool Equals(ConstKey other) => Equals(Value, other.Value) && Type == other.Type;

            /// <summary>
            /// Determines whether the specified object is equal to the current <see cref="ConstKey"/>.
            /// </summary>
            public override bool Equals(object? obj) => obj is ConstKey other && Equals(other);

            /// <summary>
            /// Generates a hash code combining the value and type components.
            /// </summary>
            public override int GetHashCode() => HashCode.Combine(Value, Type);
        }
        private string CreateSafeLikePattern(string value, LikeOperation operation)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;

            // DOS PROTECTION FIX: Validate pattern length to prevent database CPU spike
            // Extremely long LIKE patterns (millions of '%' chars) can cause severe performance issues
            const int MaxLikePatternLength = 5000;
            if (value.Length > MaxLikePatternLength)
            {
                throw new NormQueryException(
                    $"LIKE pattern too long ({value.Length} characters). Maximum allowed: {MaxLikePatternLength}. " +
                    $"Long LIKE patterns can cause database performance issues.");
            }

            var escaped = _provider.EscapeLikePattern(value);
            return operation switch
            {
                LikeOperation.Contains => $"%{escaped}%",
                LikeOperation.StartsWith => $"{escaped}%",
                LikeOperation.EndsWith => $"%{escaped}",
                _ => escaped
            };
        }
        private static Type GetRootElementType(Expression source)
        {
            while (source is MethodCallExpression mce)
            {
                // ctx.Query<T>() shows up as either an instance-style call (zero arguments) or
                // as the static extension-style NormQueryable.Query<T>(ctx) (one argument).
                // Both are the root of a query expression - stop walking and return T.
                if (mce.Method.Name == "Query"
                    && (mce.Arguments.Count == 0
                        || mce.Method.DeclaringType == typeof(NormQueryable)))
                    return GetElementType(mce);
                source = mce.Arguments[0];
            }
            return GetElementType(source);
        }
        /// <summary>
        /// Retrieves the parameter dictionary that has been populated while
        /// translating an expression tree to its SQL representation.
        /// </summary>
        /// <remarks>
        /// The returned dictionary contains parameter names and values that are
        /// emitted during translation.  The caller can reuse this collection when
        /// executing the generated SQL.
        /// </remarks>
        /// <returns>
        /// A reference to the internal dictionary of SQL parameters.  The
        /// contents should be treated as read-only by callers to avoid
        /// interfering with further translations.
        /// </returns>
        public Dictionary<string, object> GetParameters() => _params;
        private string GetSql(Expression expression)
        {
            var start = _sql.Length;
            Visit(expression);
            var segment = _sql.ToString(start, _sql.Length - start);
            _sql.Remove(start, _sql.Length - start);
            return segment;
        }
        /// <summary>
        /// Delegates to the shared ExpressionValueExtractor utility for consistent behavior.
        /// </summary>
        private static bool TryGetConstantValue(Expression e, out object? value, HashSet<Expression>? visited = null)
            => ExpressionValueExtractor.TryGetConstantValue(e, out value, visited);
        private static Expression StripQuotes(Expression e)
            => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }
            var iface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            if (iface != null) return iface.GetGenericArguments()[0];
            throw new ArgumentException($"Cannot determine element type from expression of type {type}");
        }
        private static void HandleStringContains(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.Contains, ignoreCase: false);

        private static void HandleStringStartsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.StartsWith, ignoreCase: false);

        private static void HandleStringEndsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.EndsWith, ignoreCase: false);

        private static void HandleStringContainsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.Contains, IsIgnoreCase(node.Arguments[1]));

        private static void HandleStringStartsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.StartsWith, IsIgnoreCase(node.Arguments[1]));

        private static void HandleStringEndsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.EndsWith, IsIgnoreCase(node.Arguments[1]));

        private static void HandleStringEqualsInstanceWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitEqualityPredicate(visitor, node.Object!, node.Arguments[0], IsIgnoreCase(node.Arguments[1]));

        private static void HandleStringEqualsStaticWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
            => EmitEqualityPredicate(visitor, node.Arguments[0], node.Arguments[1], IsIgnoreCase(node.Arguments[2]));

        private static void EmitLikePredicate(
            ExpressionToSqlVisitor visitor,
            Expression target,
            Expression patternExpr,
            LikeOperation op,
            bool ignoreCase)
        {
            var lhs = visitor.GetSql(target);
            if (ignoreCase) lhs = $"LOWER({lhs})";
            visitor._sql.Append(lhs).Append(" LIKE ");
            var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
            if (TryGetConstantValue(patternExpr, out var raw) && raw is string s)
            {
                // Pre-folded constant: bind the lowered pattern when ignoring case so the SQL
                // doesn't need to wrap it again at run time.
                var pattern = ignoreCase ? s.ToLowerInvariant() : s;
                visitor.AppendConstant(visitor.CreateSafeLikePattern(pattern, op), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
                return;
            }
            // Variable pattern: escape at runtime, fold to lower when ignoring case, and
            // bracket with %-wildcards according to the operation.
            var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(patternExpr));
            if (ignoreCase) escapedSql = $"LOWER({escapedSql})";
            var concat = op switch
            {
                LikeOperation.Contains => visitor._provider.GetConcatSql("'%'", visitor._provider.GetConcatSql(escapedSql, "'%'")),
                LikeOperation.StartsWith => visitor._provider.GetConcatSql(escapedSql, "'%'"),
                LikeOperation.EndsWith => visitor._provider.GetConcatSql("'%'", escapedSql),
                _ => escapedSql
            };
            visitor._sql.Append(concat);
            visitor._sql.Append($" ESCAPE '{escChar}'");
        }

        private static void EmitEqualityPredicate(
            ExpressionToSqlVisitor visitor,
            Expression left,
            Expression right,
            bool ignoreCase)
        {
            var lhs = visitor.GetSql(left);
            var rhs = visitor.GetSql(right);
            if (ignoreCase)
            {
                lhs = $"LOWER({lhs})";
                rhs = $"LOWER({rhs})";
            }
            visitor._sql.Append('(').Append(lhs).Append(" = ").Append(rhs).Append(')');
        }
        // ContainsTranslator, StartsWithTranslator, and EndsWithTranslator were consolidated into
        // _fastMethodHandlers. String methods are exclusively handled there.

        /// <summary>
        /// Directs the visitor to use the provided dictionary for parameter
        /// collection, allowing multiple visitors to share a common parameter
        /// store.
        /// </summary>
        /// <param name="shared">The dictionary to populate with parameters, or
        /// <c>null</c> to revert to the visitor's internal dictionary.</param>
        public void UseSharedParameterDictionary(Dictionary<string, object> shared)
        {
            _paramSink = shared ?? _params;
        }

        /// <summary>
        /// Registers a SQL expression for the <c>Key</c> property of a grouping parameter,
        /// so that subsequent <c>g.Key</c> accesses emit the correct SQL column reference.
        /// </summary>
        public void RegisterGroupingKey(ParameterExpression parameter, string keySql)
        {
            _groupingKeys[parameter] = keySql;
        }

        /// <summary>
        /// Translates a <c>GroupBy</c> method call into the corresponding SQL <c>GROUP BY</c>
        /// clause, registering grouping key bindings for downstream aggregate expressions.
        /// </summary>
        private void HandleGroupByMethod(MethodCallExpression node)
        {
            var keySelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            var keySql = GetSql(keySelector.Body);

            // Register grouping scope for downstream accesses to g.Key
            if (node.Arguments.Count > 2 && StripQuotes(node.Arguments[2]) is LambdaExpression resultSelector)
            {
                if (resultSelector.Parameters.Count > 1)
                {
                    RegisterGroupingKey(resultSelector.Parameters[1], keySql);
                    Visit(resultSelector.Body);
                }
                else
                {
                    Visit(resultSelector.Body);
                }
            }
            else
            {
                _sql.Append(keySql);
            }

            _sql.AppendGroupBy(keySql);
        }



        /// <summary>
        /// Quickly resets the visitor to a clean state so that it can be reused
        /// without allocating a new instance.
        /// </summary>
        /// <remarks>
        /// This method clears accumulated SQL, parameters, and internal caches
        /// while preserving preallocated buffers when possible.
        /// </remarks>
        public void FastReset()
        {
            if (_sql != null) _sql.Clear();
            _params.Clear();
            if (_paramSink != null && !ReferenceEquals(_paramSink, _params))
                _paramSink.Clear();
            _paramIndex = 0;
            _suppressNullCheck = false;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
        }

    }
}
