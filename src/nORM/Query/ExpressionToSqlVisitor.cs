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
    internal sealed partial class ExpressionToSqlVisitor : ExpressionVisitor, nORM.Internal.IResettable, IDisposable
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
        private static bool TryResolveStringCompareMode(Expression comparisonArg, out bool ignoreCase, out bool forceCaseSensitive)
        {
            ignoreCase = false;
            forceCaseSensitive = false;
            if (!TryGetConstantValue(comparisonArg, out var v))
                return false;

            if (v is bool boolIgnoreCase)
            {
                ignoreCase = boolIgnoreCase;
                forceCaseSensitive = !boolIgnoreCase;
                return true;
            }

            if (v is StringComparison sc)
            {
                ignoreCase = sc is StringComparison.OrdinalIgnoreCase
                    or StringComparison.InvariantCultureIgnoreCase
                    or StringComparison.CurrentCultureIgnoreCase;
                forceCaseSensitive = !ignoreCase;
                return true;
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
                // `list.IndexOf(x) [op] 0/-1` is the older spelling of `list.Contains(x)`.
                // Rewrite to a Contains call (optionally negated) so the existing
                // d97c5f0 IN-list handler picks it up. Without this, IndexOf on a
                // generic-collection receiver would fail the IsTranslatableMethod gate
                // and throw "Method 'IndexOf' cannot be translated to SQL".
                var asContains = TryRewriteIndexOfToContains(node);
                if (asContains != null)
                {
                    return Visit(asContains);
                }
            }
            if (TryEmitDateTimeOffsetLiteralComparison(node))
                return node;

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
                // DateTimeOffset col vs DateTime literal: SQLite stores DTOs as
                // text with an offset suffix and the literal binds as offset-less
                // text, so naive equality misses rows storing the same UTC instant
                // in a different offset. Lower to UTC epoch-microsecond comparison via
                // a provider hook. .NET's DateTime→DateTimeOffset implicit conv
                // treats Utc-kind as offset 0 and Unspecified/Local as the local
                // offset. LocalDateTime member access uses a full offset-range
                // CASE expression; DateTime literals remain a single value.
                if (TryEmitDateTimeOffsetLiteralComparison(node))
                    return node;

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

            // C# `+` on string operands is concatenation, not arithmetic. The
            // generic `+ ` emit below produces SQL numeric addition which on
            // SQLite coerces TEXT to 0 (silent "0" results for every row).
            // Route through the provider's concat SQL (`||` on SQLite,
            // `CONCAT(...)` on SQL Server/MySQL) when either operand is string.
            if (node.NodeType == ExpressionType.Add
                && (node.Left.Type == typeof(string) || node.Right.Type == typeof(string)))
            {
                var concatLeftSql = GetSql(node.Left);
                var concatRightSql = GetSql(node.Right);
                _sql.Append(_provider.GetConcatSql(concatLeftSql, concatRightSql));
                return node;
            }

            // DateTime + TimeSpan COLUMN shift in Where -- mirror of
            // SCV's 7f91efc branch. Without this SQL '+' on TEXT coerces
            // both operands to numeric (returning garbage like 2027 from
            // "2026-05-24 09:00:00.0000000" + "01:00:00") and the predicate
            // evaluates against nonsense. Parse the sub-day 'HH:mm:ss'
            // TimeSpan-column text via SUBSTR + CAST and construct the
            // strftime modifier dynamically. Sub-day TimeSpan only per
            // memory item b17440e.
            // DateTime AND DateTimeOffset use the same date-arithmetic emit path
            // (both store comparable text on SQLite and have native temporal types
            // on other providers). Treat them uniformly.
            static bool IsDateTimeOrOffset(Type t)
                => t == typeof(DateTime) || t == typeof(DateTimeOffset);
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && IsDateTimeOrOffset(Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeSpan)
                && !TryGetConstantValue(node.Right, out _))
            {
                var lhsSql = GetSql(node.Left);
                var rhsSql = GetSql(node.Right);
                // Provider hook handles TimeSpan storage format per provider:
                // SQLite parses 'HH:mm:ss' text; SqlServer / Postgres / MySQL
                // use native TIME/INTERVAL operators (no text parsing). Avoids
                // emitting SQLite-specific substr() on non-SQLite providers.
                var subtract = node.NodeType == ExpressionType.Subtract;
                var dateArithSql = _provider.AddTimeSpanColumnToDateTimeSql(lhsSql, rhsSql, subtract);
                if (dateArithSql != null)
                {
                    _sql.Append(dateArithSql);
                    return node;
                }
                throw new NormUnsupportedFeatureException(
                    $"{_provider.GetType().Name} does not implement AddTimeSpanColumnToDateTimeSql; " +
                    "DateTime/Offset +/- TimeSpan column arithmetic in WHERE requires this provider hook.");
            }
            // DateTime/DateTimeOffset + constant TimeSpan -- folds the rhs span via
            // TryGetConstantValue (closure MemberExpression carrying a TimeSpan value)
            // and routes through AddSecondsToDateTimeSql for the provider-native shift.
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && IsDateTimeOrOffset(Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeSpan)
                && node.Right is MemberExpression rhsMember
                && TryGetConstantValue(rhsMember, out var rhsTs)
                && rhsTs is TimeSpan span)
            {
                // ParameterValueExtractor walks EVERY closure-captured
                // MemberExpression in the predicate and appends a value to its
                // value-list in document order. Compiled-param names (@cp0,
                // @cp1, ...) are assigned in the order ETSV emits them. If we
                // fold the rhs TimeSpan inline without adding a placeholder
                // compiled-param, the extractor still produces a value for it
                // -- shifting every subsequent @cpN's value by one slot. The
                // anchor RHS of an outer relop would then receive the shift
                // value, returning silently wrong rows. Reserve a placeholder
                // slot so the extractor's index aligns with ETSV's @cpN names.
                var placeholderParam = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                _params[placeholderParam] = DBNull.Value;
                _compiledParams.Add(placeholderParam);
                var leftSql = GetSql(node.Left);
                var seconds = span.TotalSeconds;
                if (node.NodeType == ExpressionType.Subtract) seconds = -seconds;
                var secondsLiteral = seconds.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                var dateArithSql = _provider.AddSecondsToDateTimeSql(leftSql, secondsLiteral);
                if (dateArithSql != null)
                {
                    _sql.Append(dateArithSql);
                    return node;
                }
                throw new NormUnsupportedFeatureException(
                    $"{_provider.GetType().Name} does not implement AddSecondsToDateTimeSql; " +
                    "DateTime + constant TimeSpan arithmetic requires this provider hook.");
            }

            // Decimal comparisons / arithmetic on TEXT-stored decimal columns
            // must coerce both operands to REAL or SQLite performs lex compare
            // ('10.5' < '2' because '1' < '2'). Wrap both sides with CAST AS
            // REAL when either operand is decimal-typed and the op is a
            // comparison or numeric arithmetic. CAST(int AS REAL) is identity
            // with .0 so non-decimal pairings still round-trip.
            //
            // PRECISION TRADEOFF: REAL is IEEE-754 binary double. Numeric
            // ordering/comparison via CAST AS REAL is correct for practical
            // magnitudes (the only alternative -- TEXT lex compare -- is
            // silently wrong on every mixed-magnitude case). Arithmetic
            // results carry floating-point rounding (e.g. 0.01m is not
            // exactly representable in binary). Exact decimal semantics on
            // SQLite would require a different storage/aggregate strategy;
            // the SqlServer/Postgres/MySQL providers use native DECIMAL.
            bool isDecimalComparable = node.NodeType is
                    ExpressionType.Equal or ExpressionType.NotEqual
                    or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                    or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                    or ExpressionType.Add or ExpressionType.Subtract
                    or ExpressionType.Multiply or ExpressionType.Divide
                    or ExpressionType.Modulo;
            bool leftIsDecimal = (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(decimal);
            bool rightIsDecimal = (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(decimal);
            if (isDecimalComparable && (leftIsDecimal || rightIsDecimal))
            {
                // Route through the provider hook: SqliteProvider wraps with
                // CAST AS REAL (TEXT storage needs numeric coercion); other
                // providers' native DECIMAL is precise so the hook is identity
                // and the SQL shape stays `(left op right)`.
                var leftSqlD = _provider.NormalizeDecimalForCompare(GetSql(node.Left));
                var rightSqlD = _provider.NormalizeDecimalForCompare(GetSql(node.Right));
                _sql.Append('(').Append(leftSqlD);
                _sql.Append(node.NodeType switch
                {
                    ExpressionType.Equal => " = ",
                    ExpressionType.NotEqual => " <> ",
                    ExpressionType.GreaterThan => " > ",
                    ExpressionType.GreaterThanOrEqual => " >= ",
                    ExpressionType.LessThan => " < ",
                    ExpressionType.LessThanOrEqual => " <= ",
                    ExpressionType.Add => " + ",
                    ExpressionType.Subtract => " - ",
                    ExpressionType.Multiply => " * ",
                    ExpressionType.Divide => " / ",
                    ExpressionType.Modulo => " % ",
                    _ => throw new InvalidOperationException()
                });
                _sql.Append(rightSqlD).Append(')');
                return node;
            }

            // DateTime / DateTimeOffset comparison: SQLite stores as TEXT and lex-compares
            // ISO strings, which mis-orders rows with mixed timezone offsets ('+02:00'
            // suffix vs 'Z'). Other providers' native DATETIME types compare correctly so
            // NormalizeDateTimeForCompare is identity for them.
            //
            // CRITICAL: wrap ONLY operands that resolve to column references. Parameter
            // operands are bound from .NET DateTime values whose canonical serialization
            // ('yyyy-MM-dd HH:mm:ss.fffffff' for Microsoft.Data.Sqlite) already lex-
            // compares correctly against datetime()'s output. Wrapping a parameter side
            // breaks because SQLite's datetime() returns EMPTY for max-precision inputs
            // like DateTime.MaxValue ('9999-12-31 23:59:59.9999999') -- the .9999999
            // fractional overflows when added to the max year, producing NULL/empty and
            // collapsing the comparison to UNKNOWN/false.
            // Only ORDER-sensitive operators get the datetime() wrap. Equality (= / <>)
            // already works against the canonical storage format (callers must serialize
            // DateTimeOffset with offset suffix; wrapping the column would strip the
            // suffix and break exact-match).
            bool isDateCmp = node.NodeType is
                    ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                    or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;
            Type leftDtType = Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type;
            Type rightDtType = Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type;
            bool leftIsDt = leftDtType == typeof(DateTime) || leftDtType == typeof(DateTimeOffset);
            bool rightIsDt = rightDtType == typeof(DateTime) || rightDtType == typeof(DateTimeOffset);
            bool leftIsCol = IsColumnReference(node.Left);
            bool rightIsCol = IsColumnReference(node.Right);
            if (isDateCmp && leftIsDt && rightIsDt && (leftIsCol || rightIsCol))
            {
                var leftSql = GetSql(node.Left);
                var rightSql = GetSql(node.Right);
                _sql.Append("(")
                    .Append(leftIsCol ? _provider.NormalizeDateTimeForCompare(leftSql) : leftSql);
                _sql.Append(node.NodeType switch
                {
                    ExpressionType.Equal => " = ",
                    ExpressionType.NotEqual => " <> ",
                    ExpressionType.GreaterThan => " > ",
                    ExpressionType.GreaterThanOrEqual => " >= ",
                    ExpressionType.LessThan => " < ",
                    ExpressionType.LessThanOrEqual => " <= ",
                    _ => throw new InvalidOperationException()
                });
                _sql.Append(rightIsCol ? _provider.NormalizeDateTimeForCompare(rightSql) : rightSql).Append(")");
                return node;
            }

            // TimeSpan comparison (column vs column, column vs parameter, etc.).
            // SQLite stores TimeSpan as canonical 'c' TEXT; lex-ordering silently
            // mis-sorts multi-day durations ("10.00:00:00" < "9.23:59:59" lex but
            // 10 days > 9 days 23 hours). Both sides must be converted to fractional
            // seconds via NormalizeTimeSpanForCompare so the comparison is numeric.
            // Parameters also carry canonical TEXT (Microsoft.Data.Sqlite binds TimeSpan
            // as text), so the same conversion applies to them.
            // SQL Server / Postgres / MySQL: NormalizeTimeSpanForCompare is identity —
            // their native TIME / INTERVAL types compare correctly without conversion.
            bool isOrderOrEqualCmp = node.NodeType is
                ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                or ExpressionType.Equal or ExpressionType.NotEqual;
            Type leftTsType = Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type;
            Type rightTsType = Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type;
            bool leftIsTs = leftTsType == typeof(TimeSpan);
            bool rightIsTs = rightTsType == typeof(TimeSpan);
            if (isOrderOrEqualCmp && (leftIsTs || rightIsTs))
            {
                var leftSqlTs = GetSql(node.Left);
                var rightSqlTs = GetSql(node.Right);
                var leftNorm  = leftIsTs  ? _provider.NormalizeTimeSpanForCompare(leftSqlTs)  : leftSqlTs;
                var rightNorm = rightIsTs ? _provider.NormalizeTimeSpanForCompare(rightSqlTs) : rightSqlTs;
                _sql.Append('(').Append(leftNorm);
                _sql.Append(node.NodeType switch
                {
                    ExpressionType.Equal              => " = ",
                    ExpressionType.NotEqual           => " <> ",
                    ExpressionType.GreaterThan        => " > ",
                    ExpressionType.GreaterThanOrEqual => " >= ",
                    ExpressionType.LessThan           => " < ",
                    ExpressionType.LessThanOrEqual    => " <= ",
                    _ => throw new InvalidOperationException()
                });
                _sql.Append(rightNorm).Append(')');
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
                // SQLite, SQL Server, MySQL, and PostgreSQL all support << and >>
                // bitwise-shift operators with .NET-equivalent semantics, so emit
                // directly rather than forcing the multiply-rewrite workaround.
                ExpressionType.LeftShift => " << ",
                ExpressionType.RightShift => " >> ",
                // ExclusiveOr is handled above via DatabaseProvider.GetBitwiseXorSql
                _ => throw new NormUnsupportedFeatureException(
                    $"Binary operator '{node.NodeType}' has no portable SQL equivalent. " +
                    "For Power, use `Math.Pow(x, n)` which lowers to the provider's " +
                    "POWER / POW function.")
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
                TableMapping.TryGetMemberAccessRoot(columnMember, out var columnParameter) &&
                _parameterMappings.TryGetValue(columnParameter, out var mappedParameter) &&
                mappedParameter.Mapping.TryGetColumnForMemberAccess(columnMember, out var column))
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
                !TableMapping.TryGetMemberAccessRoot(member, out var parameter) ||
                !_parameterMappings.TryGetValue(parameter, out var info) ||
                !info.Mapping.TryGetColumnForMemberAccess(member, out _))
            {
                return false;
            }

            var columnSql = GetSql(member);
            _sql.Append(_provider.FormatBooleanPredicate(columnSql, expectedValue));
            return true;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == null && node.Member.DeclaringType == typeof(Guid) && node.Member.Name == nameof(Guid.Empty))
            {
                _sql.Append('\'').Append(Guid.Empty.ToString("D", CultureInfo.InvariantCulture)).Append('\'');
                return node;
            }
            // dtoCol.LocalDateTime — sister of the projection-side handler in
            // SelectClauseVisitor (1f06ac1). Per-instant local timezone semantics are lowered through
            // DateTimeOffsetLocalTimeSql using TimeZoneInfo.Local offset ranges.
            if (node.Member.Name == nameof(DateTimeOffset.LocalDateTime)
                && node.Member.DeclaringType == typeof(DateTimeOffset)
                && node.Expression != null
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(DateTimeOffset))
            {
                var dtoSql = GetSql(node.Expression);
                _sql.Append(DateTimeOffsetLocalTimeSql.Build(_provider, dtoSql));
                return node;
            }
            // `entity.GetType().Name/FullName/Namespace/AssemblyQualifiedName` --
            // fold to the receiver's declared compile-time type. Mirror of the
            // SCV fold in 149fa9a so the same expression works in WHERE. Type.*
            // members are declared on MemberInfo (Type's base), so check the
            // receiver type rather than node.Member.DeclaringType.
            if (node.Expression is MethodCallExpression gtCall
                && gtCall.Method.Name == "GetType"
                && gtCall.Arguments.Count == 0
                && gtCall.Object != null
                && typeof(Type).IsAssignableFrom(gtCall.Type))
            {
                var declaredType = gtCall.Object.Type;
                string? typeNameLiteral = node.Member.Name switch
                {
                    nameof(Type.Name) => declaredType.Name,
                    nameof(Type.FullName) => declaredType.FullName,
                    nameof(Type.Namespace) => declaredType.Namespace,
                    nameof(Type.AssemblyQualifiedName) => declaredType.AssemblyQualifiedName,
                    _ => null
                };
                if (typeNameLiteral != null)
                {
                    _sql.Append('\'').Append(typeNameLiteral.Replace("'", "''")).Append('\'');
                    return node;
                }
            }
            // TimeSpan member access whose receiver is a DateTime or TimeOnly
            // subtraction lowers to a fractional-seconds scalar via the provider, then
            // a unit-conversion divide. Examples: (end - start).TotalHours,
            // .TotalMinutes, .TotalSeconds, .TotalDays, .Days, .Hours, .Minutes,
            // .Seconds. Both nullable and non-nullable receivers.
            if (node.Expression is BinaryExpression timeSpanBinary
                && timeSpanBinary.NodeType == ExpressionType.Subtract
                && node.Expression.Type == typeof(TimeSpan))
            {
                if (IsDateTimeLike(timeSpanBinary.Left.Type)
                    && IsDateTimeLike(timeSpanBinary.Right.Type)
                    && TryEmitTimeSpanMember(node.Member.Name, GetSql(timeSpanBinary.Left), GetSql(timeSpanBinary.Right), useTimeOnly: false))
                {
                    return node;
                }
                if (IsTimeOnly(timeSpanBinary.Left.Type)
                    && IsTimeOnly(timeSpanBinary.Right.Type)
                    && TryEmitTimeSpanMember(node.Member.Name, GetSql(timeSpanBinary.Left), GetSql(timeSpanBinary.Right), useTimeOnly: true))
                {
                    return node;
                }
            }

            // TimeSpan stored-column member access: r.Duration.TotalSeconds, .Days, etc.
            // The subtraction path above handles (end - start).MemberName. This path handles
            // a mapped column (or any non-subtraction expression) whose CLR type is TimeSpan.
            // GetTimeSpanColumnSecondsSql wraps the column reference in provider-specific SQL
            // that evaluates to total fractional seconds, then TryEmitTimeSpanMemberFromSeconds
            // applies the same unit-conversion math as the subtraction path.
            if (node.Expression != null
                && !(node.Expression is BinaryExpression { NodeType: ExpressionType.Subtract })
                && (Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type) == typeof(TimeSpan))
            {
                var storedColSql = GetSql(node.Expression);
                var secondsSql = _provider.GetTimeSpanColumnSecondsSql(storedColSql);
                if (TryEmitTimeSpanMemberFromSeconds(node.Member.Name, secondsSql))
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
            if (node.Expression is ParameterExpression groupParameter &&
                _parameterMappings.TryGetValue(groupParameter, out _) &&
                _groupingKeys.TryGetValue(groupParameter, out var groupKey) &&
                node.Member.Name == "Key")
            {
                _sql.Append(groupKey);
                return node;
            }
            if (TableMapping.TryGetMemberAccessRoot(node, out var mappedRoot) &&
                _parameterMappings.TryGetValue(mappedRoot, out var info) &&
                info.Mapping.TryGetColumnForMemberAccess(node, out var column))
            {
                // Table aliases are generated internally and escaped when created,
                // allowing them to be used safely without additional validation.
                _sql.Append($"{info.Alias}.{column.EscCol}");
                return node;
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

        // Emit CASE WHEN col = <int> THEN '<name>' ... ELSE CAST(col AS TEXT) END for
        // an enum-typed receiver. The ELSE branch mirrors .NET's Enum.ToString behavior
        // on undefined values (returns the integer as text). Names are quote-escaped
        // by doubling single quotes so values like O'Brien (hypothetical) round-trip.
        // Shared between projection (SelectClauseVisitor) and predicates (this class)
        // via the static helper below.
        internal void EmitEnumToStringCase(string columnSql, Type enumType)
        {
            _sql.Append(BuildEnumToStringCase(_provider, columnSql, enumType));
        }

        internal static string BuildEnumToStringCase(DatabaseProvider provider, string columnSql, Type enumType)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("(CASE");
            var values = Enum.GetValues(enumType);
            var underlyingType = Enum.GetUnderlyingType(enumType);
            foreach (var value in values)
            {
                var name = Enum.GetName(enumType, value!) ?? value!.ToString()!;
                var underlying = Convert.ChangeType(value, underlyingType, System.Globalization.CultureInfo.InvariantCulture)!;
                sb.Append(" WHEN ").Append(columnSql).Append(" = ").Append(underlying)
                  .Append(" THEN '").Append(name.Replace("'", "''")).Append('\'');
            }
            sb.Append(" ELSE ").Append(provider.GetToStringSql(columnSql)).Append(" END)");
            return sb.ToString();
        }

        /// <summary>
        /// Sister of <see cref="BuildEnumToStringCase"/>: emits a CASE-WHEN cascade
        /// mapping each enum member's name (case-sensitive, matching
        /// <see cref="Enum.Parse{T}(string)"/>'s default) to its underlying integer
        /// value. Used to translate <c>Enum.Parse&lt;T&gt;(stringColumn)</c> in
        /// WHERE / projection. Result rows yielding NULL from the ELSE branch
        /// would surface as a default(enum) — matches the behaviour callers tend
        /// to expect when the data is dirty; strict Parse callers should sanitize
        /// the column server-side first.
        /// </summary>
        internal static string BuildStringToEnumCase(string columnSql, Type enumType)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("(CASE");
            var values = Enum.GetValues(enumType);
            var underlyingType = Enum.GetUnderlyingType(enumType);
            foreach (var value in values)
            {
                var name = Enum.GetName(enumType, value!) ?? value!.ToString()!;
                var underlying = Convert.ChangeType(value, underlyingType, System.Globalization.CultureInfo.InvariantCulture)!;
                sb.Append(" WHEN ").Append(columnSql).Append(" = '")
                  .Append(name.Replace("'", "''")).Append("' THEN ").Append(underlying);
            }
            sb.Append(" ELSE NULL END)");
            return sb.ToString();
        }

        // Fold a NewExpression whose arguments are all compile-time constants
        // (or evaluate to constants via Expression.Lambda().Compile()) into a
        // single bound parameter at translation time. Without this, an inline
        // `new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc)` on the RHS of
        // a Where comparison emits the raw constructor argument list as bound
        // parameters into the SQL ("col > @p0, @p1, @p2, ..."), producing
        // SQLite syntax errors. The same applies to user-defined value types
        // with constant args.
        protected override Expression VisitNew(NewExpression node)
        {
            if (node.Type == typeof(string)) return base.VisitNew(node);
            // new DateTime(year, month, day) / new DateOnly(year, month, day) /
            // new TimeOnly(hour, minute, second) with at least one non-constant
            // argument can't be folded but each provider has a native
            // from-parts primitive. Route through the provider hook before
            // the constant-fold loop below.
            if ((node.Type == typeof(DateTime) || node.Type == typeof(DateOnly) || node.Type == typeof(TimeOnly))
                && node.Arguments.Count == 3
                && node.Constructor is { } fromPartsCtor
                && fromPartsCtor.GetParameters().Length == 3
                && fromPartsCtor.GetParameters()[0].ParameterType == typeof(int)
                && fromPartsCtor.GetParameters()[1].ParameterType == typeof(int)
                && fromPartsCtor.GetParameters()[2].ParameterType == typeof(int))
            {
                bool anyNonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        anyNonConst = true;
                        break;
                    }
                if (anyNonConst)
                {
                    var aSql = GetSql(node.Arguments[0]);
                    var bSql = GetSql(node.Arguments[1]);
                    var cSql = GetSql(node.Arguments[2]);
                    _sql.Append(node.Type == typeof(DateTime)
                        ? _provider.GetDateTimeFromPartsSql(aSql, bSql, cSql)
                        : node.Type == typeof(DateOnly)
                            ? _provider.GetDateOnlyFromPartsSql(aSql, bSql, cSql)
                            : _provider.GetTimeOnlyFromPartsSql(aSql, bSql, cSql));
                    return node;
                }
            }

            // 6-arg new DateTime(y, m, d, h, mi, s) with at least one column arg.
            if (node.Type == typeof(DateTime)
                && node.Arguments.Count == 6
                && node.Constructor is { } dt6Ctor
                && dt6Ctor.GetParameters() is { Length: 6 } dt6Params
                && dt6Params[0].ParameterType == typeof(int)
                && dt6Params[1].ParameterType == typeof(int)
                && dt6Params[2].ParameterType == typeof(int)
                && dt6Params[3].ParameterType == typeof(int)
                && dt6Params[4].ParameterType == typeof(int)
                && dt6Params[5].ParameterType == typeof(int))
            {
                bool any6NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any6NonConst = true;
                        break;
                    }
                if (any6NonConst)
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    _sql.Append(_provider.GetDateTimeFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql));
                    return node;
                }
            }

            // 4-arg new TimeOnly(h, m, s, ms) with at least one column arg.
            if (node.Type == typeof(TimeOnly)
                && node.Arguments.Count == 4
                && node.Constructor is { } to4Ctor
                && to4Ctor.GetParameters() is { Length: 4 } to4Params
                && to4Params[0].ParameterType == typeof(int)
                && to4Params[1].ParameterType == typeof(int)
                && to4Params[2].ParameterType == typeof(int)
                && to4Params[3].ParameterType == typeof(int))
            {
                bool any4NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any4NonConst = true;
                        break;
                    }
                if (any4NonConst)
                {
                    var hSql4 = GetSql(node.Arguments[0]);
                    var miSql4 = GetSql(node.Arguments[1]);
                    var sSql4 = GetSql(node.Arguments[2]);
                    var msSql4 = GetSql(node.Arguments[3]);
                    _sql.Append(_provider.GetTimeOnlyFromPartsSql(hSql4, miSql4, sSql4, msSql4));
                    return node;
                }
            }

            // 7-arg new DateTimeOffset(y, m, d, h, mi, s, TimeSpan offset) with at least one
            // column date/time part and a compile-time constant offset. Lower to canonical
            // ISO-8601 text the materialiser parses via DateTimeOffset.Parse — every provider
            // can emit text without needing a native DTO-from-parts function.
            if (node.Type == typeof(DateTimeOffset)
                && node.Arguments.Count == 7
                && node.Constructor is { } dto7Ctor
                && dto7Ctor.GetParameters() is { Length: 7 } dto7Params
                && dto7Params[0].ParameterType == typeof(int)
                && dto7Params[1].ParameterType == typeof(int)
                && dto7Params[2].ParameterType == typeof(int)
                && dto7Params[3].ParameterType == typeof(int)
                && dto7Params[4].ParameterType == typeof(int)
                && dto7Params[5].ParameterType == typeof(int)
                && dto7Params[6].ParameterType == typeof(TimeSpan))
            {
                bool anyDtoPartNonConst = false;
                for (int i = 0; i < 6; i++)
                {
                    var a = node.Arguments[i];
                    if (a is not ConstantExpression && !TryGetConstantValue(a, out _))
                    {
                        anyDtoPartNonConst = true;
                        break;
                    }
                }
                if (anyDtoPartNonConst && TryGetTimeSpanConstantArg(node.Arguments[6], out var tsOff))
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    _sql.Append(_provider.GetDateTimeOffsetFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql, tsOff));
                    return node;
                }
            }

            // 7-arg new DateTime(y, m, d, h, mi, s, ms) with at least one column arg.
            if (node.Type == typeof(DateTime)
                && node.Arguments.Count == 7
                && node.Constructor is { } dt7Ctor
                && dt7Ctor.GetParameters() is { Length: 7 } dt7Params
                && dt7Params[0].ParameterType == typeof(int)
                && dt7Params[1].ParameterType == typeof(int)
                && dt7Params[2].ParameterType == typeof(int)
                && dt7Params[3].ParameterType == typeof(int)
                && dt7Params[4].ParameterType == typeof(int)
                && dt7Params[5].ParameterType == typeof(int)
                && dt7Params[6].ParameterType == typeof(int))
            {
                bool any7NonConst = false;
                foreach (var arg in node.Arguments)
                    if (arg is not ConstantExpression && !TryGetConstantValue(arg, out _))
                    {
                        any7NonConst = true;
                        break;
                    }
                if (any7NonConst)
                {
                    var ySql = GetSql(node.Arguments[0]);
                    var mSql = GetSql(node.Arguments[1]);
                    var dSql = GetSql(node.Arguments[2]);
                    var hSql = GetSql(node.Arguments[3]);
                    var miSql = GetSql(node.Arguments[4]);
                    var sSql = GetSql(node.Arguments[5]);
                    var msSql = GetSql(node.Arguments[6]);
                    _sql.Append(_provider.GetDateTimeFromPartsSql(ySql, mSql, dSql, hSql, miSql, sSql, msSql));
                    return node;
                }
            }
            foreach (var a in node.Arguments)
            {
                if (a is not ConstantExpression && !TryGetConstantValue(a, out _))
                    return base.VisitNew(node);
            }
            try
            {
                var lambda = System.Linq.Expressions.Expression.Lambda(node).Compile();
                var value = lambda.DynamicInvoke();
                // Reserve a placeholder compiled-param slot for every closure-
                // captured MemberExpression argument that the fold consumed.
                // ParameterValueExtractor walks each closure MemberExpression
                // in document order and adds a value to its list; folding the
                // NewExpression inline without compensating leaves N closure
                // values orphaned at the front of the value array, shifting
                // every subsequent @cp binding by N. Same fix shape as
                // 407e03d / eeff6e7 / cf39b61 / 04a0003.
                foreach (var arg in node.Arguments)
                {
                    if (arg is MemberExpression)
                    {
                        var placeholder = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                        _params[placeholder] = DBNull.Value;
                        _compiledParams.Add(placeholder);
                    }
                }
                AppendConstant(value, node.Type);
                return node;
            }
            catch
            {
                return base.VisitNew(node);
            }
        }

        private string GetRegexPatternSql(Expression patternExpr)
        {
            if (_provider is SqlServerProvider
                && TryGetConstantValue(patternExpr, out var raw)
                && raw is string pattern)
            {
                return "\'" + pattern.Replace("\'", "\'\'") + "\'";
            }

            return GetSql(patternExpr);
        }

        private string GetRegexReplacementSql(Expression replacementExpr)
        {
            if (_provider is SqlServerProvider
                && TryGetConstantValue(replacementExpr, out var raw)
                && raw is string replacement)
            {
                return "\'" + replacement.Replace("\'", "\'\'") + "\'";
            }

            return GetSql(replacementExpr);
        }

        private static bool TryGetConstantValue(Expression expr, out object? value)
        {
            value = null;
            if (expr is ConstantExpression c) { value = c.Value; return true; }
            try
            {
                value = System.Linq.Expressions.Expression.Lambda(expr).Compile().DynamicInvoke();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Folds a <see cref="TimeSpan"/>-typed argument to its runtime value. Used by
        /// the 7-arg <c>new DateTimeOffset(...)</c> branch whose offset arg must be a
        /// compile-time constant. ETSV-side already permits arbitrary lambda compilation
        /// (the closure fold path at the bottom of <c>VisitNew</c> does the same).
        /// </summary>
        private static bool TryGetTimeSpanConstantArg(Expression expr, out TimeSpan value)
        {
            value = default;
            if (TryGetConstantValue(expr, out var box) && box is TimeSpan ts)
            {
                value = ts;
                return true;
            }
            return false;
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
            if (node.NodeType is ExpressionType.Negate or ExpressionType.NegateChecked)
            {
                var operandType = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                if (operandType == typeof(TimeSpan))
                {
                    var operandSql = GetSql(node.Operand);
                    _sql.Append("(-1.0 * ").Append(_provider.GetTimeSpanColumnSecondsSql(operandSql)).Append(')');
                    return node;
                }

                _sql.Append("-(");
                Visit(node.Operand);
                _sql.Append(')');
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
            if (TryTranslateDateOnlyAdd(node)
                || TryTranslateTimeOnlyAdd(node)
                || TryTranslateRegexMethod(node)
                || TryTranslateStringJoin(node))
            {
                return node;
            }
            // Fast path: common string methods (Contains, StartsWith, EndsWith) are handled
            // directly via pre-built delegates, bypassing the general method translation pipeline.
            if (_fastMethodHandlers.TryGetValue(node.Method, out var handler))
            {
                handler(this, node);
                return node;
            }
            if (TryTranslateNullableGetValueOrDefault(node)
                || TryTranslateToStringCall(node)
                || TryTranslateCharMethod(node)
                || TryTranslateNumericOrGuidParse(node)
                || TryTranslateEnumMethod(node))
            {
                return node;
            }
            if (!IsTranslatableMethod(node.Method))
                throw new NormUnsupportedFeatureException(
                    $"Method '{node.Method.Name}' cannot be translated to SQL by the nORM v1 query translator. " +
                    "See docs/linq-support.md for the supported method matrix.");
            if (!_suppressNullCheck && RequiresNullCheck(node))
            {
                return TranslateWithNullCheck(node);
            }
            if (!IsNonDeterministicServerMethod(node.Method) && TryGetConstantValueSafe(node, out var constVal))
            {
                return CreateSafeParameter(constVal);
            }
            if (TryTranslateJsonValue(node))
            {
                return node;
            }
            var stringMethodResult = TryTranslateStringMethodCall(node);
            if (stringMethodResult != null)
            {
                return stringMethodResult;
            }
            if (TryTranslateConvertMethod(node))
            {
                return node;
            }
            var enumerableOrQueryableResult = TryTranslateEnumerableOrQueryableMethod(node);
            if (enumerableOrQueryableResult != null)
            {
                return enumerableOrQueryableResult;
            }
            if (TryTranslateProviderMethodCall(node))
            {
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

            _sql.Append("(SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
                .Append(" FROM ").Append(childMapping.EscTable).Append(' ').Append(_provider.Escape(subAlias))
                .Append(" WHERE ");
            AppendRelationPredicate(_sql, relation, _provider.Escape(subAlias), parentInfo.Alias);
            _sql.Append(')');
        }

        private static void AppendRelationPredicate(OptimizedSqlBuilder sql, TableMapping.Relation relation, string childAlias, string parentAlias)
        {
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                if (i > 0)
                    sql.Append(" AND ");
                sql.Append(childAlias).Append('.').Append(relation.ForeignKeys[i].EscCol)
                    .Append(" = ")
                    .Append(parentAlias).Append('.').Append(relation.PrincipalKeys[i].EscCol);
            }
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
            Expression? predicateBody = null;
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                Expression fkAccess = Expression.Property(childParam, relation.ForeignKeys[i].Prop);
                Expression pkAccess = Expression.Property(parentParam, relation.PrincipalKeys[i].Prop);
                // Promote nullable mismatches so Expression.Equal type-checks (e.g. nullable FK
                // referring to a non-nullable PK).
                if (fkAccess.Type != pkAccess.Type)
                {
                    var common = Nullable.GetUnderlyingType(fkAccess.Type) ?? fkAccess.Type;
                    if (fkAccess.Type != common) fkAccess = Expression.Convert(fkAccess, common);
                    if (pkAccess.Type != common) pkAccess = Expression.Convert(pkAccess, common);
                }

                var equality = Expression.Equal(fkAccess, pkAccess);
                predicateBody = predicateBody is null ? equality : Expression.AndAlso(predicateBody, equality);
            }
            var fkPredicate = Expression.Lambda(predicateBody!, childParam);

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
