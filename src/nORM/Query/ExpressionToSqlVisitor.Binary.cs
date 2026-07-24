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
        protected override Expression VisitBinary(BinaryExpression node)
        {
            // A constant array-index like `arr[2]` (a captured array + constant index, no row
            // reference) is a BinaryExpression of type ArrayIndex — it has no SQL operator, so it
            // would fall through to the operator switch and throw. Fold it to its value and bind
            // as a parameter (the same treatment a plain captured constant gets).
            if (node.NodeType == ExpressionType.ArrayIndex && TryGetConstantValue(node, out var arrayIndexValue))
            {
                AppendConstant(arrayIndexValue, node.Type);
                return node;
            }
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
            // A predicate comparing a value-converter column to an inline constant must bind the
            // converted (provider) value, e.g. an enum stored as a string, or a converter that
            // stores 42 as -42. Without this the constant binds in its model form and silently
            // matches the wrong rows (or none). Closure-captured values are not handled here because
            // they must be converted at execution time (the plan is fingerprint-cached across
            // differing captured values); those still flow through the normal path.
            if (TryEmitConvertedColumnConstantComparison(node))
                return node;
            if (TryEmitDateTimeOffsetLiteralComparison(node))
                return node;

            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                bool leftNull  = IsNullExpression(node.Left);
                bool rightNull = IsNullExpression(node.Right);
                if (leftNull || rightNull)
                {
                    var nullTestOperand = leftNull ? node.Right : node.Left;
                    // Whole-entity navigation null test: `e.Dept == null` asks whether
                    // the parent is missing — the FK being NULL, or (when the principal
                    // carries global filters) the parent row being filtered out.
                    if (TryEmitNavigationNullTest(nullTestOperand, testIsNull: node.NodeType == ExpressionType.Equal))
                        return node;
                    _sql.Append("(");
                    Visit(nullTestOperand);
                    _sql.Append(node.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                    _sql.Append(")");
                    return node;
                }

                // Whole-entity navigation vs a captured entity instance:
                // compare the dependent's FK to the instance's primary key.
                if (TryEmitNavigationEntityComparison(node))
                    return node;

                // Inline boolean literals (true/false) as SQL literals instead of parameterizing.
                // Parameterized booleans (WHERE col = @p0 with @p0=1) deprive query planners of
                // column selectivity statistics. Providers that prefer bare boolean predicates get
                // WHERE col / WHERE NOT col for non-nullable bools instead.
                if (TryInlineBoolLiteral(node))
                    return node;

                // Bool =/<> against a BOOLEAN EXPRESSION (e.g. x.Flag == (x.Id % 2 == 0)):
                // the expression side renders as a PREDICATE, and providers without a
                // boolean value type (SQL Server) reject `col = (pred)`. Convert each
                // predicate-shaped side to a comparable value via the provider hook
                // (same treatment as bool join keys); value-shaped sides (columns,
                // parameters) compare bare.
                if (node.Left.Type == typeof(bool) && node.Right.Type == typeof(bool)
                    && (RendersAsBoolPredicate(node.Left) || RendersAsBoolPredicate(node.Right)))
                {
                    var lbSql = RendersAsBoolPredicate(node.Left)
                        ? _provider.BooleanPredicateAsValueSql(GetSql(node.Left))
                        : GetSql(node.Left);
                    var rbSql = RendersAsBoolPredicate(node.Right)
                        ? _provider.BooleanPredicateAsValueSql(GetSql(node.Right))
                        : GetSql(node.Right);
                    _sql.Append("(")
                        .Append(lbSql)
                        .Append(node.NodeType == ExpressionType.Equal ? " = " : " <> ")
                        .Append(rbSql)
                        .Append(")");
                    return node;
                }

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

                // C# string and char equality are ordinal (case-sensitive); on providers whose
                // default collation makes `=` case-insensitive (MySQL, SQL Server), string/char
                // =/<> must emit the sargable ordinal wrap instead of a bare compare (chars
                // translate to one-character SQL strings, e.g. SUBSTR(col,1,1), and fold case the
                // same way). SQLite/PostgreSQL compare ordinally already, so their flag is false
                // and every emit below is unchanged.
                bool ordinalStringCompare = _provider.DefaultStringEqualityIsCaseInsensitive
                    && (IsStringOrCharType(node.Left.Type) || IsStringOrCharType(node.Right.Type));

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
                        // Ordinal string equality composes with the base null-safe shape directly
                        // (MySQL and SQL Server both use the base NullSafeEqual form).
                        if (ordinalStringCompare)
                            _sql.Append($"({_provider.OrdinalStringEqualSql(lf, rf)} OR ({lf} IS NULL AND {rf} IS NULL))");
                        else
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
                        {
                            _sql.Append(ordinalStringCompare
                                ? $"({lf} IS NULL OR {_provider.OrdinalStringNotEqualSql(lf, rf)})"
                                : $"({lf} IS NULL OR {lf} <> {rf})");
                        }
                        else if (ordinalStringCompare)
                        {
                            _sql.Append($"(({lf} IS NOT NULL AND {rf} IS NOT NULL AND {_provider.OrdinalStringNotEqualSql(lf, rf)})" +
                                        $" OR ({lf} IS NULL AND {rf} IS NOT NULL)" +
                                        $" OR ({lf} IS NOT NULL AND {rf} IS NULL))");
                        }
                        else
                        {
                            _sql.Append(_provider.NullSafeNotEqual(lf, rf));
                        }
                    }
                    return node;
                }

                if (ordinalStringCompare)
                {
                    var ordL = GetSql(node.Left);
                    var ordR = GetSql(node.Right);
                    _sql.Append(node.NodeType == ExpressionType.Equal
                        ? _provider.OrdinalStringEqualSql(ordL, ordR)
                        : _provider.OrdinalStringNotEqualSql(ordL, ordR));
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
                _sql.Append(_provider.GetNullSafeConcatSql(concatLeftSql, concatRightSql));
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
                // The LEFT operand is visited FIRST (the extractor walks in
                // document order), so its parameters must be created before
                // this right-hand placeholder.
                var leftSql = GetSql(node.Left);
                var placeholderParam = $"{_provider.ParamPrefix}cp{_compiledParams.Count}_unused";
                _params[placeholderParam] = DBNull.Value;
                _compiledParams.Add(placeholderParam);
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
                // Route through the provider hooks. Equality uses the canonical
                // decimal TEXT where available: the REAL coercion is double
                // precision, so decimals differing beyond ~15-17 significant
                // digits would compare equal. Relational operators and
                // arithmetic keep the numeric REAL form (text cannot order or
                // compute). Other providers' native DECIMAL is precise so both
                // hooks are identity/null and the shape stays `(left op right)`.
                // Canonical text applies only when BOTH operands are raw storage
                // (column / constant / parameter): a computed operand (Truncate,
                // arithmetic) renders as a NUMBER, and SQLite compares numbers
                // and text by storage class -- never equal. Computed operands
                // stay in the numeric REAL domain on both sides.
                bool exactEqualityOp = (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
                    && IsRawStorageOperand(node.Left) && IsRawStorageOperand(node.Right);
                string WrapDecimal(string sql)
                    => exactEqualityOp ? _provider.ExactDecimalKeySql(sql) : _provider.NormalizeDecimalForCompare(sql);
                var leftSqlD = WrapDecimal(GetSql(node.Left));
                var rightSqlD = WrapDecimal(GetSql(node.Right));
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

            // TimeOnly EQUALITY on a TEXT-storing provider (SQLite): "12:00:00.0000000" and "12:00:00" are
            // the same time but lexically distinct, so a raw `=` silently drops the differently-scaled row.
            // Canonicalize both raw-storage operands (strip trailing fraction zeros). Relational operators
            // are already lexically correct for zero-padded TimeOnly text, and native TIME providers return
            // null from the hook (identity) so the shape stays `(left op right)` there.
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                bool leftIsTimeOnly = (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(TimeOnly);
                bool rightIsTimeOnly = (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeOnly);
                if ((leftIsTimeOnly || rightIsTimeOnly)
                    && IsRawStorageOperand(node.Left) && IsRawStorageOperand(node.Right)
                    && _provider.CanonicalTimeOnlyTextForExactCompare("x") != null)
                {
                    string WrapTimeOnly(string sql) => _provider.CanonicalTimeOnlyTextForExactCompare(sql)!;
                    _sql.Append('(').Append(WrapTimeOnly(GetSql(node.Left)));
                    _sql.Append(node.NodeType == ExpressionType.Equal ? " = " : " <> ");
                    _sql.Append(WrapTimeOnly(GetSql(node.Right))).Append(')');
                    return node;
                }
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
                // A DateTime/TimeOnly SUBTRACTION operand is not TimeSpan storage: a
                // raw SQL minus of two datetime TEXTs coerces to garbage on SQLite,
                // and the text-parsing normalizer cannot run over a number. Route
                // those operands through the provider's difference-in-seconds hooks
                // (tick-exact) so `(a - b) == span` compares seconds to seconds.
                static bool IsDateSubtractionOperand(Expression e)
                {
                    while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                        e = u.Operand;
                    return e is BinaryExpression { NodeType: ExpressionType.Subtract } b
                        && (IsDateTimeLike(b.Left.Type) || IsTimeOnly(b.Left.Type));
                }
                bool anyDiffOperand = IsDateSubtractionOperand(node.Left) || IsDateSubtractionOperand(node.Right);
                string EmitTimeSpanOperand(Expression e, bool operandIsTs)
                {
                    var stripped = e;
                    while (stripped is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                        stripped = u.Operand;
                    if (stripped is BinaryExpression { NodeType: ExpressionType.Subtract } diff)
                    {
                        if (IsDateTimeLike(diff.Left.Type))
                            return _provider.GetDateTimeDifferenceSecondsSql(GetSql(diff.Left), GetSql(diff.Right));
                        if (IsTimeOnly(diff.Left.Type))
                            return _provider.GetTimeOnlyDifferenceSecondsSql(GetSql(diff.Left), GetSql(diff.Right));
                    }
                    var sqlFrag = GetSql(e);
                    if (!operandIsTs) return sqlFrag;
                    // Against a difference the comparison domain is SECONDS on every
                    // provider; between two stored TimeSpans the native domain wins
                    // (normalize is identity off-SQLite).
                    return anyDiffOperand
                        ? _provider.TimeSpanOperandToSecondsSql(sqlFrag)
                        : _provider.NormalizeTimeSpanForCompare(sqlFrag);
                }
                var leftNorm  = EmitTimeSpanOperand(node.Left, leftIsTs);
                var rightNorm = EmitTimeSpanOperand(node.Right, rightIsTs);
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
                // server's semantics (integer vs decimal vs double). Integer division is the
                // exception: C# truncates but MySQL's / yields a decimal, so integral-typed
                // division routes through the provider's integer-division operator (DIV there).
                ExpressionType.Add => " + ",
                ExpressionType.Subtract => " - ",
                ExpressionType.Multiply => " * ",
                ExpressionType.Divide when DatabaseProvider.IsIntegralArithmeticType(node.Type)
                    => $" {_provider.IntegerDivisionOperator} ",
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
        /// True when a bool-typed operand translates to a SQL PREDICATE (comparison,
        /// logical combination, negation, or a bool-returning method like Contains)
        /// rather than a comparable value (a mapped column or bound parameter).
        /// </summary>
        private static bool RendersAsBoolPredicate(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e is BinaryExpression
            {
                NodeType: ExpressionType.Equal or ExpressionType.NotEqual
                    or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                    or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                    or ExpressionType.AndAlso or ExpressionType.OrElse
            }
                or UnaryExpression { NodeType: ExpressionType.Not }
                or MethodCallExpression;
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

        // string and char both translate to SQL text comparisons, so both need the ordinal
        // (case-sensitive) equality treatment on CI-collation providers.
        private static bool IsStringOrCharType(Type t)
        {
            var u = Nullable.GetUnderlyingType(t) ?? t;
            return u == typeof(string) || u == typeof(char);
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

        /// <summary>
        /// Emits <c>(column &lt;op&gt; @p)</c> for a comparison between a value-converter column and a
        /// bindable value, binding the converter's provider representation. An inline constant is
        /// converted and baked; a closure-captured value is emitted as a compiled parameter whose
        /// converter is recorded so it is applied to the extractor-supplied value at execution time
        /// (a fingerprint-cached plan is reused across differing captures and cannot bake it). Returns
        /// false when neither/both sides are a converter column, when the value is not a simple
        /// bindable value, or when it is a literal null (the IS NULL path handles null).
        /// </summary>
        private bool TryEmitConvertedColumnConstantComparison(BinaryExpression node)
        {
            if (node.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual
                or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual))
                return false;

            var leftIsColumn = TryGetConverterColumn(node.Left, out var leftColumn, out var leftViaNav);
            var rightIsColumn = TryGetConverterColumn(node.Right, out var rightColumn, out var rightViaNav);
            if (leftIsColumn == rightIsColumn)
                return false; // neither is a converter column, or a column-vs-column comparison

            var memberSide = leftIsColumn ? node.Left : node.Right;
            var valueSide = leftIsColumn ? node.Right : node.Left;
            var column = leftIsColumn ? leftColumn : rightColumn;
            // A navigation-resolved member is nullable regardless of the principal
            // column's nullability: a missing parent yields SQL NULL, and C# null
            // semantics must keep those rows for != comparisons.
            var memberIsNullable = column.IsNullable || (leftIsColumn ? leftViaNav : rightViaNav);

            // Classify the value side before writing anything so an unhandled shape declines cleanly.
            var stripped = StripConvert(valueSide);
            var isInlineConstant = stripped is ConstantExpression;
            // A free parameter (not a mapped entity column, not a grouping key) is a compiled-query
            // parameter; VisitParameter emits a compiled slot for it.
            var isFreeParameter = stripped is ParameterExpression fpe
                && !_parameterMappings.ContainsKey(fpe)
                && !_groupingKeys.ContainsKey(fpe);
            if (isInlineConstant)
            {
                if (((ConstantExpression)stripped).Value == null)
                    return false; // literal null -> IS NULL path
            }
            else if (!isFreeParameter && !TryGetConstantValue(valueSide, out _))
            {
                return false; // not a simple constant/closure/parameter value (e.g. another column)
            }

            // Normalize to `column <op> value` — flip the operator when the column is on the right.
            var op = leftIsColumn ? node.NodeType : FlipComparison(node.NodeType);

            // A converter that stores STRINGS (enum-to-name etc.) compares under the column's
            // collation; on CI-collation providers (MySQL, SQL Server) =/<> must use the same
            // sargable ordinal wrap as plain string equality, or `Status == Active` would match
            // rows storing 'ACTIVE'. Relational operators keep the plain compare (collation
            // ordering of converter text is out of scope for ordinal equality semantics).
            if (_provider.DefaultStringEqualityIsCaseInsensitive
                && column.Converter!.ProviderType == typeof(string)
                && op is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                var colSqlOrd = GetSql(memberSide);
                var vs = _sql.Length;
                EmitConvertedValueOperand(stripped, column.Converter!, isInlineConstant, isFreeParameter);
                var valSql = _sql.ToString(vs, _sql.Length - vs);
                _sql.TruncateTo(vs);
                if (op == ExpressionType.Equal)
                    _sql.Append(_provider.OrdinalStringEqualSql(colSqlOrd, valSql));
                else if (memberIsNullable)
                    _sql.Append($"({colSqlOrd} IS NULL OR {_provider.OrdinalStringNotEqualSql(colSqlOrd, valSql)})");
                else
                    _sql.Append(_provider.OrdinalStringNotEqualSql(colSqlOrd, valSql));
                return true;
            }

            _sql.Append('(');
            if (op == ExpressionType.NotEqual && memberIsNullable)
            {
                // Preserve C# `col != value` semantics for a nullable column: NULL rows must be
                // included (SQL `NULL <> @p` is UNKNOWN, i.e. excluded).
                var columnSql = GetSql(memberSide);
                _sql.Append(columnSql).Append(" IS NULL OR ").Append(columnSql).Append(" <> ");
                EmitConvertedValueOperand(stripped, column.Converter!, isInlineConstant, isFreeParameter);
            }
            else
            {
                Visit(memberSide);
                _sql.Append(op switch
                {
                    ExpressionType.Equal => " = ",
                    ExpressionType.NotEqual => " <> ",
                    ExpressionType.GreaterThan => " > ",
                    ExpressionType.GreaterThanOrEqual => " >= ",
                    ExpressionType.LessThan => " < ",
                    ExpressionType.LessThanOrEqual => " <= ",
                    _ => throw new InvalidOperationException()
                });
                EmitConvertedValueOperand(stripped, column.Converter!, isInlineConstant, isFreeParameter);
            }
            _sql.Append(')');
            return true;
        }

        private void EmitConvertedValueOperand(Expression strippedValue, nORM.Mapping.IValueConverter converter, bool isInlineConstant, bool isFreeParameter)
        {
            if (isInlineConstant)
            {
                var raw = ((ConstantExpression)strippedValue).Value;
                var converted = converter.ConvertToProvider(raw);
                AppendConstant(converted, converted?.GetType() ?? converter.ProviderType);
                return;
            }

            if (isFreeParameter)
            {
                // Compiled-query parameter: let VisitParameter emit (and reuse) its compiled slot,
                // then record the converter for that slot name so the compiled-query binder applies it.
                Visit(strippedValue);
                if (_paramMap.TryGetValue((ParameterExpression)strippedValue, out var slotName))
                    _paramConverters[slotName] = converter;
                return;
            }

            // Closure-captured value: emit a compiled parameter (mirroring the closure branch in
            // VisitMember so the ParameterValueExtractor's positional value aligns) and record the
            // converter so the plan applies it to the extracted value at execution time.
            var paramName = QueryTranslator.TryReuseClosureSlot(strippedValue)
                ?? $"{_provider.ParamPrefix}cp{_compiledParams.Count}";
            if (!_compiledParams.Contains(paramName))
            {
                _params[paramName] = DBNull.Value;
                _compiledParams.Add(paramName);
                QueryTranslator.RecordClosureSlot(strippedValue, paramName);
            }
            _paramConverters[paramName] = converter;
            _sql.Append(paramName);
        }

        private static Expression StripConvert(Expression expr)
        {
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            return expr;
        }

        private bool TryGetConverterColumn(Expression expr, out Column column)
            => TryGetConverterColumn(expr, out column, out _);

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
