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
        /// True while visiting a position that expects a boolean PREDICATE (a CASE
        /// WHEN test); value positions wrap predicates through the provider's
        /// BooleanPredicateAsValue instead (T-SQL cannot select a bare predicate).
        /// </summary>
        private bool _inPredicatePosition;

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            var sb = EnsureBuilder();
            sb.Append("(CASE WHEN ");
            var savedPredicatePosition = _inPredicatePosition;
            _inPredicatePosition = true;
            Visit(node.Test);
            _inPredicatePosition = savedPredicatePosition;
            sb.Append(" THEN ");
            Visit(node.IfTrue);
            sb.Append(" ELSE ");
            Visit(node.IfFalse);
            sb.Append(" END)");
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            // A parameter used directly in a projection that is NOT the projection's row parameter is a
            // compiled-query VALUE parameter (Norm.CompileQuery's TParam), e.g. Select(r => r.A - k).
            // Emit a __qv-marked compiled slot so the compiled pipeline binds the live value by name;
            // without this the parameter renders as nothing and corrupts the SQL ("(A - )" syntax error).
            // The predicate path (ExpressionToSqlVisitor.VisitParameter) already does this. Scoped to when
            // OuterRowParameters is known (the row param excluded) and the param is not the DbContext
            // receiver, so ordinary projections are unaffected.
            if (SharedParams != null && SharedCompiledParams != null
                && OuterRowParameters != null && !OuterRowParameters.Contains(node)
                && !typeof(DbContext).IsAssignableFrom(node.Type))
            {
                var paramName = $"{_provider.ParamPrefix}p{SharedParams.Count}__qv";
                SharedParams[paramName] = DBNull.Value;
                SharedCompiledParams.Add(paramName);
                EnsureBuilder().Append(paramName);
                return node;
            }
            return base.VisitParameter(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var sb = EnsureBuilder();
            if (node.Value is null)
            {
                sb.Append("NULL");
                return node;
            }
            switch (node.Value)
            {
                case string s:
                    sb.Append('\'').Append(s.Replace("'", "''")).Append('\'');
                    break;
                case char c:
                    // A char literal inlines as a single-character string literal ('.'), not a
                    // bare token — the default ToString path emits an unquoted `.` (e.g. as a
                    // PadRight pad-char arg), producing invalid SQL ("near \".\": syntax error").
                    sb.Append('\'').Append(new string(c, 1).Replace("'", "''")).Append('\'');
                    break;
                case bool b:
                    sb.Append(b ? _provider.BooleanTrueLiteral : "0");
                    break;
                case System.Enum e:
                    sb.Append(Convert.ToInt64(e, System.Globalization.CultureInfo.InvariantCulture));
                    break;
                default:
                    sb.Append(System.Convert.ToString(node.Value, System.Globalization.CultureInfo.InvariantCulture));
                    break;
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            var sb = EnsureBuilder();
            // A constant array-index like `arr[2]` (captured array + constant index, no row
            // reference) is a BinaryExpression of type ArrayIndex with no SQL operator — fold it
            // to its value and emit as a constant (mirror of the ExpressionToSqlVisitor branch).
            if (node.NodeType == ExpressionType.ArrayIndex
                && QueryTranslator.TryGetConstantValue(node, out var arrayIndexValue))
            {
                Visit(Expression.Constant(arrayIndexValue, node.Type));
                return node;
            }
            // A boolean binary in a VALUE position (a projected member like
            // `Big = r.Score > 5`, `IsOrphan = r.Dept == null`, or a && chain) must
            // render as a selectable value: T-SQL rejects a bare predicate in the
            // SELECT list ('Incorrect syntax near ...'), while the other dialects
            // select predicates directly (identity hook). Re-entering with the
            // predicate flag set makes every inner branch emit the bare predicate,
            // which the provider hook then wraps once at the outermost level.
            bool isBoolPredicate = node.Type == typeof(bool) && node.NodeType is ExpressionType.Equal
                or ExpressionType.NotEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.AndAlso or ExpressionType.OrElse;
            if (isBoolPredicate && !_inPredicatePosition)
            {
                var wrapStart = sb.Length;
                _inPredicatePosition = true;
                try { VisitBinary(node); }
                finally { _inPredicatePosition = false; }
                var predicateSql = sb.ToString(wrapStart, sb.Length - wrapStart);
                sb.Length = wrapStart;
                sb.Append(_provider.BooleanPredicateAsValue(predicateSql));
                return node;
            }
            // `a ?? b` in a projection lowers to COALESCE(a, b) — emit as a function call so
            // it composes inside `new { Name = r.Name ?? "anon" }` projections.
            if (node.NodeType == ExpressionType.Coalesce)
            {
                sb.Append("COALESCE(");
                Visit(node.Left);
                sb.Append(", ");
                Visit(node.Right);
                sb.Append(')');
                return node;
            }
            // Null tests in projections: `x == null` must emit IS NULL — the generic
            // path would render `col = NULL`, which is always UNKNOWN and silently
            // materializes false for every row. A whole-entity navigation operand
            // tests its FOREIGN KEY value (e.Dept == null is orphan-ness).
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                static bool IsNullConst(Expression e)
                {
                    while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                        e = u.Operand;
                    return e is ConstantExpression { Value: null };
                }
                bool leftIsNull = IsNullConst(node.Left);
                bool rightIsNull = IsNullConst(node.Right);
                if (leftIsNull ^ rightIsNull)
                {
                    var operand = leftIsNull ? node.Right : node.Left;
                    // Whole-entity navigation: missing parent is FK-NULL, or — when the
                    // principal carries global filters — a filtered-out parent row.
                    if (TryRenderScvNavigationNullTest(operand, node.NodeType == ExpressionType.Equal, out var navTestSql))
                    {
                        sb.Append(navTestSql);
                        return node;
                    }
                    var opStart = sb.Length;
                    Visit(operand);
                    var operandSql = sb.ToString(opStart, sb.Length - opStart);
                    sb.Length = opStart;
                    // Bare predicate: the top-of-method wrapper handles value positions.
                    sb.Append('(').Append(operandSql)
                      .Append(node.NodeType == ExpressionType.Equal ? " IS NULL)" : " IS NOT NULL)");
                    return node;
                }
                // Two non-null-constant operands where a null is possible (two nullable columns, or a
                // nullable column vs a null-valued variable): the generic path emits raw `A = B` / `A <> B`,
                // whose SQL three-valued logic silently materializes C# `null == null` (true) and
                // `value != null` (true) as FALSE — a silently-wrong projected value. Emit the provider's
                // null-safe (in)equality instead, mirroring the WHERE-path (ETSV) expansion. Scoped to
                // nullable scalars (Nullable<T> / string) so whole-entity/navigation comparisons are untouched.
                else if (!leftIsNull && !rightIsNull)
                {
                    static bool CouldBeNullScalar(Expression e)
                    {
                        while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                            e = u.Operand;
                        if (e is ConstantExpression c) return c.Value is null;   // non-null constant is never null
                        var t = e.Type;
                        return System.Nullable.GetUnderlyingType(t) != null || t == typeof(string);
                    }
                    var leftCbn = CouldBeNullScalar(node.Left);
                    var rightCbn = CouldBeNullScalar(node.Right);
                    // Equal: expand only when BOTH could be null (null == "x" is false in SQL and C# alike).
                    // NotEqual: expand when EITHER could be null (value != null must stay true).
                    var needsExpansion = node.NodeType == ExpressionType.NotEqual ? (leftCbn || rightCbn) : (leftCbn && rightCbn);
                    if (needsExpansion)
                    {
                        var lStart = sb.Length;
                        Visit(node.Left);
                        var lSql = sb.ToString(lStart, sb.Length - lStart);
                        sb.Length = lStart;
                        var rStart = sb.Length;
                        Visit(node.Right);
                        var rSql = sb.ToString(rStart, sb.Length - rStart);
                        sb.Length = rStart;
                        // C# string equality is ordinal (case-sensitive). On providers whose default
                        // collation folds case (MySQL, SQL Server) the null-safe form must compose the
                        // sargable ordinal wrap, or the computed boolean is case-insensitive-wrong there
                        // (invisible on SQLite, whose default collation is already case-sensitive). Mirror
                        // the WHERE-path (ETSV) expansion exactly, including the asymmetric NotEqual forms.
                        bool ordinalStringCompare = _provider.DefaultStringEqualityIsCaseInsensitive
                            && (node.Left.Type == typeof(string) || node.Right.Type == typeof(string)
                                || (System.Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(char)
                                || (System.Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(char));
                        if (node.NodeType == ExpressionType.Equal)
                        {
                            sb.Append(ordinalStringCompare
                                ? $"({_provider.OrdinalStringEqualSql(lSql, rSql)} OR ({lSql} IS NULL AND {rSql} IS NULL))"
                                : _provider.NullSafeEqual(lSql, rSql));
                        }
                        else if (!rightCbn)
                        {
                            // Right side is a known non-null value: `rf IS NULL` can never fire, so the
                            // 3-way expansion reduces to (lf IS NULL OR lf <> rf).
                            sb.Append(ordinalStringCompare
                                ? $"({lSql} IS NULL OR {_provider.OrdinalStringNotEqualSql(lSql, rSql)})"
                                : $"({lSql} IS NULL OR {lSql} <> {rSql})");
                        }
                        else if (ordinalStringCompare)
                        {
                            sb.Append($"(({lSql} IS NOT NULL AND {rSql} IS NOT NULL AND {_provider.OrdinalStringNotEqualSql(lSql, rSql)})" +
                                      $" OR ({lSql} IS NULL AND {rSql} IS NOT NULL)" +
                                      $" OR ({lSql} IS NOT NULL AND {rSql} IS NULL))");
                        }
                        else
                        {
                            sb.Append(_provider.NullSafeNotEqual(lSql, rSql));
                        }
                        return node;
                    }
                }
            }

            // C# `+` on string operands is concatenation, not arithmetic. Emit
            // the provider's concat SQL (`||` on SQLite, `CONCAT(...)` on
            // SQL Server/MySQL) so projections like `Select(p => p.First + " " + p.Last)`
            // don't fall through to SQL numeric `+` (which on SQLite coerces TEXT
            // to 0, returning "0" per row). Capture each side via a StringBuilder
            // length-snapshot then hand the slices to GetConcatSql.
            if (node.NodeType == ExpressionType.Add
                && (node.Left.Type == typeof(string) || node.Right.Type == typeof(string)))
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var rightStart = sb.Length;
                Visit(node.Right);
                var rightSql = sb.ToString(rightStart, sb.Length - rightStart);
                sb.Length = rightStart;
                sb.Append(_provider.GetNullSafeConcatSql(leftSql, rightSql));
                return node;
            }
            // DateTime + TimeSpan COLUMN (rhs is not a foldable constant --
            // e.g. p.Stamp + p.Duration). Without this branch SQL '+' on TEXT
            // coerces to numeric and produces garbage that fails GetDateTime
            // via FromJulianDate. Emit strftime with a constructed modifier
            // 'sign || N || " seconds"' where N parses the TimeSpan column's
            // sub-day 'HH:mm:ss' text per b17440e. Sub-day only -- multi-day
            // TimeSpan columns are out of scope (same scope cap as memory
            // item TimeSpan handler).
            // DateTime AND DateTimeOffset use the same arithmetic emit path.
            static bool ScvIsDateTimeOrOffset(Type t)
                => t == typeof(DateTime) || t == typeof(DateTimeOffset);
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && ScvIsDateTimeOrOffset(Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeSpan)
                && !QueryTranslator.TryGetConstantValue(node.Right, out _))
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var rightStart = sb.Length;
                Visit(node.Right);
                var rightSql = sb.ToString(rightStart, sb.Length - rightStart);
                sb.Length = rightStart;
                var subtract = node.NodeType == ExpressionType.Subtract;
                var leftIsDto = (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(DateTimeOffset);
                var dateArithSql = leftIsDto
                    ? _provider.AddTimeSpanColumnToDateTimeOffsetSql(leftSql, rightSql, subtract)
                    : _provider.AddTimeSpanColumnToDateTimeSql(leftSql, rightSql, subtract);
                if (dateArithSql != null)
                {
                    sb.Append(dateArithSql);
                    return node;
                }
                throw new InvalidOperationException(
                    $"{_provider.GetType().Name} does not implement AddTimeSpanColumnToDateTimeSql; " +
                    "DateTime/Offset +/- TimeSpan column arithmetic in projection requires this provider hook.");
            }
            // DateTime/DateTimeOffset + constant TimeSpan -> same type. Folds via
            // TryGetConstantValue; AddSecondsToDateTimeSql handles provider-native
            // date arithmetic.
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && ScvIsDateTimeOrOffset(Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeSpan)
                && QueryTranslator.TryGetConstantValue(node.Right, out var rhsTs)
                && rhsTs is TimeSpan span)
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var seconds = span.TotalSeconds;
                if (node.NodeType == ExpressionType.Subtract) seconds = -seconds;
                var secondsLiteral = seconds.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                var dateArithSql = _provider.AddSecondsToDateTimeSql(leftSql, secondsLiteral);
                if (dateArithSql != null)
                {
                    sb.Append(dateArithSql);
                    return node;
                }
                throw new InvalidOperationException(
                    $"{_provider.GetType().Name} does not implement AddSecondsToDateTimeSql; " +
                    "DateTime + constant TimeSpan arithmetic in projection requires this provider hook.");
            }
            // DateTime - DateTime / DateTimeOffset - DateTimeOffset in projection
            // -> TimeSpan. SQL '-' on TEXT columns returns 0 (silent-wrongness);
            // route through the provider hook (julianday on SQLite parses ISO
            // offset suffixes, so DTO subtraction yields the UTC-instant
            // difference even when LHS/RHS were stored in different offsets).
            static bool IsDateOrDtoType(Type t) => t == typeof(DateTime) || t == typeof(DateTimeOffset);
            if (node.NodeType == ExpressionType.Subtract
                && IsDateOrDtoType(Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type)
                && IsDateOrDtoType(Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type))
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var rightStart = sb.Length;
                Visit(node.Right);
                var rightSql = sb.ToString(rightStart, sb.Length - rightStart);
                sb.Length = rightStart;
                // For DateTimeOffset operands, use UTC-epoch-microsecond
                // subtraction (sister of the equality lowering) to avoid the
                // julianday-delta double-precision noise that turns FromSeconds(15)
                // into 14.9999991s while still preserving practical sub-second
                // duration semantics across providers.
                Type lt = Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type;
                Type rt = Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type;
                if (lt == typeof(DateTimeOffset) && rt == typeof(DateTimeOffset))
                {
                    sb.Append(_provider.GetDateTimeOffsetDifferenceSecondsSql(leftSql, rightSql));
                }
                else
                {
                    // Provider hook returns REAL seconds (julianday delta on SQLite,
                    // DATEDIFF_BIG on SqlServer, EXTRACT(EPOCH) on Postgres,
                    // TIMESTAMPDIFF on MySQL). Materializer reads as double; user
                    // expects TimeSpan -- handled by MaterializerFactory's
                    // GetFieldValue path which converts numeric to TimeSpan via
                    // TimeSpan.FromSeconds.
                    sb.Append('(').Append(_provider.GetDateTimeDifferenceSecondsSql(leftSql, rightSql)).Append(')');
                }
                return node;
            }
            // TimeSpan + TimeSpan and TimeSpan - TimeSpan between two column
            // expressions (or sub-expressions of TimeSpan type) -> fractional
            // seconds via the provider's GetTimeSpanColumnSecondsSql hook,
            // then sum/diff. Materialiser converts the resulting numeric to
            // TimeSpan via TimeSpan.FromSeconds.
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(TimeSpan)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeSpan))
            {
                var tsLeftStart = sb.Length;
                Visit(node.Left);
                var tsLeftSql = sb.ToString(tsLeftStart, sb.Length - tsLeftStart);
                sb.Length = tsLeftStart;
                var tsRightStart = sb.Length;
                Visit(node.Right);
                var tsRightSql = sb.ToString(tsRightStart, sb.Length - tsRightStart);
                sb.Length = tsRightStart;
                var op = node.NodeType == ExpressionType.Add ? '+' : '-';
                sb.Append('(').Append(_provider.GetTimeSpanColumnSecondsSql(tsLeftSql))
                  .Append(' ').Append(op).Append(' ')
                  .Append(_provider.GetTimeSpanColumnSecondsSql(tsRightSql)).Append(')');
                return node;
            }
            // TimeOnly - TimeOnly -> TimeSpan, wrapped to [0, 24h) per
            // .NET's TimeOnly.op_Subtraction. Each provider's hook emits the
            // wrapped form so the materializer's TimeSpan.FromSeconds path
            // produces the same wall-clock-elapsed semantics.
            if (node.NodeType == ExpressionType.Subtract
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(TimeOnly)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(TimeOnly))
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var rightStart = sb.Length;
                Visit(node.Right);
                var rightSql = sb.ToString(rightStart, sb.Length - rightStart);
                sb.Length = rightStart;
                sb.Append('(').Append(_provider.GetTimeOnlyDifferenceSecondsSql(leftSql, rightSql)).Append(')');
                return node;
            }
            // Bitwise XOR on integer/enum operands -- SQLite has no `^`
            // primitive; rewrite to (a | b) & ~(a & b), the standard bit-
            // twiddling identity that holds for 64-bit signed two's-complement
            // integers. Captures each operand's SQL once and inlines into the
            // 4-position emit. Restricted to non-bool operand types since
            // `^` on bool is logical XOR (rare; would need a separate CASE
            // rewrite).
            if (node.NodeType == ExpressionType.ExclusiveOr
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) != typeof(bool))
            {
                var xorLeftStart = sb.Length;
                Visit(node.Left);
                var xorLeftSql = sb.ToString(xorLeftStart, sb.Length - xorLeftStart);
                sb.Length = xorLeftStart;
                var xorRightStart = sb.Length;
                Visit(node.Right);
                var xorRightSql = sb.ToString(xorRightStart, sb.Length - xorRightStart);
                sb.Length = xorRightStart;
                sb.Append("((").Append(xorLeftSql).Append(" | ").Append(xorRightSql)
                  .Append(") & ~(").Append(xorLeftSql).Append(" & ").Append(xorRightSql).Append("))");
                return node;
            }
            // A comparison against a value-converter column must bind the converter's PROVIDER value,
            // not the raw model value: `p.Status == Status.Active` projected as a bool would otherwise
            // emit `Status = 1` against a column storing 'Active' and be false for every row. Mirror
            // ETSV.VisitBinary's converter handling for the projection (SelectClauseVisitor) path.
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
                    or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                    or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                && TryEmitConvertedColumnComparison(node))
            {
                return node;
            }
            // Decimal comparisons / arithmetic on TEXT-stored decimal columns
            // must coerce both operands to REAL or SQLite performs lex compare
            // ('10.5' < '2' because '1' < '2'). Mirror of ETSV's bilateral
            // CAST AS REAL wrap (8d795f4). See ETSV.VisitBinary for the
            // precision-tradeoff comment.
            bool isDecCmpArith = node.NodeType is
                    ExpressionType.Equal or ExpressionType.NotEqual
                    or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                    or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                    or ExpressionType.Add or ExpressionType.Subtract
                    or ExpressionType.Multiply or ExpressionType.Divide
                    or ExpressionType.Modulo;
            bool leftIsDec = (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(decimal);
            bool rightIsDec = (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(decimal);
            if (isDecCmpArith && (leftIsDec || rightIsDec))
            {
                // Provider hooks: equality uses the exact canonical text
                // (REAL is double precision and merges decimals beyond ~15-17
                // significant digits); relational/arithmetic keep REAL.
                // Same raw-storage gating as the predicate translator: computed
                // operands render numbers, which never text-equal the canonical form.
                bool exactDecEquality = (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
                    && IsRawStorageOperandScv(node.Left) && IsRawStorageOperandScv(node.Right);
                var decLeftStart = sb.Length;
                Visit(node.Left);
                var decLeftSql = sb.ToString(decLeftStart, sb.Length - decLeftStart);
                sb.Length = decLeftStart;
                var decRightStart = sb.Length;
                Visit(node.Right);
                var decRightSql = sb.ToString(decRightStart, sb.Length - decRightStart);
                sb.Length = decRightStart;
                string WrapDec(string sqlFrag)
                    => exactDecEquality ? _provider.ExactDecimalKeySql(sqlFrag) : _provider.NormalizeDecimalForCompare(sqlFrag);
                sb.Append('(').Append(WrapDec(decLeftSql))
                  .Append(' ').Append(node.NodeType switch
                {
                    ExpressionType.Equal => "=",
                    ExpressionType.NotEqual => "<>",
                    ExpressionType.LessThan => "<",
                    ExpressionType.LessThanOrEqual => "<=",
                    ExpressionType.GreaterThan => ">",
                    ExpressionType.GreaterThanOrEqual => ">=",
                    ExpressionType.Add => "+",
                    ExpressionType.Subtract => "-",
                    ExpressionType.Multiply => "*",
                    ExpressionType.Divide => "/",
                    ExpressionType.Modulo => "%",
                    _ => throw new InvalidOperationException()
                }).Append(' ').Append(WrapDec(decRightSql)).Append(')');
                return node;
            }

            // Projected temporal EQUALITY must be by value, mirroring the predicate visitor (ETSV): a raw
            // TEXT `=` on SQLite silently mis-compares an equal value stored in a different representation.
            // TimeOnly -> canonical text (strip fraction zeros); TimeSpan -> numeric seconds; DateTimeOffset
            // (both sides) -> UTC instant. Native-typed providers return identity from these hooks. Same
            // raw-or-constant gating as the decimal block (a computed operand stays on the generic path).
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                var tempLt = Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type;
                var tempRt = Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type;
                Func<string, string>? tempWrap = null;
                if ((tempLt == typeof(TimeOnly) || tempRt == typeof(TimeOnly))
                    && _provider.CanonicalTimeOnlyTextForExactCompare("x") != null)
                    tempWrap = s => _provider.CanonicalTimeOnlyTextForExactCompare(s)!;
                else if (tempLt == typeof(TimeSpan) || tempRt == typeof(TimeSpan))
                    tempWrap = s => _provider.NormalizeTimeSpanForCompare(s);
                else if (tempLt == typeof(DateTimeOffset) && tempRt == typeof(DateTimeOffset))
                    tempWrap = s => _provider.NormalizeDateTimeOffsetForCompare(s);

                if (tempWrap != null
                    && (IsRawStorageOperandScv(node.Left) || QueryTranslator.TryGetConstantValue(node.Left, out _))
                    && (IsRawStorageOperandScv(node.Right) || QueryTranslator.TryGetConstantValue(node.Right, out _)))
                {
                    var tLeftStart = sb.Length;
                    Visit(node.Left);
                    var tLeftSql = sb.ToString(tLeftStart, sb.Length - tLeftStart);
                    sb.Length = tLeftStart;
                    var tRightStart = sb.Length;
                    Visit(node.Right);
                    var tRightSql = sb.ToString(tRightStart, sb.Length - tRightStart);
                    sb.Length = tRightStart;
                    sb.Append('(').Append(tempWrap(tLeftSql))
                      .Append(node.NodeType == ExpressionType.Equal ? " = " : " <> ")
                      .Append(tempWrap(tRightSql)).Append(')');
                    return node;
                }
            }

            // C# string equality is ordinal; on providers whose default collation folds case
            // (MySQL, SQL Server) a projected `x.Name == "abc"` must use the sargable ordinal
            // wrap so the computed boolean matches LINQ-to-Objects and the Where translation.
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
                && _provider.DefaultStringEqualityIsCaseInsensitive
                && (node.Left.Type == typeof(string) || node.Right.Type == typeof(string)
                    || (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(char)
                    || (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(char)))
            {
                var ordLeftStart = sb.Length;
                Visit(node.Left);
                var ordLeftSql = sb.ToString(ordLeftStart, sb.Length - ordLeftStart);
                sb.Length = ordLeftStart;
                var ordRightStart = sb.Length;
                Visit(node.Right);
                var ordRightSql = sb.ToString(ordRightStart, sb.Length - ordRightStart);
                sb.Length = ordRightStart;
                sb.Append(node.NodeType == ExpressionType.Equal
                    ? _provider.OrdinalStringEqualSql(ordLeftSql, ordRightSql)
                    : _provider.OrdinalStringNotEqualSql(ordLeftSql, ordRightSql));
                return node;
            }

            sb.Append('(');
            Visit(node.Left);
            sb.Append(' ').Append(node.NodeType switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "<>",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                // `&&` is always AndAlso (logical) and `||` is OrElse. `&` and
                // `|` are ExpressionType.And/Or and their meaning depends on
                // the operand type -- on bool they're logical (no short-circuit),
                // on integers/enums they're bitwise. Map accordingly so flag
                // arithmetic (p.Flags & Perm.Read) emits SQLite's bitwise &
                // rather than logical AND (which silently coerces operands to
                // truthy 0/1 and gives the wrong answer).
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                ExpressionType.And => (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(bool) ? "AND" : "&",
                ExpressionType.Or => (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(bool) ? "OR" : "|",
                // SQLite has no native XOR operator on integers; the lower
                // (a | b) - (a & b) rewrite is non-trivial in a single-line
                // emit, so XOR continues to fall through to the default throw
                // until a caller actually needs it.
                ExpressionType.Add => "+",
                ExpressionType.Subtract => "-",
                ExpressionType.Multiply => "*",
                // C# integer division truncates; MySQL's / yields a decimal, so integral-typed
                // division uses the provider's integer-division operator (DIV there, / elsewhere).
                ExpressionType.Divide when nORM.Providers.DatabaseProvider.IsIntegralArithmeticType(node.Type)
                    => _provider.IntegerDivisionOperator,
                ExpressionType.Divide => "/",
                ExpressionType.Modulo => "%",
                // SQLite, SQL Server, MySQL, and PostgreSQL all support << and >>
                // as bitwise shift operators with the same precedence semantics
                // as .NET, so emit the operator directly rather than forcing the
                // multiply-rewrite workaround the old throw suggested.
                ExpressionType.LeftShift => "<<",
                ExpressionType.RightShift => ">>",
                _ => throw new InvalidOperationException(
                    $"Binary operator '{node.NodeType}' has no portable SQL equivalent in a SELECT " +
                    "projection. For Power, use `Math.Pow(x, n)` which lowers to the provider's " +
                    "POWER / POW function."),
            }).Append(' ');
            Visit(node.Right);
            sb.Append(')');
            return node;
        }

        /// <summary>
        /// Emits <c>(column &lt;op&gt; &lt;converted literal&gt;)</c> for a projected comparison between a
        /// value-converter column and an inline constant. Returns false (falling through to the generic
        /// comparison) when neither/both sides are a converter column or the value is not an inline
        /// constant. Closure/parameter values are not handled (rare in a projection comparison).
        /// </summary>
        private bool TryEmitConvertedColumnComparison(BinaryExpression node)
        {
            var leftIsColumn = TryGetConverterColumn(node.Left, out var leftColumn);
            var rightIsColumn = TryGetConverterColumn(node.Right, out var rightColumn);
            if (leftIsColumn == rightIsColumn)
                return false;

            var memberSide = leftIsColumn ? node.Left : node.Right;
            var valueSide = leftIsColumn ? node.Right : node.Left;
            var column = leftIsColumn ? leftColumn : rightColumn;

            var stripped = valueSide;
            while (stripped is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                stripped = u.Operand;
            // Closure capture vs converter column: emit a compiled-parameter slot and register
            // the converter so binders convert the LIVE value on every plan-cache hit.
            if (stripped is MemberExpression
                && QueryTranslator.TryGetConstantValue(stripped, out _)
                && SharedParams != null && SharedCompiledParams != null && SharedParamConverters != null)
            {
                var closureOp = leftIsColumn ? node.NodeType : FlipScvComparison(node.NodeType);
                var closureSb = EnsureBuilder();
                var paramName = QueryTranslator.TryReuseClosureSlot(stripped)
                    ?? $"{_provider.ParamPrefix}cp{SharedCompiledParams.Count}";
                if (!SharedCompiledParams.Contains(paramName))
                {
                    SharedParams[paramName] = DBNull.Value;
                    SharedCompiledParams.Add(paramName);
                    QueryTranslator.RecordClosureSlot(stripped, paramName);
                }
                SharedParamConverters[paramName] = column.Converter!;
                closureSb.Append('(');
                Visit(memberSide);
                closureSb.Append(' ').Append(closureOp switch
                {
                    ExpressionType.Equal => "=",
                    ExpressionType.NotEqual => "<>",
                    ExpressionType.LessThan => "<",
                    ExpressionType.LessThanOrEqual => "<=",
                    ExpressionType.GreaterThan => ">",
                    ExpressionType.GreaterThanOrEqual => ">=",
                    _ => throw new InvalidOperationException()
                }).Append(' ').Append(paramName).Append(')');
                return true;
            }

            if (stripped is not ConstantExpression { Value: { } raw })
                return false;

            var converted = column.Converter!.ConvertToProvider(raw);
            var op = leftIsColumn ? node.NodeType : FlipScvComparison(node.NodeType);
            var sb = EnsureBuilder();
            sb.Append('(');
            Visit(memberSide);
            sb.Append(' ').Append(op switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "<>",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                _ => throw new InvalidOperationException()
            }).Append(' ').Append(FormatLiteral(converted)).Append(')');
            return true;
        }

        private bool TryGetConverterColumn(Expression expr, out Column column)
        {
            column = null!;
            while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expr = u.Operand;
            if (expr is MemberExpression me && _mapping.TryGetColumnForMemberAccess(me, out var col) && col.Converter != null)
            {
                column = col;
                return true;
            }
            return false;
        }

        private static ExpressionType FlipScvComparison(ExpressionType op) => op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op
        };

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Numeric / enum / primitive Convert: the SQL value is the operand itself,
            // EXCEPT a narrowing floating->integral cast, which truncates toward zero
            // in C# and must emit a truncating SQL cast (the raw double column would
            // crash strict drivers' integer readers at materialization).
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                var castSrc = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                var castDst = Nullable.GetUnderlyingType(node.Type) ?? node.Type;
                if ((castSrc == typeof(double) || castSrc == typeof(float) || castSrc == typeof(decimal))
                    && (castDst == typeof(int) || castDst == typeof(long) || castDst == typeof(short)
                        || castDst == typeof(byte) || castDst == typeof(sbyte) || castDst == typeof(uint)
                        || castDst == typeof(ulong) || castDst == typeof(ushort)))
                {
                    var sbCast = EnsureBuilder();
                    var castStart = sbCast.Length;
                    Visit(node.Operand);
                    var castSql = sbCast.ToString(castStart, sbCast.Length - castStart);
                    sbCast.Length = castStart;
                    sbCast.Append(_provider.FloatingToIntegralTruncatingSql(
                        castSql, castDst == typeof(long) || castDst == typeof(ulong)));
                    return node;
                }
                // A WIDENING integral->floating cast ((double)A, (float)B) carries real-arithmetic
                // semantics: without an explicit CAST, `(double)A / B` in a projection runs as INTEGER
                // division on the provider (SQLite: 3/2 = 1, not 1.5) — silent wrong result. Emit
                // CAST AS REAL. Decimal targets keep their own precise TEXT-based coercion.
                if ((castDst == typeof(double) || castDst == typeof(float))
                    && nORM.Providers.DatabaseProvider.IsIntegralArithmeticType(castSrc))
                {
                    var sbF = EnsureBuilder();
                    var fStart = sbF.Length;
                    Visit(node.Operand);
                    var fSql = sbF.ToString(fStart, sbF.Length - fStart);
                    sbF.Length = fStart;
                    sbF.Append(_provider.GetRealCastSql(fSql));
                    return node;
                }
                // A char->integer cast ((int)s[0]) is the CODE POINT in C#. A char is represented
                // as a single-character string (s[0] -> SUBSTR), so a pass-through would leave the
                // substr text and the materializer would CAST it to 0. Emit the char-code function.
                if (castSrc == typeof(char)
                    && (castDst == typeof(int) || castDst == typeof(long) || castDst == typeof(short)
                        || castDst == typeof(byte) || castDst == typeof(sbyte) || castDst == typeof(uint)
                        || castDst == typeof(ulong) || castDst == typeof(ushort)))
                {
                    var sbC = EnsureBuilder();
                    var cStart = sbC.Length;
                    Visit(node.Operand);
                    var cSql = sbC.ToString(cStart, sbC.Length - cStart);
                    sbC.Length = cStart;
                    sbC.Append(_provider.GetCharCodeSql(cSql));
                    return node;
                }
                Visit(node.Operand);
                return node;
            }
            // Unary minus / boolean NOT inside a projection. Without explicit handling, the
            // default ExpressionVisitor base just visits the operand and the unary op is
            // dropped — `r.Score < 0 ? -r.Score : r.Score` silently returned r.Score for
            // BOTH branches. Emit the operator as SQL so the value flips correctly.
            if (node.NodeType is ExpressionType.Negate or ExpressionType.NegateChecked)
            {
                var sb = EnsureBuilder();
                // TimeSpan column negation: route through the seconds-as-REAL hook so
                // the materialiser reconstructs via TimeSpan.FromSeconds(-sec) instead
                // of coercing the column text to a numeric prefix and negating.
                var opTypeNeg = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                if (opTypeNeg == typeof(TimeSpan))
                {
                    var tsStart = sb.Length;
                    Visit(node.Operand);
                    var tsColSql = sb.ToString(tsStart, sb.Length - tsStart);
                    sb.Length = tsStart;
                    sb.Append("(-1.0 * ").Append(_provider.GetTimeSpanColumnSecondsSql(tsColSql)).Append(')');
                    return node;
                }
                sb.Append("-(");
                Visit(node.Operand);
                sb.Append(')');
                return node;
            }
            if (node.NodeType is ExpressionType.Not)
            {
                var sb = EnsureBuilder();
                // C# `!x` on bool and `~x` on integer/enum both compile to
                // ExpressionType.Not -- dispatch on the operand type. SQLite
                // uses NOT for logical and `~` for bitwise.
                var notOperandType = Nullable.GetUnderlyingType(node.Operand.Type) ?? node.Operand.Type;
                if (notOperandType == typeof(bool))
                {
                    sb.Append("NOT (");
                    Visit(node.Operand);
                    sb.Append(')');
                }
                else
                {
                    sb.Append("~(");
                    Visit(node.Operand);
                    sb.Append(')');
                }
                return node;
            }
            return base.VisitUnary(node);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            var sb = EnsureBuilder();
            bool firstColumn = true;
            for (int i = 0; i < node.Bindings.Count; i++)
            {
                if (node.Bindings[i] is MemberAssignment assignment)
                {
                    // Check if this is a navigation collection (bare, or shaped as .Where(pred).ToList()).
                    if (TryMatchDetectedCollection(assignment.Expression, out var navProperty, out var navFilter, out var navProjection, out var navOrdering))
                    {
                        // Track it for later split query processing
                        _detectedCollections.Add(navProperty);
                        if (navFilter != null && RenderShapedCollectionFilter(navProperty, navFilter) is { } rendered)
                            _detectedCollectionFilters[navProperty] = rendered;
                        if (navProjection != null)
                            _detectedCollectionProjections[navProperty] = navProjection;
                        if (navOrdering != null && RenderShapedCollectionOrdering(navProperty, navOrdering) is { } renderedOrdering)
                            _detectedCollectionOrderings[navProperty] = renderedOrdering;
                        // The DTO property may be named differently from the nav; stitch to THIS member.
                        if (assignment.Member is PropertyInfo bindingProp)
                            _detectedCollectionTargetMembers[navProperty] = bindingProp;
                        // Skip adding to SQL SELECT - it will be fetched separately
                        continue;
                    }

                    if (!firstColumn) sb.Append(", ");
                    Visit(assignment.Expression);
                    sb.Append(" AS ").Append(_provider.Escape(assignment.Member.Name));
                    firstColumn = false;
                }
            }
            return node;
        }

        /// <summary>
        /// True when the operand renders raw decimal TEXT storage (column member,
        /// inline constant, or compiled parameter) rather than a computed number;
        /// mirrors the predicate translator's gating for canonical-text equality.
        /// </summary>
        private static bool IsRawStorageOperandScv(Expression e)
        {
            while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                e = u.Operand;
            return e is MemberExpression or ConstantExpression or ParameterExpression;
        }
    }
}
