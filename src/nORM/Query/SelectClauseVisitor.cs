using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed class SelectClauseVisitor : ExpressionVisitor
    {
        private readonly TableMapping _mapping;
        private readonly List<string> _groupBy;
        private readonly DatabaseProvider _provider;
        // Outer-row alias used to reference parent columns from correlated subqueries
        // emitted inside the projection (e.g. `parent.Children.Count()` →
        // `(SELECT COUNT(*) FROM Child WHERE Child.FK = {outerAlias}.PK)`). Defaults to
        // the principal table's escaped name when no JOIN alias is in scope — SQLite
        // accepts table-qualified outer-scope references even without an explicit AS alias.
        private readonly string _outerAlias;
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());
        private StringBuilder? _sb;
        private List<PropertyInfo> _detectedCollections = new();

        /// <summary>SQL aggregate function name for COUNT operations.</summary>
        private const string CountFunctionName = "COUNT";

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider, string? outerAlias = null)
        {
            _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
            _groupBy = groupBy ?? throw new ArgumentNullException(nameof(groupBy));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _outerAlias = outerAlias ?? mapping.EscTable;
        }

        /// <summary>
        /// Gets the list of navigation properties detected during translation that should be
        /// fetched via separate queries to avoid Cartesian explosion.
        /// </summary>
        public IReadOnlyList<PropertyInfo> DetectedCollections => _detectedCollections;

        /// <summary>
        /// Translates a projection expression into a SQL <c>SELECT</c> column list.
        /// Each call resets detected collections, so the result reflects only the
        /// most recent translation.
        /// </summary>
        /// <param name="e">The projection expression to translate.</param>
        /// <returns>SQL representing the projection.</returns>
        public string Translate(Expression e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));

            // Reset per-translation state so consecutive calls don't accumulate stale entries.
            _detectedCollections = new List<PropertyInfo>();

            var sb = _stringBuilderPool.Get();
            _sb = sb;
            try
            {
                Visit(e);
                return sb.ToString();
            }
            finally
            {
                _sb = null;
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var sb = EnsureBuilder();
            bool firstColumn = true;
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                // Members may be null for ValueTuple or similar parameterized constructors;
                // fall back to positional Item name (Item1, Item2, ...).
                var memberName = node.Members?[i]?.Name ?? $"Item{i + 1}";

                // Check if this is a navigation property (collection)
                if (IsNavigationCollection(arg, out var navProperty))
                {
                    // Track it for later split query processing
                    _detectedCollections.Add(navProperty);
                    // Skip adding to SQL SELECT - it will be fetched separately
                    continue;
                }

                if (!firstColumn) sb.Append(", ");
                Visit(arg);
                sb.Append(" AS ").Append(_provider.Escape(memberName));
                firstColumn = false;
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var sb = EnsureBuilder();
            // `entity.GetType().Name/FullName/Namespace/AssemblyQualifiedName` --
            // fold to the receiver's declared compile-time type. nORM doesn't
            // emit a runtime discriminator at projection-time, so the value is
            // always the declared entity type. Mirror Type.<member> on
            // typeof(T) which the existing constant-fold already handles.
            // Note: Type.Name etc. are declared on MemberInfo (its base), so we
            // check the receiver type rather than node.Member.DeclaringType.
            if (node.Expression is MethodCallExpression gtCall
                && gtCall.Method.Name == "GetType"
                && gtCall.Arguments.Count == 0
                && gtCall.Object != null
                && typeof(Type).IsAssignableFrom(gtCall.Type))
            {
                var declaredType = gtCall.Object.Type;
                string? value = node.Member.Name switch
                {
                    nameof(Type.Name) => declaredType.Name,
                    nameof(Type.FullName) => declaredType.FullName,
                    nameof(Type.Namespace) => declaredType.Namespace,
                    nameof(Type.AssemblyQualifiedName) => declaredType.AssemblyQualifiedName,
                    _ => null
                };
                if (value != null)
                {
                    sb.Append('\'').Append(value.Replace("'", "''")).Append('\'');
                    return node;
                }
            }
            if (node.Expression is ParameterExpression p && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>) && node.Member.Name == "Key")
            {
                for (int i = 0; i < _groupBy.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append(_groupBy[i]);
                }
                return node;
            }

            if (_mapping.ColumnsByName.TryGetValue(node.Member.Name, out var col))
            {
                sb.Append(col.EscCol);
                return node;
            }

            // Member on a non-entity type (DateTime.Year, string.Length, TimeSpan.TotalHours,
            // etc.) — route through the provider's function map the same way
            // ExpressionToSqlVisitor does on the WHERE side. Without this, projection emits
            // "Member 'Year' on type 'DateTime' is not mapped to a column" even though the
            // WHERE side happily accepts the same expression.
            if (node.Expression != null && node.Member.DeclaringType != null)
            {
                var exprSql = TranslateProjectionArg(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType, exprSql);
                if (fn != null)
                {
                    sb.Append(fn);
                    return node;
                }
            }

            // Closure-captured local (compiler-generated DisplayClass member) -- evaluate
            // to a constant and emit as a literal. ExpressionToSqlVisitor binds these as
            // parameters via CreateSafeParameter, but SCV writes SQL fragments without
            // parameter-manager access, so inline as a literal. Mirrors the constant-fold
            // ETSV path so projection accepts the same captured-local shapes Where does
            // -- without this, `var prefix = "x"; .Select(p => p.Name.StartsWith(prefix))`
            // throws the misleading "not mapped to a column" error pointing at the
            // DisplayClass field.
            if (QueryTranslator.TryGetConstantValue(node, out var capturedValue))
            {
                sb.Append(FormatLiteral(capturedValue));
                return node;
            }

            throw new InvalidOperationException(
                $"Member '{node.Member.Name}' on type '{node.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                $"Ensure the property is read/write and not a navigation collection.");
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var sb = EnsureBuilder();

            // string.Trim / TrimStart / TrimEnd with explicit char[] -- the
            // generic provider route would expand the char[] as multiple
            // SQL args and SQLite TRIM accepts only (s) or (s, trim_chars_str)
            // -> 'wrong number of arguments to function TRIM()'. Detect the
            // params-char[] overload, evaluate the array at translation time,
            // and emit TRIM(s, 'concatenated_chars').
            if ((node.Method.Name == nameof(string.Trim)
                    || node.Method.Name == nameof(string.TrimStart)
                    || node.Method.Name == nameof(string.TrimEnd))
                && node.Method.DeclaringType == typeof(string)
                && node.Object != null
                && node.Arguments.Count == 1
                && node.Arguments[0].Type == typeof(char[]))
            {
                // Extract the char[] manually. TryGetConstantValue refuses
                // NewArrayInit / MethodCall (security-restricted to prevent
                // RCE), so walk NewArrayInit elements ourselves; for foldable
                // MemberExpression we re-use TryGetConstantValue.
                char[]? chars = null;
                if (node.Arguments[0] is NewArrayExpression nae && nae.NodeType == ExpressionType.NewArrayInit)
                {
                    var arr = new char[nae.Expressions.Count];
                    bool allConst = true;
                    for (int i = 0; i < nae.Expressions.Count; i++)
                    {
                        if (nae.Expressions[i] is ConstantExpression ec && ec.Value is char ch)
                        {
                            arr[i] = ch;
                        }
                        else
                        {
                            allConst = false;
                            break;
                        }
                    }
                    if (allConst) chars = arr;
                }
                else if (QueryTranslator.TryGetConstantValue(node.Arguments[0], out var arrVal) && arrVal is char[] capturedChars)
                {
                    chars = capturedChars;
                }
                if (chars is { Length: > 0 })
                {
                    var sqlFn = node.Method.Name switch
                    {
                        nameof(string.TrimStart) => "LTRIM",
                        nameof(string.TrimEnd) => "RTRIM",
                        _ => "TRIM",
                    };
                    var receiverStart = sb.Length;
                    Visit(node.Object);
                    var receiverSql = sb.ToString(receiverStart, sb.Length - receiverStart);
                    sb.Length = receiverStart;
                    var trimSet = new string(chars).Replace("'", "''");
                    sb.Append(sqlFn).Append('(').Append(receiverSql).Append(", '").Append(trimSet).Append("')");
                    return node;
                }
            }

            // string.Join(separator, params string[] values) -- the C# variadic
            // form compiles to a MethodCall with a NewArrayInit second arg
            // holding the value expressions. The args array passed to the
            // provider's TranslateFunction collapses the NewArrayInit into a
            // single opaque entry, so this handler lives in SCV where we can
            // pull each element directly and call TranslateProjectionArg per
            // element, then interleave a constant separator literal.
            if (node.Method.DeclaringType == typeof(string)
                && node.Method.Name == nameof(string.Join)
                && node.Arguments.Count == 2
                && node.Arguments[1] is NewArrayExpression joinArr
                && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var joinSepVal)
                && joinSepVal is string joinSep)
            {
                var sepLit = $"'{joinSep.Replace("'", "''")}'";
                if (joinArr.Expressions.Count == 0)
                {
                    sb.Append("''");
                    return node;
                }
                var elemSqls = joinArr.Expressions.Select(e => TranslateProjectionArg(e)).ToList();
                sb.Append('(').Append(string.Join($" || {sepLit} || ", elemSqls)).Append(')');
                return node;
            }

            // string.StartsWith / EndsWith / Contains -- the projection-time
            // shape needs LIKE-wildcard escaping the generic provider route
            // can't do (the provider only sees pre-rendered SQL fragments,
            // not the raw pattern value). Mirror ExpressionToSqlVisitor's
            // CreateSafeLikePattern: extract the constant pattern, escape
            // %, _, and the escape char itself, prepend/append % per op,
            // then emit `(col LIKE 'escaped%' ESCAPE 'X')`. Variable patterns
            // (column refs or non-foldable expressions) fall through to the
            // existing provider route which uses plain || concat without
            // escape -- acceptable since column-sourced patterns rarely
            // intentionally embed wildcards.
            if (node.Object != null
                && node.Method.DeclaringType == typeof(string)
                && (node.Arguments.Count == 1
                    || (node.Arguments.Count == 2 && node.Arguments[1].Type == typeof(StringComparison)))
                && (node.Method.Name == nameof(string.StartsWith)
                    || node.Method.Name == nameof(string.EndsWith)
                    || node.Method.Name == nameof(string.Contains))
                && QueryTranslator.TryGetConstantValue(node.Arguments[0], out var rawPattern)
                && rawPattern is string patternStr)
            {
                // 2-arg overload: extract the StringComparison and lower both
                // sides for ignore-case variants -- SQLite LIKE is BINARY by
                // default but LOWER/LIKE-LOWER is the portable equivalent. The
                // ETSV side does the same.
                bool ignoreCase = false;
                if (node.Arguments.Count == 2
                    && QueryTranslator.TryGetConstantValue(node.Arguments[1], out var cmpVal)
                    && cmpVal is StringComparison cmpMode
                    && cmpMode is StringComparison.OrdinalIgnoreCase
                        or StringComparison.CurrentCultureIgnoreCase
                        or StringComparison.InvariantCultureIgnoreCase)
                {
                    ignoreCase = true;
                }
                var receiverSql = TranslateProjectionArg(node.Object);
                var escapeChar = nORM.Core.NormValidator.ValidateLikeEscapeChar(_provider.LikeEscapeChar);
                var effectivePattern = ignoreCase ? patternStr.ToLowerInvariant() : patternStr;
                var escaped = _provider.EscapeLikePattern(effectivePattern);
                var wrapped = node.Method.Name switch
                {
                    nameof(string.StartsWith) => $"{escaped}%",
                    nameof(string.EndsWith) => $"%{escaped}",
                    _ => $"%{escaped}%",
                };
                var lhs = ignoreCase ? $"LOWER({receiverSql})" : receiverSql;
                sb.Append('(').Append(lhs).Append(" LIKE '")
                  .Append(wrapped.Replace("'", "''"))
                  .Append("' ESCAPE '").Append(escapeChar).Append("')");
                return node;
            }

            // string.Equals with a StringComparison argument -- mirror
            // ExpressionToSqlVisitor's HandleStringEqualsStatic/Instance
            // fast handlers so projection and Where return identical truths.
            // The static overload takes (a, b, comparison); instance is
            // (b, comparison). Wrap both sides in LOWER for ignore-case
            // variants; otherwise emit '='. Without this branch the
            // analyzer accepts Equals but SCV emits raw 'EQUALS(...)'
            // and SQLite throws 'no such function'.
            if (node.Method.Name == nameof(string.Equals)
                && node.Method.DeclaringType == typeof(string))
            {
                Expression? lhs = null, rhs = null, comparison = null;
                if (node.Object == null
                    && node.Arguments.Count == 3
                    && node.Arguments[2].Type == typeof(StringComparison))
                {
                    lhs = node.Arguments[0];
                    rhs = node.Arguments[1];
                    comparison = node.Arguments[2];
                }
                else if (node.Object != null
                    && node.Arguments.Count == 2
                    && node.Arguments[1].Type == typeof(StringComparison))
                {
                    lhs = node.Object;
                    rhs = node.Arguments[0];
                    comparison = node.Arguments[1];
                }
                if (lhs != null && rhs != null && comparison != null)
                {
                    var lhsSql = TranslateProjectionArg(lhs);
                    var rhsSql = TranslateProjectionArg(rhs);
                    bool ignoreCase = QueryTranslator.TryGetConstantValue(comparison, out var cv)
                        && cv is StringComparison sc
                        && (sc is StringComparison.OrdinalIgnoreCase
                                or StringComparison.InvariantCultureIgnoreCase
                                or StringComparison.CurrentCultureIgnoreCase);
                    if (ignoreCase)
                        sb.Append("(LOWER(").Append(lhsSql).Append(") = LOWER(").Append(rhsSql).Append("))");
                    else
                        sb.Append('(').Append(lhsSql).Append(" = ").Append(rhsSql).Append(')');
                    return node;
                }
            }

            // numeric.ToString(formatString) -- the most-common case is a fixed-
            // decimal "F<N>" / "f<N>" format like Score.ToString("F2") for currency
            // / report rendering. SQLite has printf('%.<N>f', col); other providers
            // expose similar primitives. Detect the constant format string at
            // translation time, route to printf when it matches F<N>, otherwise
            // throw with the supported-subset hint so users get a clear error
            // rather than a silent CAST that drops the format.
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 1
                && node.Object != null
                && node.Object.Type != typeof(string)
                && node.Arguments[0].Type == typeof(string))
            {
                if (QueryTranslator.TryGetConstantValue(node.Arguments[0], out var rawFmt)
                    && rawFmt is string fmt)
                {
                    // Numeric F<N>/f<N> fixed-decimal -> printf('%.<N>f', ...).
                    if (fmt.Length >= 2
                        && (fmt[0] == 'F' || fmt[0] == 'f')
                        && int.TryParse(fmt.AsSpan(1), out var digits)
                        && digits >= 0 && digits <= 17)
                    {
                        var recvSql = TranslateProjectionArg(node.Object);
                        sb.Append("printf('%.").Append(digits).Append("f', ").Append(recvSql).Append(')');
                        return node;
                    }

                    // DateTime/DateTimeOffset/DateOnly/TimeOnly format strings:
                    // map the common .NET tokens (yyyy/MM/dd/HH/mm/ss) to strftime
                    // tokens, preserving literal punctuation. Locale-aware tokens
                    // (MMM month name, dddd day name, etc.) are not supported --
                    // those require culture data SQLite doesn't carry.
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type;
                    if (underlying == typeof(DateTime)
                        || underlying == typeof(DateTimeOffset)
                        || underlying == typeof(DateOnly)
                        || underlying == typeof(TimeOnly))
                    {
                        if (TryConvertDotNetDateFormatToStrftime(fmt, out var strftimeFmt))
                        {
                            var recvSql = TranslateProjectionArg(node.Object);
                            sb.Append("strftime('").Append(strftimeFmt).Append("', ").Append(recvSql).Append(')');
                            return node;
                        }
                        throw new InvalidOperationException(
                            $"DateTime ToString(\"{fmt}\") format is not supported in projection. " +
                            "Supported tokens: yyyy yy MM dd HH mm ss; literal characters are passed through. " +
                            "Locale-aware tokens (MMM/MMMM month names, dddd/ddd day names) are unavailable in " +
                            "SQLite; project the DateTime and format after materialization.");
                    }
                }
                throw new InvalidOperationException(
                    $"ToString(\"{node.Arguments[0]}\") format is not supported in projection on type '{node.Object.Type.Name}'. " +
                    "Supported subset: numeric \"F<N>\"/\"f<N>\" and DateTime tokens yyyy/yy/MM/dd/HH/mm/ss. " +
                    "Project the value and apply the format after materialization for other shapes.");
            }

            // No-arg ToString() on a non-string receiver -- lower to the provider's
            // CAST AS TEXT for primitives, or CASE-WHEN-per-name for enums. Mirrors
            // ExpressionToSqlVisitor's matching handlers so projection and predicate
            // paths agree on shape.
            if (node.Method.Name == nameof(object.ToString)
                && node.Arguments.Count == 0
                && node.Object != null
                && node.Object.Type != typeof(string))
            {
                var receiverStart = sb.Length;
                Visit(node.Object);
                var receiverSql = sb.ToString(receiverStart, sb.Length - receiverStart);
                sb.Length = receiverStart;
                var underlying = Nullable.GetUnderlyingType(node.Object.Type) ?? node.Object.Type;
                if (underlying.IsEnum)
                {
                    sb.Append(ExpressionToSqlVisitor.BuildEnumToStringCase(_provider, receiverSql, underlying));
                }
                else if (underlying == typeof(bool))
                {
                    // .NET bool.ToString returns "True" / "False" (capitalized) --
                    // CAST AS TEXT on a 0/1 column returns "0"/"1" instead. Emit
                    // an explicit CASE so the projected text matches .NET.
                    sb.Append("(CASE WHEN ").Append(receiverSql).Append(" = 1 THEN 'True' ELSE 'False' END)");
                }
                else
                {
                    sb.Append(_provider.GetToStringSql(receiverSql));
                }
                return node;
            }

            // `parent.Children.Count()` (or Any/All/LongCount) inside a Select projection.
            // ExpressionToSqlVisitor recognises this shape and rewrites it into a correlated
            // subquery (`(SELECT COUNT(*) FROM Child WHERE Child.FK = parent.PK)`), but this
            // visitor previously fell through to the generic aggregate path and emitted
            // `COUNT(*)` against the outer table — turning the per-row projection into a
            // single-row aggregate result. Detect the shape and emit the correlated
            // subquery here too so `Select(p => new {p.Name, Count = p.Children.Count()})`
            // returns one row per parent with the right child count.
            // Try to unwrap an optional `Where(predicate)` between the navigation and the
            // aggregate so `parent.Children.Where(c => c.IsActive).Count()` translates the
            // same way `parent.Children.Count()` does, just AND-ing the predicate into the
            // subquery WHERE. Without this, the Count fell back to `COUNT(*) FROM Parent`
            // (no row-per-parent, no filter) — silent-wrongness identical to 0977c64.
            Expression navCandidate = node.Arguments.Count >= 1 ? node.Arguments[0] : null!;
            LambdaExpression? navFilter = null;
            if (navCandidate is MethodCallExpression whereCall
                && whereCall.Method.Name == nameof(Queryable.Where)
                && whereCall.Arguments.Count == 2
                && StripQuotes(whereCall.Arguments[1]) is LambdaExpression whereLambda)
            {
                navFilter = whereLambda;
                navCandidate = whereCall.Arguments[0];
            }
            if (navCandidate is MemberExpression navMember
                && navMember.Expression is ParameterExpression
                && _mapping.Relations.TryGetValue(navMember.Member.Name, out var relation)
                && (node.Method.Name is nameof(Queryable.Count)
                                     or nameof(Queryable.LongCount)
                                     or nameof(Queryable.Any)
                                     or nameof(Queryable.All)))
            {
                // Two equivalent surface forms:
                //   p.Children.Where(predicate).Count()         — unwrapped above (navFilter)
                //   p.Children.Count(predicate)                 — predicate on the Count itself
                // Both must AND the predicate into the subquery WHERE. Without this, the
                // 2-arg overload silently drops the predicate and returns the total count.
                // All(predicate) only has the 2-arg form — its predicate must arrive here too.
                if (navFilter == null
                    && node.Arguments.Count == 2
                    && StripQuotes(node.Arguments[1]) is LambdaExpression directPred)
                {
                    navFilter = directPred;
                }
                EmitNavigationCountSubquery(sb, node, relation, navFilter);
                return node;
            }

            // `parent.Children[.Where(pred)].Select(c => c.X).Sum/Min/Max/Average()` —
            // Select interposes between the navigation and the aggregate, optionally with a
            // Where between the navigation and the Select. Mirror of ExpressionToSqlVisitor's
            // efba58f path. Without the Where unwrap, the filter silently vanished AND the
            // emit produced `SUM()` (no column ref → "wrong number of arguments to SUM").
            if (node.Arguments.Count == 1
                && node.Method.Name is nameof(Queryable.Sum)
                                   or nameof(Queryable.Min)
                                   or nameof(Queryable.Max)
                                   or nameof(Queryable.Average)
                && node.Arguments[0] is MethodCallExpression selCall
                && selCall.Method.Name == nameof(Queryable.Select)
                && selCall.Arguments.Count == 2
                && StripQuotes(selCall.Arguments[1]) is LambdaExpression selectorLambda)
            {
                Expression selSource = selCall.Arguments[0];
                LambdaExpression? selFilter = null;
                if (selSource is MethodCallExpression preWhere
                    && preWhere.Method.Name == nameof(Queryable.Where)
                    && preWhere.Arguments.Count == 2
                    && StripQuotes(preWhere.Arguments[1]) is LambdaExpression preWhereLambda)
                {
                    selFilter = preWhereLambda;
                    selSource = preWhere.Arguments[0];
                }
                if (selSource is MemberExpression selNav
                    && selNav.Expression is ParameterExpression
                    && _mapping.Relations.TryGetValue(selNav.Member.Name, out var selRelation))
                {
                    EmitNavigationScalarAggregateSubquery(sb, node.Method.Name, selRelation, selectorLambda, selFilter);
                    return node;
                }
            }

            // Direct selector overload: `parent.Children[.Where(pred)].Sum/Min/Max/Average(c => c.X)`.
            // EF Core users naturally reach for this 2-arg form first; the Select-then-Sum form
            // above only matches the 1-arg overload. Without this branch the selector lambda is
            // visited against the parent mapping and throws "Member 'X' on type 'Child' is not
            // mapped to a column in table 'Parent'", surfaced as a parity gap in 2bffa3f.
            if (node.Arguments.Count == 2
                && node.Method.Name is nameof(Queryable.Sum)
                                   or nameof(Queryable.Min)
                                   or nameof(Queryable.Max)
                                   or nameof(Queryable.Average)
                && StripQuotes(node.Arguments[1]) is LambdaExpression directSelectorLambda)
            {
                Expression directSource = node.Arguments[0];
                LambdaExpression? directFilter = null;
                if (directSource is MethodCallExpression directWhere
                    && directWhere.Method.Name == nameof(Queryable.Where)
                    && directWhere.Arguments.Count == 2
                    && StripQuotes(directWhere.Arguments[1]) is LambdaExpression directWhereLambda)
                {
                    directFilter = directWhereLambda;
                    directSource = directWhere.Arguments[0];
                }
                if (directSource is MemberExpression directNav
                    && directNav.Expression is ParameterExpression
                    && _mapping.Relations.TryGetValue(directNav.Member.Name, out var directRelation))
                {
                    EmitNavigationScalarAggregateSubquery(sb, node.Method.Name, directRelation, directSelectorLambda, directFilter);
                    return node;
                }
            }

            // Use ToUpperInvariant to avoid locale-sensitive casing (e.g., Turkish-I problem).
            var methodNameUpper = node.Method.Name.ToUpperInvariant();

            // static string.Format("template", args...) — emit as a provider concat of
            // literal pieces and projected argument columns. Format specifiers (`{0:N2}` /
            // `{0,5}`) fall through to the aggregate path (which throws), matching the
            // WHERE-side behaviour: simple positional placeholders translate, anything
            // richer needs client-eval.
            if (node.Method.DeclaringType == typeof(string)
                && node.Object == null
                && node.Method.Name == nameof(string.Format)
                && node.Arguments.Count >= 2
                && node.Arguments[0] is ConstantExpression { Value: string template })
            {
                var segments = TryParseFormatSegments(template);
                if (segments != null)
                {
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
                            if (seg.ArgIndex >= argExprs.Count) goto fallthrough;
                            var arg = argExprs[seg.ArgIndex];
                            var inner = TranslateProjectionArg(arg);
                            if (arg.Type != typeof(string))
                                inner = _provider.GetToStringSql(inner);
                            parts.Add(inner);
                        }
                    }
                    if (parts.Count == 0) { sb.Append("''"); return node; }
                    sb.Append(parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next)));
                    return node;
                }
            }
            fallthrough:

            // Scalar/named functions on known static surfaces (Math.Abs, Math.Min,
            // DateTime.AddDays, Convert.ToInt32, NormFunctions.ILike, etc.). The
            // Queryable-aggregate fall-through below hard-assumes the 2-arg
            // (source, selector-lambda) shape used by Sum/Min/Max/Count, so without
            // this routing `Math.Abs(p.Score)` emits `ABS(*)` and `Math.Min(p.A, p.B)`
            // throws "Expected a lambda expression as argument 1 of 'Min'".
            // Enumerable/Queryable are excluded so the aggregate path still handles
            // grouping-aggregate calls.
            var declType = node.Method.DeclaringType;
            if (declType != null && declType != typeof(Enumerable) && declType != typeof(Queryable))
            {
                var fnArgs = new string[node.Arguments.Count + (node.Object != null ? 1 : 0)];
                int fnIdx = 0;
                if (node.Object != null) fnArgs[fnIdx++] = TranslateProjectionArg(node.Object);
                foreach (var a in node.Arguments) fnArgs[fnIdx++] = TranslateProjectionArg(a);
                // Overload-aware hook first -- providers may dispatch on the
                // full MethodInfo when the stringified args alone are ambiguous.
                var overloadSql = _provider.TranslateMethodCall(node, fnArgs);
                if (overloadSql != null)
                {
                    sb.Append(overloadSql);
                    return node;
                }
                var fnSql = _provider.TranslateFunction(node.Method.Name, declType, fnArgs);
                if (fnSql != null)
                {
                    sb.Append(fnSql);
                    return node;
                }
            }

            sb.Append(methodNameUpper).Append('(');
            if (node.Arguments.Count > 1)
            {
                // StripQuotes handles LINQ's UnaryExpression{Quote()} wrapping around lambdas.
                if (StripQuotes(node.Arguments[1]) is not LambdaExpression lambda)
                    throw new InvalidOperationException(
                        $"Expected a lambda expression as argument 1 of '{node.Method.Name}', but got '{node.Arguments[1].NodeType}'.");
                if (lambda.Body is MemberExpression me)
                {
                    if (!_mapping.ColumnsByName.TryGetValue(me.Member.Name, out var col))
                        throw new InvalidOperationException(
                            $"Member '{me.Member.Name}' on type '{me.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                            $"Ensure the property is read/write and not a navigation collection.");
                    sb.Append(col.EscCol);
                }
                else
                {
                    // Computed selector: arithmetic, conditional, etc. Visit the body so the
                    // ConditionalExpression / BinaryExpression overrides turn it into SQL.
                    Visit(lambda.Body);
                }
            }
            else if (!string.Equals(methodNameUpper, CountFunctionName, StringComparison.Ordinal))
            {
                sb.Append(_groupBy.FirstOrDefault() ?? "*");
            }
            else
            {
                sb.Append('*');
            }
            sb.Append(')');
            return node;
        }

        private void EmitNavigationCountSubquery(StringBuilder sb, MethodCallExpression node, TableMapping.Relation relation, LambdaExpression? extraFilter)
        {
            // Resolve the dependent table mapping from the relation's DependentType. The
            // navigation registration on Relations doesn't carry the dependent TableMapping
            // directly, so look it up via the principal mapping's column lookup (Relations is
            // populated post-build, so we can reach DependentType.Mapping through the
            // ForeignKey property — which IS on the dependent type).
            var depType = relation.DependentType;
            // We need an escaped table identifier and column for the dependent. Since SCV
            // doesn't have a TableMappingProvider, build the SQL identifiers manually from
            // attributes — match the same convention TableMapping uses (table name from
            // [Table] attribute or type name; columns from property names escaped by provider).
            var depTable = depType.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), inherit: false)
                .Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>().FirstOrDefault()?.Name ?? depType.Name;
            var depAlias = _provider.Escape("__nav");
            var fkCol = _provider.Escape(relation.ForeignKey.Prop.Name);
            var pkCol = _provider.Escape(relation.PrincipalKey.Prop.Name);
            // Outer alias / table-name reference for parent columns — set via SCV ctor.
            var outerAlias = _outerAlias;
            var extraFilterSql = extraFilter != null
                ? RenderNavigationFilter(extraFilter, depAlias)
                : null;
            // Use predicate-overload Count(predicate) sugar when the unwrapped filter is the
            // first / only filter and the outer call is Count() — keeps the SQL compact.
            // For Any/All, AND the filter into the EXISTS / NOT EXISTS subquery's WHERE.

            sb.Append('(').Append("SELECT ");
            if (node.Method.Name is nameof(Queryable.Any))
            {
                sb.Append("CASE WHEN EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
                sb.Append(") THEN 1 ELSE 0 END");
            }
            else if (node.Method.Name is nameof(Queryable.All))
            {
                sb.Append("CASE WHEN NOT EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol);
                // All(p) ≡ NOT EXISTS(row matching NOT p) — invert the extra filter, not AND it.
                if (extraFilterSql != null) sb.Append(" AND NOT (").Append(extraFilterSql).Append(')');
                sb.Append(") THEN 1 ELSE 0 END");
            }
            else
            {
                sb.Append("COUNT(*) FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol);
                if (extraFilterSql != null) sb.Append(" AND ").Append(extraFilterSql);
            }
            sb.Append(')');
        }

        private string RenderNavigationFilter(LambdaExpression filter, string depAlias)
        {
            // Render a simple `c => c.X op constant` style predicate against the dependent
            // alias. Supports BinaryExpression with member-access on one side and a constant
            // (or member of a closure) on the other — the same surface area as the rest of
            // SCV's translatable subset. More complex predicates fall through to the throw
            // below which routes back to client-eval messaging.
            if (filter.Body is BinaryExpression be)
            {
                var lhs = RenderFilterSide(be.Left, filter.Parameters[0], depAlias);
                var rhs = RenderFilterSide(be.Right, filter.Parameters[0], depAlias);
                var op = be.NodeType switch
                {
                    ExpressionType.Equal => "=",
                    ExpressionType.NotEqual => "<>",
                    ExpressionType.GreaterThan => ">",
                    ExpressionType.GreaterThanOrEqual => ">=",
                    ExpressionType.LessThan => "<",
                    ExpressionType.LessThanOrEqual => "<=",
                    ExpressionType.AndAlso => "AND",
                    ExpressionType.OrElse => "OR",
                    _ => throw new InvalidOperationException(
                        $"Navigation filter binary operator '{be.NodeType}' isn't yet supported in a projection subquery. " +
                        "Use a simple comparison (==, !=, <, >, <=, >=, &&, ||) or wrap with `ClientEvaluationPolicy.Allow`.")
                };
                return $"{lhs} {op} {rhs}";
            }
            throw new InvalidOperationException(
                "Navigation filter inside a projection subquery currently supports only simple binary predicates " +
                "(`c => c.X op constant`). For more complex shapes, wrap with `ClientEvaluationPolicy.Allow`.");
        }

        private string RenderFilterSide(Expression expr, ParameterExpression elementParam, string depAlias)
        {
            // Member access on the element parameter → column on the dependent.
            if (expr is MemberExpression me && me.Expression == elementParam)
            {
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                return $"{depAlias}.{_provider.Escape(colName)}";
            }
            // Constant or closure-captured value → literal-ize. The projection-subquery
            // emit doesn't have access to the parameter manager here, so inline as a SQL
            // literal — only handles simple ints, doubles, strings, bools. Anything else
            // falls through to the throw.
            if (expr is ConstantExpression ce)
                return FormatLiteral(ce.Value);
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert } u && u.Operand is ConstantExpression ce2)
                return FormatLiteral(ce2.Value);
            if (expr is MemberExpression closureMe && QueryTranslator.TryGetConstantValue(closureMe, out var closureVal))
                return FormatLiteral(closureVal);
            throw new InvalidOperationException(
                $"Navigation filter side '{expr}' isn't a simple member access or constant — only `c.X op constant` is supported in a projection subquery.");
        }

        private static string FormatLiteral(object? value)
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
                string s => $"'{s.Replace("'", "''")}'",
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

        private void EmitNavigationScalarAggregateSubquery(StringBuilder sb, string methodName, TableMapping.Relation relation, LambdaExpression selectorLambda, LambdaExpression? extraFilter = null)
        {
            // Mirror of EmitNavigationCountSubquery — adds aggregate-function dispatch and
            // a per-row selector. Selector is a member access on the dependent element
            // (e.g. `c => c.Amount`); resolve it to the column name via attribute lookup.
            var depType = relation.DependentType;
            var depTable = depType.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), inherit: false)
                .Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>().FirstOrDefault()?.Name ?? depType.Name;
            var depAlias = _provider.Escape("__nav");
            var fkCol = _provider.Escape(relation.ForeignKey.Prop.Name);
            var pkCol = _provider.Escape(relation.PrincipalKey.Prop.Name);

            // Resolve the selector to a column name. Only a simple member access is supported
            // here (matching efba58f scope) — `c => c.X` not `c => c.X + 1`.
            string selectorSql;
            if (selectorLambda.Body is MemberExpression me)
            {
                var colAttr = me.Member.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute), inherit: false)
                    .Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>().FirstOrDefault();
                var colName = colAttr?.Name ?? me.Member.Name;
                selectorSql = $"{depAlias}.{_provider.Escape(colName)}";
            }
            else
            {
                throw new InvalidOperationException(
                    "Navigation aggregate Select(c => …).Sum/Min/Max/Average in a projection currently supports only a bare member access selector (`c => c.X`). " +
                    "Computed selectors (e.g. `c => c.A + c.B`) aren't yet routed through the correlated subquery emit — wrap with `ClientEvaluationPolicy.Allow` or aggregate after materialising.");
            }

            var sqlAgg = methodName switch
            {
                nameof(Queryable.Sum) => "SUM",
                nameof(Queryable.Min) => "MIN",
                nameof(Queryable.Max) => "MAX",
                nameof(Queryable.Average) => "AVG",
                _ => methodName.ToUpperInvariant()
            };

            sb.Append('(').Append("SELECT ").Append(sqlAgg).Append('(').Append(selectorSql).Append(')')
              .Append(" FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
              .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(_outerAlias).Append('.').Append(pkCol);
            if (extraFilter != null)
            {
                var filterSql = RenderNavigationFilter(extraFilter, depAlias);
                sb.Append(" AND ").Append(filterSql);
            }
            sb.Append(')');
        }

        private string TranslateProjectionArg(Expression arg)
        {
            var saved = _sb;
            var tmp = new StringBuilder();
            _sb = tmp;
            try
            {
                Visit(arg);
                return tmp.ToString();
            }
            finally
            {
                _sb = saved;
            }
        }

        private readonly struct FormatSegment
        {
            public readonly bool IsLiteral;
            public readonly string? Literal;
            public readonly int ArgIndex;
            public FormatSegment(string l) { IsLiteral = true; Literal = l; ArgIndex = -1; }
            public FormatSegment(int i) { IsLiteral = false; Literal = null; ArgIndex = i; }
        }

        /// <summary>
        /// Translates a .NET DateTime custom format string into a SQLite
        /// strftime format string. Returns false on any unsupported token
        /// (notably locale-aware MMM/MMMM/dddd/ddd). Single-quote characters
        /// in literal segments are doubled per SQL string-literal rules so the
        /// caller can wrap the result in '...'. Strftime % is escaped as %%.
        /// </summary>
        internal static bool TryConvertDotNetDateFormatToStrftime(string fmt, out string strftime)
        {
            var sb = new StringBuilder(fmt.Length + 4);
            int i = 0;
            while (i < fmt.Length)
            {
                if (i + 4 <= fmt.Length && fmt[i] == 'y' && fmt[i + 1] == 'y' && fmt[i + 2] == 'y' && fmt[i + 3] == 'y')
                { sb.Append("%Y"); i += 4; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'y' && fmt[i + 1] == 'y')
                { sb.Append("%y"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'M' && fmt[i + 1] == 'M')
                { sb.Append("%m"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'd' && fmt[i + 1] == 'd')
                { sb.Append("%d"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'H' && fmt[i + 1] == 'H')
                { sb.Append("%H"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 'm' && fmt[i + 1] == 'm')
                { sb.Append("%M"); i += 2; continue; }
                if (i + 2 <= fmt.Length && fmt[i] == 's' && fmt[i + 1] == 's')
                { sb.Append("%S"); i += 2; continue; }
                // Reject locale-aware and unsupported single-character tokens
                // before they end up in the strftime literal as themselves.
                char c = fmt[i];
                if (c == 'M' || c == 'd' || c == 'H' || c == 'h' || c == 'm'
                    || c == 's' || c == 'y' || c == 'f' || c == 'F' || c == 'z' || c == 'K' || c == 't')
                {
                    strftime = string.Empty;
                    return false;
                }
                if (c == '\'') sb.Append("''");
                else if (c == '%') sb.Append("%%");
                else sb.Append(c);
                i++;
            }
            strftime = sb.ToString();
            return true;
        }

        private static List<FormatSegment>? TryParseFormatSegments(string template)
        {
            var segments = new List<FormatSegment>();
            var literal = new StringBuilder();
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
                    return null;
                }
                else { literal.Append(c); i++; }
            }
            if (literal.Length > 0) segments.Add(new FormatSegment(literal.ToString()));
            return segments;
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            var sb = EnsureBuilder();
            sb.Append("(CASE WHEN ");
            Visit(node.Test);
            sb.Append(" THEN ");
            Visit(node.IfTrue);
            sb.Append(" ELSE ");
            Visit(node.IfFalse);
            sb.Append(" END)");
            return node;
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
                sb.Append(_provider.GetConcatSql(leftSql, rightSql));
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
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(DateTime)
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
                var sign = node.NodeType == ExpressionType.Add ? "+" : "-";
                sb.Append("RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', ").Append(leftSql)
                  .Append(", '").Append(sign).Append("' || (CAST(substr(").Append(rightSql).Append(", 1, 2) AS INTEGER) * 3600 + ")
                  .Append("CAST(substr(").Append(rightSql).Append(", 4, 2) AS INTEGER) * 60 + ")
                  .Append("CAST(substr(").Append(rightSql).Append(", 7, 2) AS INTEGER)) || ' seconds'), '0'), '.')");
                return node;
            }
            // DateTime + TimeSpan / DateTime - TimeSpan -> DateTime. The
            // TimeSpan operand folds via TryGetConstantValue; emit
            // strftime + RTRIM to match Microsoft.Data.Sqlite's FFFFFFF
            // binding so the result round-trips for downstream comparisons.
            if ((node.NodeType == ExpressionType.Add || node.NodeType == ExpressionType.Subtract)
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(DateTime)
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
                sb.Append("RTRIM(RTRIM(strftime('%Y-%m-%d %H:%M:%f', ").Append(leftSql)
                  .Append(", '").Append(secondsLiteral).Append(" seconds'), '0'), '.')");
                return node;
            }
            // DateTime - DateTime in projection -> TimeSpan. SQL '-' on TEXT
            // columns returns 0 (silent-wrongness); convert via julianday math
            // and tag the column with the marker the materializer recognizes
            // for "REAL seconds -> TimeSpan.FromSeconds" conversion.
            if (node.NodeType == ExpressionType.Subtract
                && (Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type) == typeof(DateTime)
                && (Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type) == typeof(DateTime))
            {
                var leftStart = sb.Length;
                Visit(node.Left);
                var leftSql = sb.ToString(leftStart, sb.Length - leftStart);
                sb.Length = leftStart;
                var rightStart = sb.Length;
                Visit(node.Right);
                var rightSql = sb.ToString(rightStart, sb.Length - rightStart);
                sb.Length = rightStart;
                // (julianday(end) - julianday(start)) * 86400 -> REAL seconds.
                // Materializer reads as double; user expects TimeSpan -- handled
                // by MaterializerFactory's GetFieldValue path which converts a
                // numeric reader value to TimeSpan via TimeSpan.FromSeconds.
                sb.Append("((julianday(").Append(leftSql).Append(") - julianday(")
                  .Append(rightSql).Append(")) * 86400.0)");
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

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Numeric / enum / primitive Convert: the SQL value is the operand itself.
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
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
                    // Check if this is a navigation property (collection)
                    if (IsNavigationCollection(assignment.Expression, out var navProperty))
                    {
                        // Track it for later split query processing
                        _detectedCollections.Add(navProperty);
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

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}