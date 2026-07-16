using System;
using System.Collections.Generic;
using System.Collections.Frozen;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Internal;

#nullable enable
namespace nORM.Query
{
    internal sealed partial class ExpressionToSqlVisitor
    {
        /// <summary>
        /// Declaring types whose methods can be translated to SQL. Frozen at startup
        /// to avoid per-call allocation and to enable O(1) lookup.
        /// </summary>
        private static readonly FrozenSet<Type> s_safeDeclaringTypes = new HashSet<Type>
        {
            typeof(string), typeof(Math), typeof(DateTime), typeof(Convert),
            typeof(Enumerable), typeof(Queryable), typeof(Json),
            typeof(NormFunctions),
            // IEEE 754 predicates (IsNaN, IsInfinity, IsFinite, IsNegativeInfinity,
            // IsPositiveInfinity) live as statics on double/float. Admitting the
            // declaring types lets the generic TranslateFunction routing pick
            // them up via SqliteProvider's typeof(double)/typeof(float) switch.
            // typeof(decimal) admits decimal.Round overloads (dispatched via
            // TranslateMethodCall) plus future decimal-static translations.
            typeof(double), typeof(float), typeof(decimal),
            // TimeSpan.Compare / TimeSpan.FromHours etc are statics; admit so
            // the provider's typeof(TimeSpan) switch (TimeSpan.Compare landed
            // in 9ae9dab) reaches the WHERE path. DateTimeOffset already
            // routes via typeof(DateTime)|typeof(DateTimeOffset) in the
            // provider switch but the analyzer needs the type admitted to
            // get past the IsTranslatableMethod gate.
            typeof(TimeSpan), typeof(DateTimeOffset),
            // Admit DateOnly/TimeOnly so their statics (FromDateTime,
            // FromDayNumber, ParseExact, IsBetween, etc.) reach the WHERE
            // path. The provider switch already has the emits.
            typeof(DateOnly), typeof(TimeOnly), typeof(Guid)
        }.ToFrozenSet();

        /// <summary>
        /// Replaces the `NormQueryable.Query&lt;T&gt;(ctxConstant)` MethodCall at the
        /// root of a sub-expression's SPINE (the Arguments[0] chain) with a
        /// `ConstantExpression(IQueryable&lt;T&gt;)` so the QueryTranslator recognizes
        /// it as the query source. Deliberately NOT recursive into lambda bodies:
        /// a Query root nested inside a predicate belongs to its own subquery
        /// builder, which must first reserve an alignment slot for the consumed
        /// ctx capture — erasing it here would skip that reservation while the
        /// ParameterValueExtractor still walks the original member, shifting
        /// every later positional binding.
        /// </summary>
        internal static class QueryCallMaterializer
        {
            public static Expression Materialize(Expression e)
            {
                if (e is MethodCallExpression mce)
                {
                    if (mce.Method.DeclaringType == typeof(NormQueryable) && mce.Method.Name == nameof(NormQueryable.Query))
                    {
                        var compiled = Expression.Lambda(mce).Compile();
                        var queryable = compiled.DynamicInvoke();
                        return Expression.Constant(queryable, mce.Type);
                    }
                    if (mce.Arguments.Count > 0)
                    {
                        var newSource = Materialize(mce.Arguments[0]);
                        if (!ReferenceEquals(newSource, mce.Arguments[0]))
                        {
                            var args = mce.Arguments.ToArray();
                            args[0] = newSource;
                            return mce.Update(mce.Object, args);
                        }
                    }
                }
                return e;
            }
        }

        private static bool IsDateTimeLike(Type t)
        {
            var underlying = Nullable.GetUnderlyingType(t) ?? t;
            return underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset);
        }

        private static bool IsTimeOnly(Type t)
            => (Nullable.GetUnderlyingType(t) ?? t) == typeof(TimeOnly);

        /// <summary>
        /// Emits the SQL for a TimeSpan member access on a `(end - start)` subtraction.
        /// Returns true when the member name maps to a unit conversion; false otherwise
        /// (the caller falls through to the normal member-resolution path).
        /// When <paramref name="useTimeOnly"/> is true the wrapped TimeOnly-diff hook
        /// is used so the result stays in [0, 24h) matching .NET's TimeOnly subtraction.
        /// </summary>
        private bool TryEmitTimeSpanMember(string memberName, string endSql, string startSql, bool useTimeOnly = false)
        {
            var secondsSql = useTimeOnly
                ? _provider.GetTimeOnlyDifferenceSecondsSql(endSql, startSql)
                : _provider.GetDateTimeDifferenceSecondsSql(endSql, startSql);
            return TryEmitTimeSpanMemberFromSeconds(memberName, secondsSql);
        }

        // Shared emit logic for both (end-start).MemberName and storedColumn.MemberName.
        // secondsSql is total fractional seconds from any source.
        // Total* return fractional values; Days/Hours/Minutes/Seconds match TimeSpan
        // integer-component semantics (truncate toward zero with modular wrap).
        private bool TryEmitTimeSpanMemberFromSeconds(string memberName, string secondsSql)
        {
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
                    _sql.Append(_provider.GetTruncateToIntSql($"({secondsSql} / 86400)"));
                    return true;
                case nameof(TimeSpan.Hours):
                    _sql.Append('(').Append(_provider.GetTruncateToIntSql($"({secondsSql} / 3600)")).Append(" % 24)");
                    return true;
                case nameof(TimeSpan.Minutes):
                    _sql.Append('(').Append(_provider.GetTruncateToIntSql($"({secondsSql} / 60)")).Append(" % 60)");
                    return true;
                case nameof(TimeSpan.Seconds):
                    _sql.Append('(').Append(_provider.GetTruncateToIntSql(secondsSql)).Append(" % 60)");
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
            // Instance Contains on a closure-captured collection (List<T>, HashSet<T>,
            // ICollection<T>, etc.) is rewritten to a SQL IN clause by the handler
            // around line 1318 -- but that handler only runs if we admit the call here.
            // Without this clause, `Where(i => list.Contains(i.Id))` throws "Method
            // 'Contains' cannot be translated" while the array equivalent
            // `Where(i => arr.Contains(i.Id))` works because it binds to
            // Enumerable.Contains (declaring type Enumerable is already in
            // s_safeDeclaringTypes). We require a single argument and a non-null
            // receiver whose type is a generic collection to keep the surface
            // narrow -- string.Contains has its own LIKE-based path and is reached
            // through the s_safeDeclaringTypes branch below.
            if (method.Name == nameof(List<int>.Contains)
                && method.GetParameters().Length == 1
                && method.DeclaringType is { } dt
                && dt != typeof(string)
                && IsTranslatableContainsReceiver(dt))
            {
                return true;
            }
            // Dictionary<K,V>.ContainsKey(k) -- treated as Keys.Contains(k) by the
            // handler around line 1318 once admitted here.
            // Dictionary<K,V>.ContainsValue(v) -- treated as Values.Contains(v) on the
            // same path; both walk a projected collection rather than the dictionary's
            // KeyValuePair enumeration.
            if ((method.Name == "ContainsKey" || method.Name == "ContainsValue")
                && method.GetParameters().Length == 1
                && method.DeclaringType is { } dictDt
                && IsDictionaryLikeReceiver(dictDt))
            {
                return true;
            }
            if (method.DeclaringType == null || !s_safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            // System.Convert.ToString is intentionally translatable as CAST(... AS TEXT);
            // it's only the default object.ToString that has no SQL equivalent.
            if (method.DeclaringType == typeof(Convert) && method.Name == nameof(Convert.ToString))
                return true;
            return !s_untranslatableMethods.Contains(method.Name);
        }

        private static bool IsTranslatableContainsReceiver(Type t)
        {
            if (typeof(System.Collections.IEnumerable).IsAssignableFrom(t))
                return true;
            // Generic interfaces (ICollection<T>, IList<T>, etc.) -- the runtime
            // erases the parameter so check the open form on the type's interfaces.
            foreach (var i in t.GetInterfaces())
            {
                if (i.IsGenericType)
                {
                    var def = i.GetGenericTypeDefinition();
                    if (def == typeof(ICollection<>) || def == typeof(IEnumerable<>))
                        return true;
                }
            }
            return false;
        }

        private static bool IsDictionaryLikeReceiver(Type t)
        {
            if (typeof(System.Collections.IDictionary).IsAssignableFrom(t))
                return true;
            foreach (var i in t.GetInterfaces())
            {
                if (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                    return true;
            }
            return false;
        }
        private static bool IsNonDeterministicServerMethod(MethodInfo method)
            => method.DeclaringType == typeof(Guid) && method.Name == nameof(Guid.NewGuid)
               || method.DeclaringType == typeof(NormFunctions)
                  && (method.Name == nameof(NormFunctions.ServerNewGuid)
                      || method.Name == nameof(NormFunctions.ServerUtcNow)
                      || method.Name == nameof(NormFunctions.ServerRandom));
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
            // Collection-receiver Contains (List<T>.Contains, HashSet<T>.Contains, etc.)
            // is rewritten to a SQL IN clause by the dedicated handler below; emitting a
            // CASE WHEN list IS NULL ... wrapper around it would try to bind the entire
            // CLR collection as a SQL parameter and fail. The Contains handler already
            // resolves the collection to a constant in the host process before walking
            // its items, so the null-check is unnecessary anyway.
            if (node.Method.Name == nameof(List<int>.Contains)
                && node.Method.GetParameters().Length == 1
                && node.Method.DeclaringType is { } dt
                && dt != typeof(string)
                && IsTranslatableContainsReceiver(dt))
            {
                return false;
            }
            if ((node.Method.Name == "ContainsKey" || node.Method.Name == "ContainsValue")
                && node.Method.GetParameters().Length == 1
                && node.Method.DeclaringType is { } dictDt
                && IsDictionaryLikeReceiver(dictDt))
            {
                return false;
            }
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
            // Contains("")/StartsWith("")/EndsWith("") are always true in C#, so an
            // empty needle must produce the match-everything pattern. The old
            // empty-string return emitted `LIKE ''`, which matches ONLY empty values
            // and silently dropped every other row.
            if (string.IsNullOrEmpty(value)) return "%";

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
        // True when the expression resolves to a column reference (member of the
        // lambda parameter), not a constant / closure / parameter. Used by the
        // DateTime-comparison normalization to wrap only column operands.
        private bool IsColumnReference(Expression expr)
        {
            // Strip Convert wrappers (e.g. (DateTime?)col).
            while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
                expr = u.Operand;
            if (expr is MemberExpression me && me.Expression is ParameterExpression)
                return true;
            // Joined-table member access via correlated parameter is also a column.
            if (expr is MemberExpression me2 && me2.Expression is ParameterExpression pe2 && _parameterMappings.ContainsKey(pe2))
                return true;
            return false;
        }

        private string GetSql(Expression expression)
        {
            var start = _sql.Length;
            Visit(expression);
            var segment = _sql.ToString(start, _sql.Length - start);
            _sql.Remove(start, _sql.Length - start);
            return segment;
        }

        private string GetStringToBoolSql(string innerSql)
            => $"(LOWER(LTRIM(RTRIM({_provider.GetToStringSql(innerSql)}))) = 'true')";

        private static long ToUnixTimeMicroseconds(DateTimeOffset value)
            => (value.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10;

        // dtoCol [==/!=] dateTimeLiteral — see Bxxxx commit message: SQLite text
        // storage of DTOs needs UTC-instant comparison, not byte equality of
        // canonical text. Unspecified / Local DateTime literals use the local offset
        // for that literal wall-clock instant.
        private bool TryEmitDateTimeOffsetLiteralComparison(BinaryExpression node)
        {
            if (node.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual
                or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
                or ExpressionType.LessThan or ExpressionType.LessThanOrEqual))
                return false;

            var leftType = Nullable.GetUnderlyingType(node.Left.Type) ?? node.Left.Type;
            var rightType = Nullable.GetUnderlyingType(node.Right.Type) ?? node.Right.Type;

            Expression colSide; Expression litSide;
            bool columnOnLeft;
            if (leftType == typeof(DateTimeOffset) && IsColumnReference(node.Left))
            {
                colSide = node.Left;
                litSide = node.Right;
                columnOnLeft = true;
            }
            else if (rightType == typeof(DateTimeOffset) && IsColumnReference(node.Right))
            {
                colSide = node.Right;
                litSide = node.Left;
                columnOnLeft = false;
            }
            else return false;

            // Peel any Convert wrapping the literal (implicit DateTime→DTO).
            var peeled = litSide;
            while (peeled is UnaryExpression u2 && (u2.NodeType == ExpressionType.Convert || u2.NodeType == ExpressionType.ConvertChecked))
                peeled = u2.Operand;

            if (!TryGetConstantValue(peeled, out var raw) || raw == null)
                return false;

            DateTimeOffset utcInstant;
            if (raw is DateTime dt)
            {
                if (dt.Kind == DateTimeKind.Utc)
                    utcInstant = new DateTimeOffset(dt, TimeSpan.Zero);
                else
                {
                    var localOffset = TimeZoneInfo.Local.GetUtcOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified));
                    utcInstant = new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), localOffset);
                }
            }
            else if (raw is DateTimeOffset dto) utcInstant = dto;
            else return false;

            var epochUs = ToUnixTimeMicroseconds(utcInstant);
            var colSql = GetSql(colSide);
            var epochSql = _provider.GetDateTimeOffsetUtcEpochMicrosecondsSql(colSql);
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _sql.Append("(");
            if (columnOnLeft)
            {
                _sql.Append(epochSql).Append(ComparisonSql(node.NodeType));
                _sql.AppendParameterizedValue(paramName, epochUs, _paramSink);
            }
            else
            {
                _sql.AppendParameterizedValue(paramName, epochUs, _paramSink);
                _sql.Append(ComparisonSql(node.NodeType)).Append(epochSql);
            }
            _sql.Append(")");
            return true;
        }

        private static string ComparisonSql(ExpressionType nodeType) => nodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " <> ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            _ => throw new InvalidOperationException($"Unsupported comparison node '{nodeType}'.")
        };
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

        // The PATTERN (Arguments[0]) precedes the StringComparison (Arguments[1]) in
        // the extractor's expression-order walk, and a closure pattern now emits a
        // REAL compiled parameter inside EmitLikePredicate — so the pattern's slot
        // must be created FIRST or the needle value binds to the comparison's
        // placeholder position and the predicate silently matches nothing.
        private static void HandleStringContainsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.Contains, IsIgnoreCase(node.Arguments[1]));
            ReserveCompiledParamSlotIfClosure(visitor, node.Arguments[1]);
        }

        private static void HandleStringStartsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.StartsWith, IsIgnoreCase(node.Arguments[1]));
            ReserveCompiledParamSlotIfClosure(visitor, node.Arguments[1]);
        }

        private static void HandleStringEndsWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            EmitLikePredicate(visitor, node.Object!, node.Arguments[0], LikeOperation.EndsWith, IsIgnoreCase(node.Arguments[1]));
            ReserveCompiledParamSlotIfClosure(visitor, node.Arguments[1]);
        }

        // As with the LIKE handlers above: the value operands precede the
        // StringComparison in the extractor's expression-order walk, so their
        // compiled parameters must be created BEFORE the comparison's placeholder
        // or the values bind to the wrong slots.
        private static void HandleStringEqualsInstanceWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            EmitEqualityPredicate(visitor, node.Object!, node.Arguments[0], IsIgnoreCase(node.Arguments[1]));
            ReserveCompiledParamSlotIfClosure(visitor, node.Arguments[1]);
        }

        private static void HandleStringEqualsStaticWithComparison(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            EmitEqualityPredicate(visitor, node.Arguments[0], node.Arguments[1], IsIgnoreCase(node.Arguments[2]));
            ReserveCompiledParamSlotIfClosure(visitor, node.Arguments[2]);
        }

        // ParameterValueExtractor walks every closure MemberExpression in the
        // predicate; when a handler folds a closure-captured arg inline without
        // reserving a compiled-param slot, the value-list shifts and downstream
        // @cp bindings get the wrong values. Reserve a placeholder when the arg
        // is a closure capture. Same fix shape as 407e03d / eeff6e7 / cf39b61 /
        // 04a0003 / 7d6d7ac.
        private static void ReserveCompiledParamSlotIfClosure(ExpressionToSqlVisitor visitor, Expression arg)
        {
            if (arg is not MemberExpression) return;
            var placeholder = $"{visitor._provider.ParamPrefix}cp{visitor._compiledParams.Count}_unused";
            visitor._params[placeholder] = DBNull.Value;
            visitor._compiledParams.Add(placeholder);
        }

        private static void EmitLikePredicate(
            ExpressionToSqlVisitor visitor,
            Expression target,
            Expression patternExpr,
            LikeOperation op,
            bool ignoreCase)
        {
            var lhs = visitor.GetSql(target);
            if (ignoreCase)
            {
                // Case-insensitive: fold both sides to lower-case and use a plain LIKE. Correct on
                // every provider (a case-insensitive LIKE over already-lowered text stays
                // case-insensitive), so no binary-collation forcing here.
                lhs = $"LOWER({lhs})";
            }
            else
            {
                // Ordinal (case-sensitive) match, matching .NET's default string.Contains/StartsWith/
                // EndsWith. SQLite's LIKE folds ASCII case irrespective of collation, so bypass LIKE
                // entirely to a byte-exact instr/substr construct. The other providers reuse
                // ForceCaseSensitiveStringComparison — the same hook string equality uses — to make
                // the LIKE operand binary (MySQL `BINARY col`, SQL Server `COLLATE ..._BIN2`;
                // PostgreSQL's LIKE is already case-sensitive so its identity default is correct).
                if (visitor._provider.UsesOrdinalStringMatchBypass)
                {
                    var patternSql = visitor.GetSql(patternExpr);
                    var kind = op switch
                    {
                        LikeOperation.StartsWith => nORM.Providers.OrdinalStringMatch.StartsWith,
                        LikeOperation.EndsWith => nORM.Providers.OrdinalStringMatch.EndsWith,
                        _ => nORM.Providers.OrdinalStringMatch.Contains
                    };
                    visitor._sql.Append(visitor._provider.GetOrdinalStringMatchSql(lhs, patternSql, kind));
                    return;
                }
                lhs = visitor._provider.ForceCaseSensitiveStringComparison(lhs);
            }
            visitor._sql.Append(lhs).Append(" LIKE ");
            var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
            // Pre-fold ONLY compile-time literals into the pattern. A closure-captured
            // needle must go through the variable branch below: baking its CURRENT value
            // into the cached plan made every later execution replay the FIRST needle
            // (the reserved @cp slot was a DBNull placeholder, so Contains/StartsWith/
            // EndsWith with a different captured string silently matched — and bulk
            // ExecuteUpdate/Delete wrote or deleted — the wrong rows on every LIKE-path
            // provider; SQLite escaped only because its ordinal bypass parameterizes).
            if (patternExpr is ConstantExpression && TryGetConstantValue(patternExpr, out var raw)
                && (raw is string || raw is char))
            {
                // Pre-folded constant: bind the lowered pattern when ignoring case so the SQL
                // doesn't need to wrap it again at run time. A char needle folds as its
                // one-character string.
                var s = raw as string ?? ((char)raw!).ToString();
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
                visitor._sql.Append('(').Append(lhs).Append(" = ").Append(rhs).Append(')');
                return;
            }
            // Ordinal comparison (string.Equals default / StringComparison.Ordinal): providers
            // whose default collation folds case need the sargable ordinal wrap.
            if (visitor._provider.DefaultStringEqualityIsCaseInsensitive)
            {
                visitor._sql.Append(visitor._provider.OrdinalStringEqualSql(lhs, rhs));
                return;
            }
            visitor._sql.Append('(').Append(lhs).Append(" = ").Append(rhs).Append(')');
        }
        // ContainsTranslator, StartsWithTranslator, and EndsWithTranslator were consolidated into
        // _fastMethodHandlers. String methods are exclusively handled there.
    }
}
