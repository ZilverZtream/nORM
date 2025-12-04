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
        private readonly List<string> _ownedCompiledParams = new();
        private readonly Dictionary<ParameterExpression, string> _ownedParamMap = new();
        private List<string> _compiledParams = null!;
        private Dictionary<ParameterExpression, string> _paramMap = null!;
        private bool _suppressNullCheck = false;
        private const int _constParamMapLimit = 1024;
        private readonly Dictionary<ConstKey, string> _constParamMap = new();
        private readonly Dictionary<(ParameterExpression Param, string Member), string> _memberParamMap = new();
        private static readonly Expression _emptyExpression = Expression.Empty();
        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();
        // REFACTOR (TASK 16): Consolidated string method translation. Removed redundant _translators dictionary.
        // All string methods (Contains, StartsWith, EndsWith) are now handled via _fastMethodHandlers for better performance.
        private static readonly Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> _fastMethodHandlers =
            new()
            {
                { typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!, HandleStringContains },
                { typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) })!, HandleStringStartsWith },
                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };
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
            _paramIndex = 0;
            _suppressNullCheck = false;
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
            Visit(expression);
            return builder.ToSqlString();
        }
        protected override Expression VisitBinary(BinaryExpression node)
        {
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
                _ => throw new NotSupportedException($"Binary operator '{node.NodeType}' not supported.")
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }
        protected override Expression VisitMember(MemberExpression node)
        {
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
                AppendConstant(value, node.Type);
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
            throw new NotSupportedException($"Member '{node.Member.Name}' is not supported in this context.");
        }
        protected override Expression VisitConstant(ConstantExpression node)
        {
            AppendConstant(node.Value, node.Type);
            return node;
        }
        protected override Expression VisitParameter(ParameterExpression node)
        {
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
                _sql.Append("(NOT(");
                Visit(node.Operand);
                _sql.Append("))");
                return node;
            }
            return base.VisitUnary(node);
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // ADD FAST PATH FOR COMMON METHODS
            if (_fastMethodHandlers.TryGetValue(node.Method, out var handler))
            {
                handler(this, node);
                return node;
            }
            if (!IsTranslatableMethod(node.Method))
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

                    // Basic validation: ensure path doesn't contain suspicious SQL characters
                    // JSON paths should only contain alphanumeric, dots, brackets, underscores, and dollar signs
                    if (jsonPath.IndexOfAny(new[] { '\'', '"', ';', '-', '/', '*', '\\' }) >= 0)
                    {
                        throw new NormQueryException(
                            $"JSON path '{jsonPath}' contains invalid characters. " +
                            "JSON paths should only contain alphanumeric characters, dots, brackets, underscores, and dollar signs.");
                    }

                    // Limit path length to prevent potential DoS
                    if (jsonPath.Length > 500)
                    {
                        throw new NormQueryException(
                            $"JSON path exceeds maximum length of 500 characters (actual: {jsonPath.Length}).");
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
            // REFACTOR (TASK 16): Removed redundant _translators check. String methods handled by _fastMethodHandlers.
            if (node.Method.DeclaringType == typeof(string))
            {
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
                throw new NotSupportedException($"String method '{node.Method.Name}' not supported.");
            }
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.GroupBy):
                        HandleGroupByMethod(node);
                        return node;
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1 && node.Arguments[0] is ParameterExpression cp && _parameterMappings.ContainsKey(cp))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                var info = _parameterMappings[cp];
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
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
                        break;
                    case "Sum":
                    case "Average":
                    case "Min":
                    case "Max":
                        if (node.Arguments.Count >= 2 && node.Arguments[0] is ParameterExpression gp && _parameterMappings.ContainsKey(gp))
                        {
                            var selector = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (selector != null)
                            {
                                var info = _parameterMappings[gp];
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
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
                        _sql.Append("(1=0)");
                        return node;
                    }
                    // PARAMETER LIMIT FIX: Use percentage-based buffer instead of fixed value
                    // Reserve 20% of max parameters for other query parts (joins, WHERE, projections, etc.)
                    // This is more robust than a fixed buffer of 10 which doesn't scale with query complexity
                    var reservedBuffer = Math.Max(20, (int)(_provider.MaxParameters * 0.2));
                    var remainingParams = _provider.MaxParameters - _paramIndex - reservedBuffer;

                    if (remainingParams <= 0)
                        throw new NormQueryException(
                            $"Query complexity exceeds database parameter limit. " +
                            $"Current parameters: {_paramIndex}, Maximum: {_provider.MaxParameters}, Reserved: {reservedBuffer}. " +
                            $"Consider simplifying the query or using fewer items in IN clauses.");

                    if (items.Count > remainingParams)
                        throw new NormQueryException(
                            $"IN clause with {items.Count} items exceeds remaining parameter budget of {remainingParams}. " +
                            $"Current parameters: {_paramIndex}, Maximum: {_provider.MaxParameters}. " +
                            $"Consider using a temporary table or reducing the number of items.");
                    var maxBatchSize = Math.Max(1, Math.Min(1000, remainingParams));
                    if (items.Count > maxBatchSize)
                    {
                        _sql.Append("(");
                        for (int batch = 0; batch < items.Count; batch += maxBatchSize)
                        {
                            if (batch > 0) _sql.Append(" OR ");
                            var batchItems = items.Skip(batch).Take(maxBatchSize);
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
                    }
                    else
                    {
                        Visit(valueExpr);
                        _sql.Append(" IN (");
                        for (int i = 0; i < items.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _sql.AppendParameterizedValue(paramName, items[i], _paramSink);
                        }
                        _sql.Append(")");
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
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new ArgumentException("All requires a predicate");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate: true);
                        return node;
                    case nameof(Queryable.Contains):
                        BuildIn(node.Arguments[0], node.Arguments[1]);
                        return node;
                    default:
                        throw new NotSupportedException($"Queryable method '{node.Method.Name}' not supported.");
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
            throw new NotSupportedException($"Method '{node.Method.Name}' not supported.");
        }
        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, _params, _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }
        private void BuildIn(Expression source, Expression value)
        {
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, _params, _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            Visit(value);
            _sql.Append(" IN (");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }
        private bool IsTranslatableMethod(MethodInfo method)
        {
            if (method.GetCustomAttribute<SqlFunctionAttribute>() != null)
                return true;
            var safeDeclaringTypes = new HashSet<Type>
            {
                typeof(string), typeof(Math), typeof(DateTime), typeof(Convert), typeof(Enumerable), typeof(Queryable), typeof(Json)
            };
            if (method.DeclaringType == null || !safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            var dangerousMethods = new HashSet<string>
            {
                "GetType", "ToString", "GetHashCode"
            };
            return !dangerousMethods.Contains(method.Name);
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
        private bool TryGetConstantValueSafe(Expression expr, out object? value, int maxDepth = 5)
        {
            if (maxDepth <= 0)
            {
                value = null;
                return false;
            }
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
            if (_constParamMap.Count >= _constParamMapLimit)
                _constParamMap.Clear();
            _constParamMap[key] = paramName;
        }
        private Expression CreateSafeParameter(object? value)
        {
            if (value is string str && str.Length > 8000)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "String parameter exceeds maximum length"));
            if (value is byte[] bytes && bytes.Length > 8000)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Binary parameter exceeds maximum length"));
            AppendConstant(value, value?.GetType() ?? typeof(object));
            // Returning a cached empty expression avoids allocating a new
            // Expression instance for each constant value translated. The
            // actual value has already been written directly to the
            // parameter collection in AppendConstant, so no further
            // expression tree representation is required here.
            return _emptyExpression;
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
                if (mce.Method.Name == "Query" && mce.Arguments.Count == 0)
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
        /// REFACTOR (TASK 19): Use shared ExpressionValueExtractor utility.
        /// Eliminates duplicate logic and ensures consistent behavior across the codebase.
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
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var contains) && contains is string cs)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(cs, LikeOperation.Contains), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor._sql.Append("CONCAT('%', ");
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(escapedSql);
                visitor._sql.Append(", '%')");
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        private static void HandleStringStartsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var starts) && starts is string ss)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(ss, LikeOperation.StartsWith), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor._sql.Append("CONCAT(");
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(escapedSql);
                visitor._sql.Append(", '%')");
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        private static void HandleStringEndsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var ends) && ends is string es)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(es, LikeOperation.EndsWith), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor._sql.Append("CONCAT('%', ");
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(escapedSql);
                visitor._sql.Append(")");
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        // REFACTOR (TASK 16): Removed redundant IMethodTranslator interface and translator classes.
        // ContainsTranslator, StartsWithTranslator, and EndsWithTranslator were duplicate logic.
        // String methods are now exclusively handled by _fastMethodHandlers for better performance.

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

        public void RegisterGroupingKey(ParameterExpression parameter, string keySql)
        {
            _groupingKeys[parameter] = keySql;
        }

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
            if (!ReferenceEquals(_paramSink, _params))
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
