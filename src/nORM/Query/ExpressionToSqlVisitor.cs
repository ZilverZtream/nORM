using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using System.Text;
using Microsoft.Extensions.ObjectPool;

#nullable enable

namespace nORM.Query
{
    internal sealed class ExpressionToSqlVisitor : ExpressionVisitor
    {
        private DbContext _ctx = null!;
        private TableMapping _mapping = null!;
        private DatabaseProvider _provider = null!;
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _parameterMappings = new();
        private ParameterExpression _parameter = null!;
        private string _tableAlias = string.Empty;
        private OptimizedSqlBuilder _sql = null!;
        private readonly Dictionary<string, object> _params = new();
        private int _paramIndex = 0;
        private readonly List<string> _ownedCompiledParams = new();
        private readonly Dictionary<ParameterExpression, string> _ownedParamMap = new();
        private List<string> _compiledParams = null!;
        private Dictionary<ParameterExpression, string> _paramMap = null!;
        private bool _suppressNullCheck = false;

        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        internal ExpressionToSqlVisitor() { }

        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                                      List<string>? compiledParams = null,
                                      Dictionary<ParameterExpression, string>? paramMap = null)
        {
            Initialize(ctx, mapping, provider, parameter, tableAlias, correlated, compiledParams, paramMap);
        }

        public void Initialize(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                               ParameterExpression parameter, string tableAlias,
                               Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                               List<string>? compiledParams = null,
                               Dictionary<ParameterExpression, string>? paramMap = null)
        {
            _ctx = ctx;
            _mapping = mapping;
            _provider = provider;
            _parameter = parameter;
            _tableAlias = tableAlias;

            _parameterMappings.Clear();
            if (correlated != null)
            {
                foreach (var kvp in correlated)
                    _parameterMappings[kvp.Key] = kvp.Value;
            }
            _parameterMappings[parameter] = (mapping, tableAlias);

            _compiledParams = compiledParams ?? _ownedCompiledParams;
            if (compiledParams == null)
                _ownedCompiledParams.Clear();

            _paramMap = paramMap ?? _ownedParamMap;
            if (paramMap == null)
                _ownedParamMap.Clear();

            _paramIndex = 0;
            _suppressNullCheck = false;
        }

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
        }

        public string Translate(Expression expression)
        {
            var sb = _stringBuilderPool.Get();
            try
            {
                using var builder = new OptimizedSqlBuilder(sb);
                _sql = builder;
                Visit(expression);
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
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
                var column = info.Mapping.Columns.FirstOrDefault(c => c.Prop.Name == node.Member.Name);
                if (column != null)
                {
                    // Table aliases are generated internally (e.g. T0, T1) and are not influenced
                    // by user input, so escaping them adds unnecessary quoting that breaks
                    // expected SQL output. Use the alias directly while keeping column names
                    // escaped to avoid injection.
                    _sql.Append($"{info.Alias}.{column.EscCol}");
                    return node;
                }
            }

            if (TryGetConstantValue(node, out var value))
            {
                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                _sql.AppendParameterizedValue(paramName, value, _params);
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
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _sql.AppendParameterizedValue(paramName, node.Value, _params);
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
                    var jsonSql = _provider.TranslateJsonPathAccess(columnSql, jsonPath);
                    _sql.Append(jsonSql);
                    return node;
                }
                else
                {
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "JSONPath argument in Json.Value must be a constant string."));
                }
            }

            if (node.Method.DeclaringType == typeof(string))
            {
                switch (node.Method.Name)
                {
                      case "Contains":
                          Visit(node.Object);
                          _sql.Append(" LIKE ");
                          if (TryGetConstantValue(node.Arguments[0], out var contains) && contains is string cs)
                          {
                              var containsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                              _sql.AppendParameterizedValue(containsParam, CreateSafeLikePattern(cs, LikeOperation.Contains), _params)
                                  .Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                          }
                          else
                          {
                              throw new NotSupportedException("Only constant values are supported in Contains().");
                          }
                          return node;
                      case "StartsWith":
                          Visit(node.Object);
                          _sql.Append(" LIKE ");
                          if (TryGetConstantValue(node.Arguments[0], out var starts) && starts is string ss)
                          {
                              var startsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                              _sql.AppendParameterizedValue(startsParam, CreateSafeLikePattern(ss, LikeOperation.StartsWith), _params)
                                  .Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                          }
                          else
                          {
                              throw new NotSupportedException("Only constant values are supported in StartsWith().");
                          }
                          return node;
                      case "EndsWith":
                          Visit(node.Object);
                          _sql.Append(" LIKE ");
                          if (TryGetConstantValue(node.Arguments[0], out var ends) && ends is string es)
                          {
                              var endsParam = $"{_provider.ParamPrefix}p{_paramIndex++}";
                              _sql.AppendParameterizedValue(endsParam, CreateSafeLikePattern(es, LikeOperation.EndsWith), _params)
                                  .Append($" ESCAPE '{_provider.LikeEscapeChar}'");
                          }
                          else
                          {
                              throw new NotSupportedException("Only constant values are supported in EndsWith().");
                          }
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

                throw new NotSupportedException($"String method '{node.Method.Name}' not supported.");
            }
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1 && node.Arguments[0] is ParameterExpression cp && _parameterMappings.ContainsKey(cp))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                var info = _parameterMappings[cp];
                                var visitor = ExpressionVisitorPool.Get(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
                                var predSql = visitor.Translate(countSelector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _sql.Append($"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)");
                                ExpressionVisitorPool.Return(visitor);
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
                                var visitor = ExpressionVisitorPool.Get(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap);
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
                                ExpressionVisitorPool.Return(visitor);
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

                    var exceedsLimit = _provider.MaxParameters != int.MaxValue && (_paramIndex + items.Count) > _provider.MaxParameters;

                    Visit(valueExpr);
                    _sql.Append(" IN (");
                    for (int i = 0; i < items.Count; i++)
                    {
                        if (i > 0) _sql.Append(", ");
                        if (exceedsLimit)
                        {
                            AppendSqlLiteral(items[i]);
                        }
                        else
                        {
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _sql.AppendParameterizedValue(paramName, items[i], _params);
                        }
                    }
                    _sql.Append(")");
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

            using var subTranslator = new QueryTranslator(_ctx, mapping, _params, ref _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);

            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }

        private void BuildIn(Expression source, Expression value)
        {
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);

            using var subTranslator = new QueryTranslator(_ctx, mapping, _params, ref _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count);
            var subPlan = subTranslator.Translate(source);

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

        private Expression CreateSafeParameter(object? value)
        {
            if (value is string str && str.Length > 8000)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "String parameter exceeds maximum length"));

            if (value is byte[] bytes && bytes.Length > 8000)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "Binary parameter exceeds maximum length"));

            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _sql.AppendParameterizedValue(paramName, value, _params);
            return Expression.Constant(value);
        }

        private enum LikeOperation
        {
            Contains,
            StartsWith,
            EndsWith
        }

        private string CreateSafeLikePattern(string value, LikeOperation operation)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;

            // Validate and double-escape
            var escaped = value
                .Replace("\\", "\\\\")  // Escape backslashes first
                .Replace("%", "\\%")
                .Replace("_", "\\_")
                .Replace("[", "\\[");

            return operation switch
            {
                LikeOperation.Contains => $"%{escaped}%",
                LikeOperation.StartsWith => $"{escaped}%",
                LikeOperation.EndsWith => $"%{escaped}",
                _ => escaped
            };
        }

        private void AppendSqlLiteral(object? value)
        {
            if (value == null || value == DBNull.Value)
            {
                _sql.Append("NULL");
                return;
            }

            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Boolean:
                    _sql.Append((bool)value ? "1" : "0");
                    return;
                case TypeCode.String:
                    var s = (string)value;
                    _sql.Append('\'').Append(s.Replace("'", "''")).Append('\'');
                    return;
                case TypeCode.Char:
                    var c = (char)value;
                    _sql.Append('\'').Append(c == '\'' ? "''" : c.ToString()).Append('\'');
                    return;
                case TypeCode.DateTime:
                    var dt = (DateTime)value;
                    _sql.Append('\'').Append(dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)).Append('\'');
                    return;
            }

            if (value is DateTimeOffset dto)
            {
                _sql.Append('\'').Append(dto.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)).Append('\'');
            }
            else if (value is Guid guid)
            {
                _sql.Append('\'').Append(guid.ToString()).Append('\'');
            }
            else if (value is byte[] bytes)
            {
                _sql.Append("0x").Append(Convert.ToHexString(bytes));
            }
            else if (value.GetType().IsEnum)
            {
                var enumVal = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                _sql.Append(enumVal.ToString(CultureInfo.InvariantCulture));
            }
            else if (value is IFormattable formattable)
            {
                _sql.Append(formattable.ToString(null, CultureInfo.InvariantCulture));
            }
            else
            {
                _sql.Append(value.ToString());
            }
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


        public Dictionary<string, object> GetParameters() => _params;

        private string GetSql(Expression expression)
        {
            var start = _sql.Length;
            Visit(expression);
            var segment = _sql.ToString(start, _sql.Length - start);
            _sql.Remove(start, _sql.Length - start);
            return segment;
        }

        private static bool TryGetConstantValue(Expression e, out object? value)
        {
            switch (e)
            {
                case ConstantExpression ce:
                    value = ce.Value;
                    return true;
                case MemberExpression me:
                    if (me.Expression != null && TryGetConstantValue(me.Expression, out var obj))
                    {
                        value = me.Member switch
                        {
                            FieldInfo fi => fi.GetValue(obj),
                            PropertyInfo pi => pi.GetValue(obj),
                            _ => null
                        };
                        return true;
                    }
                    break;
                case MethodCallExpression mce:
                    object? instance = null;
                    if (mce.Object != null && !TryGetConstantValue(mce.Object, out instance))
                    {
                        value = null;
                        return false;
                    }

                    var args = new object?[mce.Arguments.Count];
                    for (int i = 0; i < mce.Arguments.Count; i++)
                    {
                        if (!TryGetConstantValue(mce.Arguments[i], out var argVal))
                        {
                            value = null;
                            return false;
                        }
                        args[i] = argVal;
                    }

                    value = mce.Method.Invoke(instance, args);
                    return true;
            }

            value = null;
            return false;
        }

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

    }
}