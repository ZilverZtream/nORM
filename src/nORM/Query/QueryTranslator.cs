using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator : ExpressionVisitor
    {
        private readonly DbContext _ctx;
        private readonly SqlClauseBuilder _clauses = new();
        private readonly Dictionary<string, object> _params = new();
        private readonly MaterializerFactory _materializerFactory = new();
        private TableMapping _mapping = null!;
        private Type? _rootType;
        private int _paramIndex = 0;
        private readonly List<string> _compiledParams;
        private readonly Dictionary<ParameterExpression, string> _paramMap;
        private readonly List<IncludePlan> _includes = new();
        private LambdaExpression? _projection;
        private bool _isAggregate = false;
        private string _methodName = "";
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _correlatedParams;
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value - used in complex join scenarios
        private GroupJoinInfo? _groupJoinInfo;
#pragma warning restore CS0649
        private int _joinCounter = 0;
        private DatabaseProvider _provider;
        private bool _singleResult = false;
        private bool _noTracking = false;
        private bool _splitQuery = false;
        private readonly HashSet<string> _tables;

        private StringBuilder _sql => _clauses.Sql;
        private StringBuilder _where => _clauses.Where;
        private StringBuilder _having => _clauses.Having;
        private List<(string col, bool asc)> _orderBy => _clauses.OrderBy;
        private List<string> _groupBy => _clauses.GroupBy;
        private int? _take { get => _clauses.Take; set => _clauses.Take = value; }
        private int? _skip { get => _clauses.Skip; set => _clauses.Skip = value; }
        private string? _takeParam { get => _clauses.TakeParam; set => _clauses.TakeParam = value; }
        private string? _skipParam { get => _clauses.SkipParam; set => _clauses.SkipParam = value; }
        private bool _isDistinct { get => _clauses.IsDistinct; set => _clauses.IsDistinct = value; }

        // Initialize _groupJoinInfo in constructor to suppress warning
        // This field is used in complex join scenarios

        public QueryTranslator(DbContext ctx)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = null!;
            _rootType = null;
            _correlatedParams = new();
            _compiledParams = new();
            _paramMap = new();
            _tables = new HashSet<string>();
        }

        internal QueryTranslator(
            DbContext ctx,
            TableMapping mapping,
            Dictionary<string, object> parameters,
            ref int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = mapping;
            _rootType = mapping.Type;
            _params = parameters;
            _paramIndex = pIndex;
            _correlatedParams = correlated;
            _tables = tables;
            _compiledParams = compiledParams;
            _paramMap = paramMap;
            _tables.Add(mapping.TableName);
            _joinCounter = joinStart;
        }

        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
            => _materializerFactory.CreateMaterializer(mapping, targetType, projection);

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

            throw new NormQueryTranslationException($"Cannot determine element type from expression of type {type}");
        }

        public QueryPlan Translate(Expression e)
        {
            // Determine root query type and handle TPH discriminator filters
            _rootType = GetElementType(e);
            _mapping = TrackMapping(_rootType);

            // Walk up inheritance hierarchy to find base mapping with discriminator
            var baseType = _rootType.BaseType;
            while (baseType != null && baseType != typeof(object))
            {
                var baseMap = TrackMapping(baseType);
                if (baseMap.DiscriminatorColumn != null)
                {
                    _mapping = baseMap;
                    var discAttr = _rootType.GetCustomAttribute<DiscriminatorValueAttribute>();
                    if (discAttr != null)
                    {
                        var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                        _params[paramName] = discAttr.Value;
                        _where.Append($"({_mapping.DiscriminatorColumn!.EscCol} = {paramName})");
                    }
                    break;
                }
                baseType = baseType.BaseType;
            }

            Visit(e);

            var materializerType = _projection?.Body.Type ?? _rootType ?? _mapping.Type;
            if (_isAggregate && _groupBy.Count == 0 && (e as MethodCallExpression)?.Method.Name is "Count" or "LongCount")
            {
                materializerType = typeof(int);
            }

            var materializer = _materializerFactory.CreateMaterializer(_mapping, materializerType, _projection);
            var isScalar = _isAggregate && _groupBy.Count == 0;

            if (_sql.Length == 0)
            {
                if (_isAggregate && _groupBy.Count == 0)
                {
                    _sql.Append($"SELECT COUNT(*) FROM {_mapping.EscTable}");
                }
                else
                {
                    string select;
                    if (_projection != null)
                    {
                        var selectVisitor = new SelectClauseVisitor(_mapping, _groupBy, _provider);
                        select = selectVisitor.Translate(_projection.Body);
                    }
                    else
                    {
                        select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                    }

                    var alias = _correlatedParams.Count > 0 ? _correlatedParams.Values.First().Alias : null;
                    var distinct = _isDistinct ? "DISTINCT " : string.Empty;
                    _sql.Insert(0, $"SELECT {distinct}{select} FROM {_mapping.EscTable}" + (alias != null ? $" {alias}" : string.Empty));
                }
            }

            if (_where.Length > 0) _sql.Append($" WHERE {_where}");

            if (_groupBy.Count > 0) _sql.Append(" GROUP BY " + string.Join(", ", _groupBy));
            if (_having.Length > 0) _sql.Append(" HAVING " + _having);
            if (_orderBy.Count > 0) _sql.Append(" ORDER BY " + string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}")));
            _ctx.Provider.ApplyPaging(_sql, _take, _skip, _takeParam, _skipParam);

            var singleResult = _singleResult || _methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;

            var elementType = _groupJoinInfo?.ResultType ?? materializerType;

            var plan = new QueryPlan(_sql.ToString(), _params, _compiledParams, materializer, elementType, isScalar, singleResult, _noTracking, _methodName, _includes, _groupJoinInfo, _tables.ToArray(), _splitQuery);
            QueryPlanValidator.Validate(plan, _provider);
            return plan;
        }

        private TableMapping TrackMapping(Type type)
        {
            var map = _ctx.GetMapping(type);
            _tables.Add(map.TableName);
            return map;
        }

        private string TranslateSubExpression(Expression e)
        {
            var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter);
            var subPlan = subTranslator.Translate(e);
            _paramIndex = subTranslator._paramIndex;
            return subPlan.Sql;
        }




        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _methodName = node.Method.Name;
            if (_methodTranslators.TryGetValue(_methodName, out var translator))
            {
                return translator.Translate(this, node);
            }
            return base.VisitMethodCall(node);
        }

        private Expression HandleJoin(MethodCallExpression node, bool isGroupJoin = false)
        {
            // Join signature: Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector)
            // node.Arguments[0] = outer query
            // node.Arguments[1] = inner query  
            // node.Arguments[2] = outer key selector
            // node.Arguments[3] = inner key selector
            // node.Arguments[4] = result selector

            if (node.Arguments.Count < 5)
                throw new NormQueryTranslationException("Join operation requires 5 arguments");

            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new NormQueryTranslationException("Join selectors must be lambda expressions");

            // Visit the outer query first to establish the base table
            Visit(outerQuery);

            // Get the inner table mapping
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = TrackMapping(innerElementType);

            // Generate table aliases
            var outerAlias = "T0";

            if (isGroupJoin)
            {
                var outerType = outerKeySelector.Parameters[0].Type;
                var innerType = innerKeySelector.Parameters[0].Type;
                var resultType = resultSelector.Body.Type;

                var innerAliasG = "T" + (++_joinCounter);

                if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                    _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
                var outerKeyVisitorG = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap);
                var outerKeySqlG = outerKeyVisitorG.Translate(outerKeySelector.Body);

                if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                    _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAliasG);
                var innerKeyVisitorG = new ExpressionToSqlVisitor(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAliasG, _correlatedParams, _compiledParams, _paramMap);
                var innerKeySqlG = innerKeyVisitorG.Translate(innerKeySelector.Body);

                foreach (var kvp in outerKeyVisitorG.GetParameters())
                    _params[kvp.Key] = kvp.Value;
                foreach (var kvp in innerKeyVisitorG.GetParameters())
                    _params[kvp.Key] = kvp.Value;

                var joinSqlG = new StringBuilder(256);
                var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerColumns = innerMapping.Columns.Select(c => $"{innerAliasG}.{c.EscCol}");
                joinSqlG.Append($"SELECT {string.Join(", ", outerColumns.Concat(innerColumns))} ");
                joinSqlG.Append($"FROM {_mapping.EscTable} {outerAlias} ");
                joinSqlG.Append($"LEFT JOIN {innerMapping.EscTable} {innerAliasG} ON {outerKeySqlG} = {innerKeySqlG}");
                joinSqlG.Append($" ORDER BY {outerKeySqlG}");

                _sql.Clear();
                _sql.Append(joinSqlG.ToString());

                // Reset projection so outer entities are materialized directly
                _projection = null;

                var innerKeyColumn = innerMapping.Columns.FirstOrDefault(c =>
                    ExtractPropertyName(innerKeySelector.Body) == c.PropName);
                if (innerKeyColumn != null)
                {
                    var outerKeyFunc = CreateObjectKeySelector(outerKeySelector);
                    var resultSelectorFunc = CompileGroupJoinResultSelector(resultSelector);
                    _groupJoinInfo = new GroupJoinInfo(
                        outerType,
                        innerType,
                        resultType,
                        outerKeyFunc,
                        innerKeyColumn,
                        resultSelectorFunc
                    );
                }

                return node;
            }

            var innerAlias = "T" + (++_joinCounter);

            var joinType = "INNER JOIN";

            if (!_correlatedParams.ContainsKey(outerKeySelector.Parameters[0]))
                _correlatedParams[outerKeySelector.Parameters[0]] = (_mapping, outerAlias);
            var outerKeyVisitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias, _correlatedParams, _compiledParams, _paramMap);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);

            if (!_correlatedParams.ContainsKey(innerKeySelector.Parameters[0]))
                _correlatedParams[innerKeySelector.Parameters[0]] = (innerMapping, innerAlias);
            var innerKeyVisitor = new ExpressionToSqlVisitor(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias, _correlatedParams, _compiledParams, _paramMap);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);

            foreach (var kvp in outerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            foreach (var kvp in innerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;

            var joinSql = new StringBuilder(256);

            if (_projection?.Body is NewExpression newExpr)
            {
                var neededColumns = ExtractNeededColumns(newExpr, _mapping, innerMapping);
                if (neededColumns.Count == 0)
                {
                    var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerColumns = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.Append($"SELECT {string.Join(", ", outerColumns.Concat(innerColumns))} ");
                }
                else
                {
                    joinSql.Append($"SELECT {string.Join(", ", neededColumns)} ");
                }
            }
            else
            {
                var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerColumns = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                joinSql.Append($"SELECT {string.Join(", ", outerColumns.Concat(innerColumns))} ");
            }

            joinSql.Append($"FROM {_mapping.EscTable} {outerAlias} ");
            joinSql.Append($"{joinType} {innerMapping.EscTable} {innerAlias} ");
            joinSql.Append($"ON {outerKeySql} = {innerKeySql}");

            _sql.Clear();
            _sql.Append(joinSql.ToString());

            if (resultSelector != null)
                _projection = resultSelector;

            return node;
        }

        private Expression HandleGroupJoin(MethodCallExpression node)
        {
            return HandleJoin(node, isGroupJoin: true);
        }

        private Expression HandleSelectMany(MethodCallExpression node)
        {
            // SelectMany can be used in different ways:
            // 1. SelectMany(collectionSelector) - flattens collections
            // 2. SelectMany(collectionSelector, resultSelector) - joins and projects

            if (node.Arguments.Count < 2)
                throw new NormQueryTranslationException("SelectMany requires at least 2 arguments");

            var sourceQuery = node.Arguments[0];
            var collectionSelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                                   ?? throw new NormQueryTranslationException("Collection selector must be a lambda expression");

            // Visit the source query first to establish base mapping
            Visit(sourceQuery);

            var outerMapping = _mapping;
            var outerAlias = "T0";

            // Track the outer parameter for correlated references
            if (!_correlatedParams.ContainsKey(collectionSelector.Parameters[0]))
                _correlatedParams[collectionSelector.Parameters[0]] = (outerMapping, outerAlias);

            // Determine if a result selector is provided
            var resultSelector = node.Arguments.Count > 2
                ? StripQuotes(node.Arguments[2]) as LambdaExpression
                : null;

            // Navigation property: treat as INNER JOIN
            if (collectionSelector.Body is MemberExpression memberExpr &&
                outerMapping.Relations.TryGetValue(memberExpr.Member.Name, out var relation))
            {
                var innerMapping = TrackMapping(relation.DependentType);
                var innerAlias = "T" + (++_joinCounter);

                if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                    !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
                {
                    _correlatedParams[resultSelector.Parameters[1]] = (innerMapping, innerAlias);
                }

                var joinSql = new StringBuilder(256);

                if (_projection?.Body is NewExpression newExpr)
                {
                    var neededColumns = ExtractNeededColumns(newExpr, outerMapping, innerMapping);
                    if (neededColumns.Count == 0)
                    {
                        var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                        var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                        joinSql.Append($"SELECT {string.Join(", ", outerCols.Concat(innerCols))} ");
                    }
                    else
                    {
                        joinSql.Append($"SELECT {string.Join(", ", neededColumns)} ");
                    }
                }
                else if (resultSelector == null)
                {
                    var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.Append($"SELECT {string.Join(", ", innerCols)} ");
                }
                else
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                    joinSql.Append($"SELECT {string.Join(", ", outerCols.Concat(innerCols))} ");
                }

                joinSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
                joinSql.Append($"INNER JOIN {innerMapping.EscTable} {innerAlias} ");
                joinSql.Append($"ON {outerAlias}.{relation.PrincipalKey.EscCol} = {innerAlias}.{relation.ForeignKey.EscCol}");

                _sql.Clear();
                _sql.Append(joinSql.ToString());

                if (resultSelector != null)
                {
                    _projection = resultSelector;
                }
                else
                {
                    _mapping = innerMapping;
                }

                return node;
            }

            // Otherwise treat as CROSS JOIN
            var innerType = GetElementType(collectionSelector.Body);
            var crossMapping = TrackMapping(innerType);
            var crossAlias = "T" + (++_joinCounter);

            if (resultSelector != null && resultSelector.Parameters.Count > 1 &&
                !_correlatedParams.ContainsKey(resultSelector.Parameters[1]))
            {
                _correlatedParams[resultSelector.Parameters[1]] = (crossMapping, crossAlias);
            }

            var crossSql = new StringBuilder(256);

            if (_projection?.Body is NewExpression crossNew)
            {
                var neededColumns = ExtractNeededColumns(crossNew, outerMapping, crossMapping);
                if (neededColumns.Count == 0)
                {
                    var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                    var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                    crossSql.Append($"SELECT {string.Join(", ", outerCols.Concat(innerCols))} ");
                }
                else
                {
                    crossSql.Append($"SELECT {string.Join(", ", neededColumns)} ");
                }
            }
            else if (resultSelector == null)
            {
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.Append($"SELECT {string.Join(", ", innerCols)} ");
            }
            else
            {
                var outerCols = outerMapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerCols = crossMapping.Columns.Select(c => $"{crossAlias}.{c.EscCol}");
                crossSql.Append($"SELECT {string.Join(", ", outerCols.Concat(innerCols))} ");
            }

            crossSql.Append($"FROM {outerMapping.EscTable} {outerAlias} ");
            crossSql.Append($"CROSS JOIN {crossMapping.EscTable} {crossAlias}");

            _sql.Clear();
            _sql.Append(crossSql.ToString());

            if (resultSelector != null)
            {
                _projection = resultSelector;
            }
            else
            {
                _mapping = crossMapping;
            }

            return node;
        }

        private Expression HandleSetOperation(MethodCallExpression node)
        {
            _isAggregate = true;
            _singleResult = true;

            var source = node.Arguments[0];
            var elementType = source.Type.GetGenericArguments().First();

            if (node.Method.Name == nameof(Queryable.Any) && node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression anyPred)
            {
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(anyPred));
            }
            else if (node.Method.Name == nameof(Queryable.All) && node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression allPred)
            {
                var param = allPred.Parameters[0];
                var notBody = Expression.Not(allPred.Body);
                var notPred = Expression.Lambda(notBody, param);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(notPred));
            }
            else if (node.Method.Name == nameof(Queryable.Contains) && node.Arguments.Count == 2)
            {
                var param = Expression.Parameter(elementType, "x");
                var value = Expression.Convert(node.Arguments[1], elementType);
                var eq = Expression.Equal(param, value);
                var lambda = Expression.Lambda(eq, param);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { elementType }, source, Expression.Quote(lambda));
            }

            var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex, _correlatedParams, _tables, _compiledParams, _paramMap, _joinCounter);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator._paramIndex;
            _mapping = subTranslator._mapping;

            var subSqlBuilder = new StringBuilder();
            var fromIndex = subPlan.Sql.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);
            if (fromIndex >= 0)
            {
                subSqlBuilder.Append("SELECT 1 ");
                subSqlBuilder.Append(subPlan.Sql[fromIndex..]);
            }
            else
            {
                subSqlBuilder.Append(subPlan.Sql);
            }
            _ctx.Provider.ApplyPaging(subSqlBuilder, 1, null, null, null);

            switch (node.Method.Name)
            {
                case nameof(Queryable.Any):
                case nameof(Queryable.Contains):
                    _sql.Append("SELECT 1 WHERE EXISTS(");
                    _sql.Append(subSqlBuilder);
                    _sql.Append(")");
                    break;
                case nameof(Queryable.All):
                    _sql.Append("SELECT 1 WHERE NOT EXISTS(");
                    _sql.Append(subSqlBuilder);
                    _sql.Append(")");
                    break;
            }

            return node;
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

        private static bool TryGetIntValue(Expression expr, out int value)
        {
            value = 0;
            if (expr is ConstantExpression c && c.Value is int i)
            {
                value = i;
                return true;
            }
            return false;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q && q.ElementType != null)
            {
                if (_rootType == null || q.ElementType != _rootType)
                {
                    _rootType = q.ElementType;
            _mapping = TrackMapping(q.ElementType);
                }
                return node;
            }

            if (node.Value != null)
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                _params[paramName] = node.Value;
                _sql.Append(paramName);
            }
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (_correlatedParams.ContainsKey(node))
                return base.VisitParameter(node);

            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }

            var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
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
                _ => throw new NormUnsupportedFeatureException($"Op '{node.NodeType}' not supported.")
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression?.NodeType == ExpressionType.Parameter)
            {
                _sql.Append(_mapping.Columns.First(c => c.Prop.Name == node.Member.Name).EscCol);
                return node;
            }

            if (TryGetConstantValue(node, out var value))
            {
                var paramName = _ctx.Provider.ParamPrefix + "p" + _paramIndex++;
                _params[paramName] = value ?? DBNull.Value;
                _sql.Append(paramName);
                return node;
            }

            throw new NormUnsupportedFeatureException($"Member '{node.Member.Name}' is not supported in this context.");
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        private static List<string> ExtractNeededColumns(NewExpression newExpr, TableMapping outerMapping, TableMapping innerMapping)
        {
            var neededColumns = new HashSet<string>();
            var outerAlias = "T0";
            var innerAlias = "T1";

            foreach (var arg in newExpr.Arguments)
            {
                if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression paramExpr)
                {
                    var isOuterTable = paramExpr.Type == outerMapping.Type;
                    var mapping = isOuterTable ? outerMapping : innerMapping;
                    var alias = isOuterTable ? outerAlias : innerAlias;

                    var column = mapping.Columns.FirstOrDefault(c => c.Prop.Name == memberExpr.Member.Name);
                    if (column != null)
                    {
                        neededColumns.Add($"{alias}.{column.EscCol}");
                    }
                }
                else if (arg is ParameterExpression param)
                {
                    var isOuter = param.Type == outerMapping.Type;
                    var mapping = isOuter ? outerMapping : innerMapping;
                    var alias = isOuter ? outerAlias : innerAlias;
                    foreach (var col in mapping.Columns)
                        neededColumns.Add($"{alias}.{col.EscCol}");
                }
            }

            return neededColumns.ToList();
        }

        private static bool IsRecordType(Type type) =>
            type.GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.NonPublic) != null;

        private static string? ExtractPropertyName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression { Operand: MemberExpression member } => member.Member.Name,
                _ => null
            };
        }

        private Expression HandleAggregateExpression(MethodCallExpression node)
        {
            // node.Arguments[0] = source query
            // node.Arguments[1] = selector lambda
            // node.Arguments[2] = function name
            
            var sourceQuery = node.Arguments[0];
            var selectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            var functionConstant = node.Arguments[2] as ConstantExpression;
            
            if (selectorLambda == null || functionConstant?.Value is not string functionName)
                throw new NormQueryTranslationException("Invalid aggregate expression structure");

            Visit(sourceQuery);
            
            var param = selectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var columnSql = visitor.Translate(selectorLambda.Body);
            
            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;

            _isAggregate = true;
            _sql.Clear();
            
            var sqlFunction = functionName.ToUpperInvariant();
            if (sqlFunction == "AVERAGE") sqlFunction = "AVG";
            
            _sql.Append($"SELECT {sqlFunction}({columnSql})");
            
            return node;
        }

        private Expression HandleGroupBy(MethodCallExpression node)
        {
            // GroupBy(source, keySelector) or GroupBy(source, keySelector, resultSelector)
            var sourceQuery = node.Arguments[0];
            var keySelectorLambda = StripQuotes(node.Arguments[1]) as LambdaExpression;
            
            if (keySelectorLambda == null)
                throw new NormQueryTranslationException("GroupBy key selector must be a lambda expression");

            Visit(sourceQuery);
            
            var param = keySelectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var groupBySql = visitor.Translate(keySelectorLambda.Body);
            
            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;

            _groupBy.Add(groupBySql);
            
            // If there's a result selector, handle the projection
            if (node.Arguments.Count > 2)
            {
                var resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
                if (resultSelector != null)
                {
                    _projection = resultSelector;
                    
                    // Clear the default select and let the projection handling rebuild it
                    _sql.Clear();
                    
                    // Analyze the result selector to build appropriate SELECT clause
                    BuildGroupBySelectClause(resultSelector, groupBySql, alias);
                }
            }
            
            return node;
        }

        private void BuildGroupBySelectClause(LambdaExpression resultSelector, string groupBySql, string alias)
        {
            var selectItems = new List<string>();
            
            // Add the grouping key
            selectItems.Add($"{groupBySql} AS GroupKey");
            
            // Analyze the result selector body to find aggregates
            if (resultSelector.Body is NewExpression newExpr)
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberName = newExpr.Members?[i]?.Name ?? $"Item{i + 1}";
                    
                    if (arg is MethodCallExpression methodCall)
                    {
                        var aggregateSql = TranslateGroupAggregateMethod(methodCall, alias);
                        if (aggregateSql != null)
                        {
                            selectItems.Add($"{aggregateSql} AS {memberName}");
                        }
                    }
                    else if (arg is ParameterExpression param && param == resultSelector.Parameters[0])
                    {
                        // This is the key parameter, already added
                        continue;
                    }
                    else
                    {
                        // Try to translate as regular expression
                        if (!_correlatedParams.ContainsKey(resultSelector.Parameters[0]))
                            _correlatedParams[resultSelector.Parameters[0]] = (_mapping, alias);
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                        var sql = visitor.Translate(arg);
                        selectItems.Add($"{sql} AS {memberName}");
                        
                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                    }
                }
            }
            
            _sql.Append($"SELECT {string.Join(", ", selectItems)}");
        }

        private string? TranslateGroupAggregateMethod(MethodCallExpression methodCall, string alias)
        {
            var methodName = methodCall.Method.Name;
            
            // Handle IGrouping<TKey, TElement> methods
            switch (methodName)
            {
                case "Count":
                    return "COUNT(*)";
                case "LongCount":
                    return "COUNT(*)";
                case "Sum":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            return $"SUM({columnSql})";
                        }
                    }
                    return "SUM(*)";
                case "Average":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            return $"AVG({columnSql})";
                        }
                    }
                    return "AVG(*)";
                case "Min":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            return $"MIN({columnSql})";
                        }
                    }
                    return null;
                case "Max":
                    if (methodCall.Arguments.Count > 0)
                    {
                        var selector = StripQuotes(methodCall.Arguments[0]) as LambdaExpression;
                        if (selector != null)
                        {
                            if (!_correlatedParams.ContainsKey(selector.Parameters[0]))
                                _correlatedParams[selector.Parameters[0]] = (_mapping, alias);
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias, _correlatedParams, _compiledParams, _paramMap);
                            var columnSql = visitor.Translate(selector.Body);
                            foreach (var kvp in visitor.GetParameters())
                                _params[kvp.Key] = kvp.Value;
                            return $"MAX({columnSql})";
                        }
                    }
                    return null;
                default:
                    return null;
            }
        }

        private Expression HandleDirectAggregate(MethodCallExpression node)
        {
            // Handle direct aggregate calls like query.Sum(x => x.Amount)
            var sourceQuery = node.Arguments[0];
            
            Visit(sourceQuery);
            
            if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression selector)
            {
                var param = selector.Parameters[0];
                var alias = "T" + _joinCounter;
                if (!_correlatedParams.ContainsKey(param))
                    _correlatedParams[param] = (_mapping, alias);
                var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
                var columnSql = visitor.Translate(selector.Body);
                
                foreach (var kvp in visitor.GetParameters())
                    _params[kvp.Key] = kvp.Value;

                _isAggregate = true;
                _sql.Clear();
                
                var sqlFunction = node.Method.Name.ToUpperInvariant();
                if (sqlFunction == "AVERAGE") sqlFunction = "AVG";
                
                _sql.Append($"SELECT {sqlFunction}({columnSql})");
            }
            
            return node;
        }

        private Expression HandleAllOperation(MethodCallExpression node)
        {
            // ALL is translated as NOT EXISTS with negated predicate
            var sourceQuery = node.Arguments[0];
            var predicate = node.Arguments[1] as LambdaExpression;
            
            if (predicate == null)
                throw new NormQueryTranslationException("All operation requires a predicate");

            Visit(sourceQuery);
            
            // Create negated predicate: NOT (predicate)
            var param = predicate.Parameters[0];
            var alias = "T" + _joinCounter;
            if (!_correlatedParams.ContainsKey(param))
                _correlatedParams[param] = (_mapping, alias);
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias, _correlatedParams, _compiledParams, _paramMap);
            var predicateSql = visitor.Translate(predicate.Body);
            
            foreach (var kvp in visitor.GetParameters())
                _params[kvp.Key] = kvp.Value;

            // Wrap in NOT EXISTS
            _sql.Insert(0, "SELECT CASE WHEN NOT EXISTS(");
            if (_where.Length > 0)
                _where.Append($" AND NOT ({predicateSql})");
            else
                _where.Append($"NOT ({predicateSql})");
            _sql.Append(") THEN 1 ELSE 0 END");
            
            return node;
        }

        private Expression HandleRowNumberOperation(MethodCallExpression node)
        {
            // WithRowNumber adds ROW_NUMBER() OVER() to the select clause
            var sourceQuery = node.Arguments[0];
            var resultSelector = node.Arguments[1] as LambdaExpression;
            
            if (resultSelector == null)
                throw new NormQueryTranslationException("WithRowNumber requires a result selector");

            Visit(sourceQuery);
            
            // Modify the projection to include ROW_NUMBER()
            _projection = resultSelector;
            
            // The result selector will be handled during materialization
            // We need to add ROW_NUMBER() OVER (ORDER BY ...) to the SELECT clause
            var orderByClause = _orderBy.Count > 0 
                ? $"ORDER BY {string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}"))}"
                : "ORDER BY (SELECT NULL)";
            
            if (_sql.Length == 0)
            {
                var select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                _sql.Append($"SELECT {select}, ROW_NUMBER() OVER ({orderByClause}) AS RowNumber FROM {_mapping.EscTable}");
            }
            else
            {
                // Insert ROW_NUMBER() into existing SELECT
                var selectIndex = _sql.ToString().IndexOf("SELECT") + 6;
                _sql.Insert(selectIndex, $" ROW_NUMBER() OVER ({orderByClause}) AS RowNumber,");
            }
            
            return node;
        }

        private Expression HandleRankOperation(MethodCallExpression node)
        {
            // WithRank adds RANK() OVER() to the select clause
            var sourceQuery = node.Arguments[0];
            var resultSelector = node.Arguments[1] as LambdaExpression;

            if (resultSelector == null)
                throw new NormQueryTranslationException("WithRank requires a result selector");

            Visit(sourceQuery);

            _projection = resultSelector;

            var orderByClause = _orderBy.Count > 0
                ? $"ORDER BY {string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}"))}"
                : "ORDER BY (SELECT NULL)";

            if (_sql.Length == 0)
            {
                var select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                _sql.Append($"SELECT {select}, RANK() OVER ({orderByClause}) AS Rank FROM {_mapping.EscTable}");
            }
            else
            {
                var selectIndex = _sql.ToString().IndexOf("SELECT") + 6;
                _sql.Insert(selectIndex, $" RANK() OVER ({orderByClause}) AS Rank,");
            }

            return node;
        }

        private Expression HandleDenseRankOperation(MethodCallExpression node)
        {
            // WithDenseRank adds DENSE_RANK() OVER() to the select clause
            var sourceQuery = node.Arguments[0];
            var resultSelector = node.Arguments[1] as LambdaExpression;

            if (resultSelector == null)
                throw new NormQueryTranslationException("WithDenseRank requires a result selector");

            Visit(sourceQuery);

            _projection = resultSelector;

            var orderByClause = _orderBy.Count > 0
                ? $"ORDER BY {string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}"))}"
                : "ORDER BY (SELECT NULL)";

            if (_sql.Length == 0)
            {
                var select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                _sql.Append($"SELECT {select}, DENSE_RANK() OVER ({orderByClause}) AS DenseRank FROM {_mapping.EscTable}");
            }
            else
            {
                var selectIndex = _sql.ToString().IndexOf("SELECT") + 6;
                _sql.Insert(selectIndex, $" DENSE_RANK() OVER ({orderByClause}) AS DenseRank,");
            }

            return node;
        }

        private static Func<object, object> CreateObjectKeySelector(LambdaExpression keySelector)
        {
            var parameterType = keySelector.Parameters[0].Type;
            var returnType = keySelector.ReturnType;

            var objParam = Expression.Parameter(typeof(object), "obj");
            var castParam = Expression.Convert(objParam, parameterType);
            var body = new ParameterReplacer(keySelector.Parameters[0], castParam).Visit(keySelector.Body)!;
            var convertBody = Expression.Convert(body, typeof(object));
            var invoker = Expression.Lambda<Func<object, object>>(convertBody, objParam).Compile();

            return obj =>
            {
                try
                {
                    var result = invoker(obj);
                    if (result == null)
                        return DBNull.Value;
                    if (returnType.IsValueType && result.GetType() != typeof(object))
                        return result;
                    return result;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Error executing key selector for type {parameterType.Name}: {ex.Message}", ex);
                }
            };
        }

        private static Func<object, IEnumerable<object>, object> CompileGroupJoinResultSelector(LambdaExpression resultSelector)
        {
            var outerParam = Expression.Parameter(typeof(object), "outer");
            var innerParam = Expression.Parameter(typeof(IEnumerable<object>), "inners");

            var castOuter = Expression.Convert(outerParam, resultSelector.Parameters[0].Type);
            var innerElementType = resultSelector.Parameters[1].Type.GetGenericArguments()[0];
            var castMethod = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(innerElementType);
            var castInner = Expression.Call(castMethod, innerParam);

            Expression body = resultSelector.Body;
            body = new ParameterReplacer(resultSelector.Parameters[0], castOuter).Visit(body)!;
            body = new ParameterReplacer(resultSelector.Parameters[1], castInner).Visit(body)!;
            body = Expression.Convert(body, typeof(object));

            var lambda = Expression.Lambda<Func<object, IEnumerable<object>, object>>(body, outerParam, innerParam);
            return lambda.Compile();
        }

        private sealed class ParameterReplacer : ExpressionVisitor
        {
            private readonly ParameterExpression _from;
            private readonly Expression _to;

            public ParameterReplacer(ParameterExpression from, Expression to)
            {
                _from = from;
                _to = to;
            }

            protected override Expression VisitParameter(ParameterExpression node) =>
                node == _from ? _to : base.VisitParameter(node);
        }
    }
}
