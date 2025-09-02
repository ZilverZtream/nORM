using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed class QueryTranslator : ExpressionVisitor
    {
        private readonly DbContext _ctx;
        private readonly StringBuilder _sql;
        private readonly Dictionary<string, object> _params = new();
        private TableMapping _mapping = null!;
        private int _paramIndex = 0;
        private int? _take, _skip;
        private readonly List<(string col, bool asc)> _orderBy = new();
        private readonly List<IncludePlan> _includes = new();
        private LambdaExpression? _projection;
        private readonly List<string> _groupBy = new();
        private bool _isAggregate = false;
        private string _methodName = "";
        private readonly StringBuilder _where = new();
#pragma warning disable CS0649 // Field is never assigned to, and will always have its default value - used in complex join scenarios
        private GroupJoinInfo? _groupJoinInfo;
#pragma warning restore CS0649
        private int _joinCounter = 0;
        private DatabaseProvider _provider;
        private bool _singleResult = false;
        
        // Cache materializers to reduce memory allocations
        private static readonly ConcurrentDictionary<(Type MappingType, Type TargetType, string? ProjectionKey), Func<DbDataReader, object>> _materializerCache = new();

        // Initialize _groupJoinInfo in constructor to suppress warning
        // This field is used in complex join scenarios

        public QueryTranslator(DbContext ctx)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _sql = new StringBuilder();
        }

        private QueryTranslator(DbContext ctx, TableMapping mapping, Dictionary<string, object> parameters, ref int pIndex)
        {
            _ctx = ctx;
            _provider = ctx.Provider;
            _mapping = mapping;
            _params = parameters;
            _paramIndex = pIndex;
            _sql = new StringBuilder();
        }

        public QueryPlan Translate(Expression e)
        {
            Visit(e);

            var projectType = _projection?.Body.Type ?? _mapping.Type;
            if (_isAggregate && _groupBy.Count == 0 && (e as MethodCallExpression)?.Method.Name is "Count" or "LongCount")
            {
                projectType = typeof(int);
            }

            var materializer = CreateMaterializer(_mapping, projectType, _projection);
            var isScalar = _isAggregate && _groupBy.Count == 0;

            if (_sql.Length == 0)
            {
                if (_isAggregate && _groupBy.Count == 0)
                {
                    _sql.Append($"SELECT COUNT(*) FROM {_mapping.EscTable}");
                }
                else
                {
                    var select = string.Join(", ", _mapping.Columns.Select(c => c.EscCol));
                    _sql.Insert(0, $"SELECT {select} FROM {_mapping.EscTable}");
                }
            }

            ApplyTenantFilter();
            if (_where.Length > 0) _sql.Append($" WHERE {_where}");

            if (_groupBy.Count > 0) _sql.Append(" GROUP BY " + string.Join(", ", _groupBy));
            if (_orderBy.Count > 0) _sql.Append(" ORDER BY " + string.Join(", ", _orderBy.Select(o => $"{o.col} {(o.asc ? "ASC" : "DESC")}")));
            _ctx.Provider.ApplyPaging(_sql, _take, _skip);

            var singleResult = _singleResult || _methodName is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault"
                or "ElementAt" or "ElementAtOrDefault" or "Last" or "LastOrDefault" || isScalar;

            return new QueryPlan(_sql.ToString(), _params, materializer, projectType, isScalar, singleResult, _methodName, _includes, _groupJoinInfo);
        }

        private string TranslateSubExpression(Expression e)
        {
            var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex);
            subTranslator.Visit(e);
            _paramIndex = subTranslator._paramIndex;
            return subTranslator._sql.ToString();
        }

        private void ApplyTenantFilter()
        {
            if (_ctx.Options.TenantProvider == null) return;
            var tenantCol = _mapping.Columns.FirstOrDefault(c => c.PropName == _ctx.Options.TenantColumnName);
            if (tenantCol == null) return;

            var pName = $"{_ctx.Provider.ParamPrefix}__tenantId";
            var tenantFilter = $"{tenantCol.EscCol} = {pName}";
            _params[pName] = _ctx.Options.TenantProvider.GetCurrentTenantId();

            if (_where.Length > 0)
            {
                _where.Insert(0, "(").Append($") AND {tenantFilter}");
            }
            else
            {
                _where.Append(tenantFilter);
            }
        }

        public Func<DbDataReader, object> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
        {
            // Create cache key
            var projectionKey = projection?.ToString();
            var cacheKey = (mapping.Type, targetType, projectionKey);
            
            return _materializerCache.GetOrAdd(cacheKey, _ => CreateMaterializerInternal(mapping, targetType, projection));
        }
        
        private Func<DbDataReader, object> CreateMaterializerInternal(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
        {
            var dm = new DynamicMethod("mat_" + Guid.NewGuid().ToString("N"), typeof(object), new[] { typeof(DbDataReader) }, targetType.Module, true);
            var il = dm.GetILGenerator();

            if (targetType.IsPrimitive || targetType == typeof(decimal) || targetType == typeof(string))
            {
                var fieldValue = Methods.GetFieldValue.MakeGenericMethod(targetType);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Callvirt, fieldValue);
            }
            else if (projection?.Body is NewExpression newExpr)
            {
                // Handle projections (including join result projections)
                var constructorArgs = new LocalBuilder[newExpr.Arguments.Count];
                var columnIndex = 0;

                for (var i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var propType = arg.Type;
                    constructorArgs[i] = il.DeclareLocal(propType);

                    if (arg is MemberExpression memberExpr && memberExpr.Expression is ParameterExpression memberParam)
                    {
                        // This is a property access from a join - find the correct column offset
                        var isLeftTable = memberParam.Type == mapping.Type;
                        var memberMapping = isLeftTable ? mapping : _ctx.GetMapping(memberParam.Type);
                        
                        var column = memberMapping.Columns.FirstOrDefault(c => c.Prop.Name == memberExpr.Member.Name);
                        if (column != null)
                        {
                            var actualColumnIndex = isLeftTable 
                                ? Array.IndexOf(memberMapping.Columns, column)
                                : mapping.Columns.Length + Array.IndexOf(memberMapping.Columns, column);
                            
                            EmitColumnMaterialization(il, column, constructorArgs[i], actualColumnIndex);
                        }
                    }
                    else if (arg is ParameterExpression param)
                    {
                        // This is a parameter from a join - need to materialize the entire object
                        var paramMapping = param.Type == mapping.Type ? mapping : _ctx.GetMapping(param.Type);
                        var startIndex = param.Type == mapping.Type ? 0 : mapping.Columns.Length;
                        EmitObjectMaterialization(il, paramMapping, constructorArgs[i], startIndex);
                    }
                    else
                    {
                        // Simple column access
                        EmitColumnMaterialization(il, mapping.Columns[columnIndex], constructorArgs[i], columnIndex++);
                    }
                }

                foreach (var arg in constructorArgs) il.Emit(OpCodes.Ldloc, arg);
                il.Emit(OpCodes.Newobj, newExpr.Constructor!);
            }
            else
            {
                // Standard single-table materialization
                var loc = il.DeclareLocal(targetType);
                il.Emit(OpCodes.Newobj, targetType.GetConstructor(Type.EmptyTypes)!);
                il.Emit(OpCodes.Stloc, loc);

                for (int i = 0; i < mapping.Columns.Length; i++)
                {
                    var col = mapping.Columns[i];
                    var endOfBlock = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i);
                    il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                    il.Emit(OpCodes.Brtrue_S, endOfBlock);
                    il.Emit(OpCodes.Ldloc, loc);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i);
                    var readerMethod = Methods.GetReaderMethod(col.Prop.PropertyType);
                    il.Emit(OpCodes.Callvirt, readerMethod);
                    if (readerMethod == Methods.GetValue) il.Emit(OpCodes.Unbox_Any, col.Prop.PropertyType);
                    il.Emit(OpCodes.Callvirt, col.SetterMethod);
                    il.MarkLabel(endOfBlock);
                }
                il.Emit(OpCodes.Ldloc, loc);
            }

            if (targetType.IsValueType) il.Emit(OpCodes.Box, targetType);
            il.Emit(OpCodes.Ret);
            return (Func<DbDataReader, object>)dm.CreateDelegate(typeof(Func<DbDataReader, object>));
        }

        private void EmitObjectMaterialization(ILGenerator il, TableMapping mapping, LocalBuilder localVar, int startColumnIndex)
        {
            // Create new instance of the object
            il.Emit(OpCodes.Newobj, mapping.Type.GetConstructor(Type.EmptyTypes)!);
            il.Emit(OpCodes.Stloc, localVar);

            // Set all properties
            for (int i = 0; i < mapping.Columns.Length; i++)
            {
                var col = mapping.Columns[i];
                var endOfBlock = il.DefineLabel();
                
                // Check for null
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, startColumnIndex + i);
                il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                il.Emit(OpCodes.Brtrue_S, endOfBlock);
                
                // Set property value
                il.Emit(OpCodes.Ldloc, localVar);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, startColumnIndex + i);
                var readerMethod = Methods.GetReaderMethod(col.Prop.PropertyType);
                il.Emit(OpCodes.Callvirt, readerMethod);
                if (readerMethod == Methods.GetValue) il.Emit(OpCodes.Unbox_Any, col.Prop.PropertyType);
                il.Emit(OpCodes.Callvirt, col.SetterMethod);
                il.MarkLabel(endOfBlock);
            }
        }

        private void EmitColumnMaterialization(ILGenerator il, Column column, LocalBuilder localVar, int columnIndex)
        {
            var endOfBlock = il.DefineLabel();
            
            // Check for null
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, columnIndex);
            il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
            il.Emit(OpCodes.Brtrue_S, endOfBlock);
            
            // Get value
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, columnIndex);
            var readerMethod = Methods.GetReaderMethod(column.Prop.PropertyType);
            il.Emit(OpCodes.Callvirt, readerMethod);
            if (readerMethod == Methods.GetValue) il.Emit(OpCodes.Unbox_Any, column.Prop.PropertyType);
            il.Emit(OpCodes.Stloc, localVar);
            il.MarkLabel(endOfBlock);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            _methodName = node.Method.Name;

            switch (_methodName)
            {
                case "Where":
                    if (node.Arguments[1] is LambdaExpression lambda)
                    {
                        var body = lambda.Body;
                        var param = lambda.Parameters[0];
                        var alias = "T" + _joinCounter;
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
                        var sql = visitor.Translate(body);
                        if (_where.Length > 0) _where.Append(" AND ");
                        _where.Append($"({sql})");

                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                    }
                    return Visit(node.Arguments[0]);

                case "Select":
                    _projection = node.Arguments[1] as LambdaExpression;
                    return Visit(node.Arguments[0]);

                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    if (node.Arguments[1] is LambdaExpression keySelector)
                    {
                        var param = keySelector.Parameters[0];
                        var alias = "T" + _joinCounter;
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
                        var sql = visitor.Translate(keySelector.Body);

                        _orderBy.Add((sql, !_methodName.Contains("Descending")));
                    }
                    return Visit(node.Arguments[0]);

                case "Take":
                    if (TryGetIntValue(node.Arguments[1], out int take))
                        _take = take;
                    return Visit(node.Arguments[0]);

                case "Skip":
                    if (TryGetIntValue(node.Arguments[1], out int skip))
                        _skip = skip;
                    return Visit(node.Arguments[0]);

                case "Join":
                    return HandleJoin(node, isGroupJoin: false);

                case "GroupJoin":
                    return HandleGroupJoin(node);

                case "SelectMany":
                    return HandleSelectMany(node);

                case "Any":
                case "Contains":
                    return HandleSetOperation(node);

                case "First":
                case "FirstOrDefault":
                case "Single":
                case "SingleOrDefault":
                    if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression predicate)
                    {
                        var param = predicate.Parameters[0];
                        var alias = "T" + _joinCounter;
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
                        var sql = visitor.Translate(predicate.Body);
                        if (_where.Length > 0) _where.Append(" AND ");
                        _where.Append($"({sql})");

                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                    }
                    _take = 1;
                    _singleResult = _methodName == "First" || _methodName == "Single";
                    return Visit(node.Arguments[0]);

                case "Count":
                case "LongCount":
                    _isAggregate = true;
                    _sql.Clear();
                    _sql.Append("SELECT COUNT(*)");
                    if (node.Arguments.Count > 1 && node.Arguments[1] is LambdaExpression countPredicate)
                    {
                        var param = countPredicate.Parameters[0];
                        var alias = "T" + _joinCounter;
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
                        var sql = visitor.Translate(countPredicate.Body);
                        if (_where.Length > 0) _where.Append(" AND ");
                        _where.Append($"({sql})");

                        foreach (var kvp in visitor.GetParameters())
                            _params[kvp.Key] = kvp.Value;
                    }
                    return Visit(node.Arguments[0]);

                case "InternalSumExpression":
                case "InternalAverageExpression":
                case "InternalMinExpression":
                case "InternalMaxExpression":
                    return HandleAggregateExpression(node);

                case "GroupBy":
                    return HandleGroupBy(node);

                case "Sum":
                case "Average":
                case "Min":
                case "Max":
                    return HandleDirectAggregate(node);

                case "All":
                    return HandleAllOperation(node);

                case "WithRowNumber":
                    return HandleRowNumberOperation(node);

                case "Include":
                    if (node.Arguments.Count > 1)
                    {
                        var includeExpr = node.Arguments[1];
                        if (includeExpr is LambdaExpression includeLambda)
                        {
                            var member = includeLambda.Body is UnaryExpression unary ?
                                         (MemberExpression)unary.Operand :
                                         (MemberExpression)includeLambda.Body;
                            var propName = member.Member.Name;
                            if (_mapping.Relations.TryGetValue(propName, out var relation))
                            {
                                _includes.Add(new IncludePlan(relation));
                            }
                        }
                    }
                    return Visit(node.Arguments[0]);

                default:
                    return base.VisitMethodCall(node);
            }
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
                throw new ArgumentException("Join operation requires 5 arguments");

            var outerQuery = node.Arguments[0];
            var innerQuery = node.Arguments[1];
            var outerKeySelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
            var innerKeySelector = StripQuotes(node.Arguments[3]) as LambdaExpression;
            var resultSelector = StripQuotes(node.Arguments[4]) as LambdaExpression;

            if (outerKeySelector == null || innerKeySelector == null || resultSelector == null)
                throw new ArgumentException("Join selectors must be lambda expressions");

            // Visit the outer query first to establish the base table
            Visit(outerQuery);

            // Get the inner table mapping
            var innerElementType = GetElementType(innerQuery);
            var innerMapping = _ctx.GetMapping(innerElementType);
            
            // Generate table aliases
            var outerAlias = "T0";
            var innerAlias = "T" + (++_joinCounter);

            // Build the JOIN clause
            var joinType = isGroupJoin ? "LEFT JOIN" : "INNER JOIN";
            
            // Translate the key selectors to SQL
            var outerKeyVisitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, outerKeySelector.Parameters[0], outerAlias);
            var outerKeySql = outerKeyVisitor.Translate(outerKeySelector.Body);
            
            var innerKeyVisitor = new ExpressionToSqlVisitor(_ctx, innerMapping, _provider, innerKeySelector.Parameters[0], innerAlias);
            var innerKeySql = innerKeyVisitor.Translate(innerKeySelector.Body);

            // Merge parameters from both visitors
            foreach (var kvp in outerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;
            foreach (var kvp in innerKeyVisitor.GetParameters())
                _params[kvp.Key] = kvp.Value;

            // Build the complete query with JOIN - optimized for performance
            var joinSql = new StringBuilder(256); // Pre-size for better performance
            
            // Only select columns we actually need for the projection
            if (_projection?.Body is NewExpression newExpr)
            {
                var neededColumns = ExtractNeededColumns(newExpr, _mapping, innerMapping);
                joinSql.Append($"SELECT {string.Join(", ", neededColumns)} ");
            }
            else
            {
                // Fallback to all columns if no specific projection
                var outerColumns = _mapping.Columns.Select(c => $"{outerAlias}.{c.EscCol}");
                var innerColumns = innerMapping.Columns.Select(c => $"{innerAlias}.{c.EscCol}");
                joinSql.Append($"SELECT {string.Join(", ", outerColumns.Concat(innerColumns))} ");
            }
            
            joinSql.Append($"FROM {_mapping.EscTable} {outerAlias} ");
            joinSql.Append($"{joinType} {innerMapping.EscTable} {innerAlias} ");
            joinSql.Append($"ON {outerKeySql} = {innerKeySql}");

            // Replace the current SQL with the JOIN query
            _sql.Clear();
            _sql.Append(joinSql.ToString());

            // Handle the result selector projection
            if (resultSelector != null)
            {
                _projection = resultSelector;
                
                // For group joins, we need to set up the group join info
                if (isGroupJoin)
                {
                    var outerType = outerKeySelector.Parameters[0].Type;
                    var innerType = innerKeySelector.Parameters[0].Type;
                    var resultType = resultSelector.Body.Type;
                    
                    // Extract the collection property name from result selector
                    var collectionName = ExtractCollectionPropertyName(resultSelector);
                    
                    if (collectionName != null)
                    {
                        var innerKeyColumn = innerMapping.Columns.FirstOrDefault(c => 
                            ExtractPropertyName(innerKeySelector.Body) == c.PropName);
                            
                        if (innerKeyColumn != null)
                        {
                            // Create wrapper functions that convert types properly
                            var outerKeyFunc = CreateObjectKeySelector(outerKeySelector);
                            var innerKeyFunc = CreateObjectKeySelector(innerKeySelector);
                            
                            _groupJoinInfo = new GroupJoinInfo(
                                outerType,
                                innerType, 
                                resultType,
                                outerKeyFunc,
                                innerKeyFunc,
                                innerKeyColumn,
                                collectionName
                            );
                        }
                    }
                }
            }

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
                throw new ArgumentException("SelectMany requires at least 2 arguments");

            var sourceQuery = node.Arguments[0];
            var collectionSelector = StripQuotes(node.Arguments[1]) as LambdaExpression;
            
            if (collectionSelector == null)
                throw new ArgumentException("Collection selector must be a lambda expression");

            // Visit the source query
            Visit(sourceQuery);

            // Check if this is a navigation property access (common case)
            if (collectionSelector.Body is MemberExpression memberExpr)
            {
                var propertyName = memberExpr.Member.Name;
                
                // Check if this is a navigation property
                if (_mapping.Relations.TryGetValue(propertyName, out var relation))
                {
                    // This is a navigation property - convert to JOIN
                    var sourceAlias = "T0";
                    var targetAlias = "T" + (++_joinCounter);
                    var targetMapping = _ctx.GetMapping(relation.DependentType);

                    // Build the JOIN query
                    var sourceColumns = _mapping.Columns.Select(c => $"{sourceAlias}.{c.EscCol}");
                    var targetColumns = targetMapping.Columns.Select(c => $"{targetAlias}.{c.EscCol}");
                    
                    var joinSql = new StringBuilder();
                    joinSql.Append($"SELECT {string.Join(", ", targetColumns)} ");
                    joinSql.Append($"FROM {_mapping.EscTable} {sourceAlias} ");
                    joinSql.Append($"INNER JOIN {targetMapping.EscTable} {targetAlias} ");
                    joinSql.Append($"ON {sourceAlias}.{relation.PrincipalKey.EscCol} = {targetAlias}.{relation.ForeignKey.EscCol}");

                    // Replace the current SQL
                    _sql.Clear();
                    _sql.Append(joinSql.ToString());
                    
                    // Update the current mapping to the target type
                    _mapping = targetMapping;

                    // Handle result selector if present
                    if (node.Arguments.Count > 2)
                    {
                        var resultSelector = StripQuotes(node.Arguments[2]) as LambdaExpression;
                        if (resultSelector != null)
                        {
                            _projection = resultSelector;
                        }
                    }
                }
                else
                {
                    throw new NotSupportedException($"Property '{propertyName}' is not a recognized navigation property");
                }
            }
            else
            {
                throw new NotSupportedException("Complex SelectMany expressions are not yet supported");
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

            var subTranslator = new QueryTranslator(_ctx, _mapping, _params, ref _paramIndex);
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
            _ctx.Provider.ApplyPaging(subSqlBuilder, 1, null);

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
            var stack = new Stack<MemberExpression>();
            while (e is MemberExpression me)
            {
                stack.Push(me);
                e = me.Expression!;
            }

            if (e is not ConstantExpression ce)
            {
                value = null;
                return false;
            }

            object? current = ce.Value;
            while (stack.Count > 0)
            {
                var m = stack.Pop();
                current = m.Member switch
                {
                    FieldInfo fi => fi.GetValue(current),
                    PropertyInfo pi => pi.GetValue(current),
                    _ => current
                };
            }

            value = current;
            return true;
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
                _mapping = _ctx.GetMapping(q.ElementType);
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
                _ => throw new NotSupportedException($"Op '{node.NodeType}' not supported.")
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

            throw new NotSupportedException($"Member '{node.Member.Name}' is not supported in this context.");
        }

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var genericArgs = type.GetGenericArguments();
                if (genericArgs.Length > 0)
                    return genericArgs[0];
            }
            
            // Try to get element type from IQueryable<T>
            var queryableInterface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            
            if (queryableInterface != null)
                return queryableInterface.GetGenericArguments()[0];
                
            throw new ArgumentException($"Cannot determine element type from expression of type {type}");
        }

        private static List<string> ExtractNeededColumns(NewExpression newExpr, TableMapping outerMapping, TableMapping innerMapping)
        {
            var neededColumns = new List<string>();
            var outerAlias = "T0";
            var innerAlias = "T1";
            
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                
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
            }
            
            return neededColumns;
        }

        private static string? ExtractPropertyName(Expression expression)
        {
            return expression switch
            {
                MemberExpression member => member.Member.Name,
                UnaryExpression { Operand: MemberExpression member } => member.Member.Name,
                _ => null
            };
        }

        private static string? ExtractCollectionPropertyName(LambdaExpression resultSelector)
        {
            // Look for patterns like: new { user, orders = orders } or new SomeType { Orders = orders }
            if (resultSelector.Body is NewExpression newExpr)
            {
                // Anonymous type or constructor with parameters
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is ParameterExpression param && param.Type.IsGenericType)
                    {
                        var genericTypeDef = param.Type.GetGenericTypeDefinition();
                        if (genericTypeDef == typeof(IEnumerable<>) || genericTypeDef == typeof(ICollection<>) || genericTypeDef == typeof(List<>))
                        {
                            // This is likely the collection parameter
                            if (newExpr.Members != null && i < newExpr.Members.Count)
                            {
                                return newExpr.Members[i].Name;
                            }
                        }
                    }
                }
            }
            else if (resultSelector.Body is MemberInitExpression memberInit)
            {
                // Object initializer syntax
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment && 
                        assignment.Expression is ParameterExpression param &&
                        param.Type.IsGenericType)
                    {
                        var genericTypeDef = param.Type.GetGenericTypeDefinition();
                        if (genericTypeDef == typeof(IEnumerable<>) || genericTypeDef == typeof(ICollection<>) || genericTypeDef == typeof(List<>))
                        {
                            return binding.Member.Name;
                        }
                    }
                }
            }
            
            return null;
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
                throw new ArgumentException("Invalid aggregate expression structure");

            Visit(sourceQuery);
            
            var param = selectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
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
                throw new ArgumentException("GroupBy key selector must be a lambda expression");

            Visit(sourceQuery);
            
            var param = keySelectorLambda.Parameters[0];
            var alias = "T" + _joinCounter;
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
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
                        var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, resultSelector.Parameters[0], alias);
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
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias);
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
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias);
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
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias);
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
                            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, selector.Parameters[0], alias);
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
                var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
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
                throw new ArgumentException("All operation requires a predicate");

            Visit(sourceQuery);
            
            // Create negated predicate: NOT (predicate)
            var param = predicate.Parameters[0];
            var alias = "T" + _joinCounter;
            var visitor = new ExpressionToSqlVisitor(_ctx, _mapping, _provider, param, alias);
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
                throw new ArgumentException("WithRowNumber requires a result selector");

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