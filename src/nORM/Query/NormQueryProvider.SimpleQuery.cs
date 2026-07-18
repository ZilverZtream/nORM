using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {        private static string BuildSimpleWhereCacheKey(LambdaExpression lambda)
        {
            if (lambda.Body is MemberExpression { Type: var memberType } member &&
                memberType == typeof(bool) &&
                TableMapping.TryGetMemberAccessPath(member, out var boolPath))
            {
                return boolPath;
            }

            if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not, Operand: MemberExpression { Type: var negatedType } negatedMember }
                && negatedType == typeof(bool) &&
                TableMapping.TryGetMemberAccessPath(negatedMember, out var negatedPath))
            {
                return string.Concat(negatedPath, ":BOOL_FALSE");
            }

            if (lambda.Body is BinaryExpression { NodeType: ExpressionType.Equal } binary
                && binary.Left is MemberExpression comparedMember &&
                TableMapping.TryGetMemberAccessPath(comparedMember, out var comparedPath))
            {
                if (comparedMember.Type == typeof(bool) &&
                    ExpressionValueExtractor.TryGetConstantValue(binary.Right, out var boolValue) &&
                    boolValue is bool expected)
                    return expected
                        ? comparedPath
                        : string.Concat(comparedPath, ":BOOL_FALSE");

                if (IsNullConstant(binary.Right))
                    return string.Concat(comparedPath, ":NULL");

                return string.Concat(comparedPath, ":EQ");
            }

            return lambda.Body.ToString();
        }

        private bool TryGetSimpleQuery(Expression expr, out string sql, out Dictionary<string, object> parameters, out string? resultMethodName)
        {
            sql = string.Empty;
            parameters = _emptyParams;
            resultMethodName = null;
            if (_ctx.Options.GlobalFilters.Count > 0 || _ctx.Options.TenantProvider != null)
                return false;
            // Traverse to find root query and optional predicate (a single Where,
            // or the First/FirstOrDefault predicate overload -- same slot, since
            // First(source, predicate) is Where(source, predicate).First()).
            LambdaExpression? whereLambda = null;
            Expression current = expr;
            // Unwrap result operators like First/Single/Take and skip AsNoTracking
            while (current is MethodCallExpression mc)
            {
                if (mc.Method.DeclaringType == typeof(Queryable))
                {
                    if (mc.Method.Name == nameof(Queryable.Where))
                    {
                        if (whereLambda != null) return false; // only support a single predicate
                        whereLambda = StripQuotes(mc.Arguments[1]) as LambdaExpression;
                        if (whereLambda == null) return false;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault))
                    {
                        if (mc.Arguments.Count > 1)
                        {
                            // First(source, predicate) folds into the same single-predicate
                            // slot Where uses. The traversal sees First BEFORE an upstream
                            // Where, so a later Where hitting the occupied slot declines --
                            // two predicates go to the full translator.
                            whereLambda = StripQuotes(mc.Arguments[1]) as LambdaExpression;
                            if (whereLambda == null) return false;
                        }
                        resultMethodName = mc.Method.Name;
                        current = mc.Arguments[0];
                    }
                    else if (mc.Method.Name is nameof(Queryable.Take))
                    {
                        // This fast path never emits a LIMIT/TOP, so accepting Take would silently
                        // return the WHOLE table instead of the first n rows. Fall through to the full
                        // translator, which applies the row limit correctly.
                        return false;
                    }
                    else if (mc.Method.Name is nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault))
                    {
                        return false;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (mc.Method.Name == "AsNoTracking" && mc.Arguments.Count >= 1)
                {
                    // Skip AsNoTracking (nORM-specific method, not on Queryable)
                    current = mc.Arguments[0];
                }
                else
                {
                    return false;
                }
            }
            if (current is not ConstantExpression constant)
                return false;
            var elementType = GetElementType(constant);
            var map = _ctx.GetMapping(elementType);
            var whereKey = whereLambda == null ? "" : BuildSimpleWhereCacheKey(whereLambda);
            var cacheKey = string.Concat("SIMPLE:", elementType.FullName, ":", resultMethodName ?? "", ":", whereKey);
            if (!_simpleSqlCache.TryGetValue(cacheKey, out var cachedSql))
            {
                // Use string interpolation instead of StringBuilder for small queries;
                // StringBuilder overhead is not worth it for simple SELECT statements.
                var columnList = string.Join(", ", map.Columns.Select(c => c.EscCol));
                string whereClause = "";

                if (whereLambda != null)
                {
                    var lambda = whereLambda;
                    // Support boolean member: u => u.IsActive
                    if (lambda.Body is MemberExpression boolMember && boolMember.Type == typeof(bool))
                    {
                        if (!map.TryGetColumnForMemberAccess(boolMember, out var boolCol) || boolCol.Converter != null)
                            return false;
                        whereClause = $" WHERE {_ctx.RawProvider.FormatBooleanPredicate(boolCol.EscCol, expectedValue: true)}";
                    }
                    // Support negated boolean member: u => !u.IsActive
                    else if (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr
                             && notExpr.Operand is MemberExpression negBoolMember
                             && negBoolMember.Type == typeof(bool))
                    {
                        if (!map.TryGetColumnForMemberAccess(negBoolMember, out var boolCol) || boolCol.Converter != null)
                            return false;
                        whereClause = $" WHERE {_ctx.RawProvider.FormatBooleanPredicate(boolCol.EscCol, expectedValue: false)}";
                    }
                    else
                    {
                        if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                            return false;
                        if (be.Left is not MemberExpression me)
                            return false;
                        if (!map.TryGetColumnForMemberAccess(me, out var column))
                            return false;
                        // A value converter changes the stored representation, but this fast path binds the
                        // RAW model value (col = @p with the unconverted value), so it would silently match
                        // nothing. Defer converter columns to the full translator, which applies the converter.
                        if (column.Converter != null)
                            return false;
                        // Use ExpressionValueExtractor instead of Compile().DynamicInvoke();
                        // DynamicInvoke is significantly slower and poses RCE risks.
                        if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                            return false;
                        if (me.Type == typeof(bool) && value is bool boolValue)
                        {
                            whereClause = $" WHERE {_ctx.RawProvider.FormatBooleanPredicate(column.EscCol, boolValue)}";
                        }
                        // Null value: emit IS NULL (SQL "col = NULL" is always UNKNOWN/false).
                        else if (value == null || value == DBNull.Value)
                        {
                            whereClause = $" WHERE {column.EscCol} IS NULL";
                            // no parameters needed
                        }
                        else
                        {
                            var paramName = _ctx.RawProvider.ParamPrefix + "p0";
                            // C# string/char equality is ordinal; CI-collation providers (MySQL,
                            // SQL Server) need the sargable ordinal wrap here too, or this fast
                            // path would match different rows than the full translator.
                            var colClrType = Nullable.GetUnderlyingType(me.Type) ?? me.Type;
                            whereClause = (colClrType == typeof(string) || colClrType == typeof(char))
                                          && _ctx.RawProvider.DefaultStringEqualityIsCaseInsensitive
                                ? $" WHERE {_ctx.RawProvider.OrdinalStringEqualSql(column.EscCol, paramName)}"
                                : $" WHERE {column.EscCol} = {paramName}";
                            parameters = new Dictionary<string, object>(1) { [paramName] = value };
                        }
                    }
                }

                sql = $"SELECT {columnList} FROM {map.EscTable}{whereClause}";
                _simpleSqlCache[cacheKey] = sql;
                return true;
            }
            sql = cachedSql!;
            // SQL cached; still need parameter value if a predicate is present
            if (whereLambda != null)
            {
                var lambda = whereLambda;
                if ((lambda.Body is MemberExpression boolMember2 && boolMember2.Type == typeof(bool))
                    || (lambda.Body is UnaryExpression { NodeType: ExpressionType.Not } notExpr3
                        && notExpr3.Operand is MemberExpression negBm && negBm.Type == typeof(bool)))
                {
                    // no parameter to bind for boolean literal predicate
                }
                else
                {
                    if (lambda.Body is not BinaryExpression be || be.NodeType != ExpressionType.Equal)
                        return false;
                    if (!ExpressionValueExtractor.TryGetConstantValue(be.Right, out var value))
                        return false;
                    // Boolean literals and NULL predicates are part of the SQL cache key and need no parameter.
                    var isBoolLiteralPredicate = be.Left is MemberExpression { Type: var memberType }
                                                 && memberType == typeof(bool)
                                                 && value is bool;
                    if (!isBoolLiteralPredicate && value != null && value != DBNull.Value)
                    {
                        var paramName = _ctx.RawProvider.ParamPrefix + "p0";
                        parameters = new Dictionary<string, object>(1) { [paramName] = value };
                    }
                }
            }
            return true;
        }

        private async Task<TResult> ExecuteSimpleAsync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName, CancellationToken ct)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            await _ctx.EnsureConnectionAsync(ct).ConfigureAwait(false);
            await using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = await _executor.MaterializeAsync(plan, cmd, ct).ConfigureAwait(false);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }
        private TResult ExecuteSimpleSync<TResult>(string sql, Dictionary<string, object> parameters, string? requestedMethodName = null)
        {
            var sw = _ctx.Options.Logger != null ? Stopwatch.StartNew() : null;
            Type resultType = typeof(TResult);
            bool returnsList = false;
            Type elementType;
            if (resultType.IsGenericType)
            {
                var genDef = resultType.GetGenericTypeDefinition();
                if (genDef == typeof(List<>) || genDef == typeof(IEnumerable<>))
                {
                    elementType = resultType.GetGenericArguments()[0];
                    returnsList = true;
                }
                else
                {
                    elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
                }
            }
            else
            {
                elementType = resultType.IsArray ? resultType.GetElementType()! : resultType;
            }
            var mapping = _ctx.GetMapping(elementType);

            // Use singleton MaterializerFactory (it only wraps static caches)
            var materializer = _sharedMaterializerFactory.CreateMaterializer(mapping, elementType);
            var syncMaterializer = _sharedMaterializerFactory.CreateSyncMaterializer(mapping, elementType);

            var plan = new QueryPlan(
                sql,
                parameters,
                Array.Empty<string>(),
                materializer,
                syncMaterializer,
                elementType,
                IsScalar: false,
                SingleResult: !returnsList,
                NoTracking: false,
                MethodName: returnsList ? "ToList" : nameof(Queryable.FirstOrDefault),
                Includes: _emptyIncludes,
                GroupJoinInfo: null,
                Tables: _simpleQueryTableCache.GetOrAdd(mapping.TableName, static t => new List<string>(1) { t }),
                SplitQuery: false,
                CommandTimeout: _ctx.Options.TimeoutConfiguration.BaseTimeout,
                IsCacheable: false,
                CacheExpiration: null
            );
            _ctx.EnsureConnection();
            using var cmd = _ctx.CreateCommand();
            cmd.CommandTimeout = (int)plan.CommandTimeout.TotalSeconds;
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.AddOptimizedParam(p.Key, p.Value);
            var list = _executor.Materialize(plan, cmd);
            sw?.Stop();
            _ctx.Options.Logger?.LogQuery(sql, parameters, sw?.Elapsed ?? default, list.Count);
            if (returnsList)
            {
                return (TResult)list;
            }
            // Preserve First vs FirstOrDefault semantics.
            if (list.Count > 0) return (TResult)list[0]!;
            if (requestedMethodName == nameof(Queryable.First))
                throw new InvalidOperationException("Sequence contains no elements");
            return default!;
        }
    }
}
