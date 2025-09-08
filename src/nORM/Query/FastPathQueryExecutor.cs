using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal static class FastPathQueryExecutor
    {
        private static readonly ConcurrentDictionary<int, string> _sqlTemplateCache = new();

        private readonly record struct WhereInfo(string Property, object Value);

        public static bool TryExecute<T>(Expression expr, DbContext ctx, out Task<object> result) where T : class, new()
        {
            result = default!;

            if (ctx.Options.GlobalFilters.Count > 0 || ctx.Options.TenantProvider != null)
                return false;

            if (IsSimpleCountPattern(expr, out var hasPredicate))
            {
                if (hasPredicate)
                {
                    return false;
                }

                result = ExecuteSimpleCount<T>(ctx);
                return true;
            }

            if (IsSimpleWherePattern(expr, out var whereInfo, out var takeCount))
            {
                result = ExecuteSimpleWhere<T>(ctx, whereInfo, takeCount).ContinueWith(t => (object)t.Result);
                return true;
            }

            if (IsSimpleTakePattern(expr, out takeCount))
            {
                result = ExecuteSimpleTake<T>(ctx, takeCount).ContinueWith(t => (object)t.Result);
                return true;
            }

            return false;
        }

        private static bool IsSimpleCountPattern(Expression expr, out bool hasPredicate)
        {
            hasPredicate = false;
            if (expr is not MethodCallExpression countCall ||
                (countCall.Method.Name != nameof(Queryable.Count) && countCall.Method.Name != nameof(Queryable.LongCount)))
            {
                return false;
            }

            if (countCall.Arguments.Count == 2)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                if (countCall.Arguments[1] is not LambdaExpression) return false;
                hasPredicate = true;
                return true;
            }

            if (countCall.Arguments.Count == 1)
            {
                if (Unwrap(countCall.Arguments[0]) is not ConstantExpression) return false;
                hasPredicate = false;
                return true;
            }

            return false;
        }

        private static bool IsSimpleWherePattern(Expression expr, out WhereInfo info, out int? takeCount)
        {
            info = default;
            takeCount = null;

            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce)
                    takeCount = (int)ce.Value!;
                else
                    return false;
                expr = takeCall.Arguments[0];
            }

            if (expr is not MethodCallExpression whereCall || whereCall.Method.Name != nameof(Queryable.Where))
                return false;

            if (Unwrap(whereCall.Arguments[0]) is not ConstantExpression)
                return false;

            if (whereCall.Arguments[1] is not LambdaExpression lambda)
                return false;

            var body = lambda.Body;

            // Support boolean member access: u => u.IsActive
            if (body is MemberExpression meBoolean && meBoolean.Type == typeof(bool))
            {
                info = new WhereInfo(meBoolean.Member.Name, true);
                return true;
            }

            if (body is BinaryExpression be && be.NodeType == ExpressionType.Equal && be.Left is MemberExpression me)
            {
                try
                {
                    object value = be.Right is ConstantExpression c ? c.Value! : Expression.Lambda(be.Right).Compile().DynamicInvoke()!;
                    info = new WhereInfo(me.Member.Name, value);
                    return true;
                }
                catch
                {
                    return false;
                }
            }

            return false;
        }

        private static bool IsSimpleTakePattern(Expression expr, out int? takeCount)
        {
            takeCount = null;
            if (expr is MethodCallExpression takeCall && takeCall.Method.Name == nameof(Queryable.Take))
            {
                if (takeCall.Arguments[1] is ConstantExpression ce && Unwrap(takeCall.Arguments[0]) is ConstantExpression)
                {
                    takeCount = (int)ce.Value!;
                    return true;
                }
            }
            return false;
        }

        private static Expression Unwrap(Expression e)
        {
            while (e is MethodCallExpression m)
            {
                if (m.Method.Name == "AsNoTracking" && m.Arguments.Count == 1)
                {
                    e = m.Arguments[0];
                    continue;
                }
                break;
            }
            return e;
        }

        private static string GetSqlTemplate<T>(DbContext ctx) where T : class
        {
            var key = typeof(T).GetHashCode();
            if (!_sqlTemplateCache.TryGetValue(key, out var template))
            {
                var map = ctx.GetMapping(typeof(T));
                var cols = string.Join(", ", map.Columns.Select(c => c.EscCol));
                template = $"SELECT {cols} FROM {map.EscTable}";
                _sqlTemplateCache[key] = template;
            }
            return template;
        }

        private static async Task<List<T>> ExecuteSimpleWhere<T>(DbContext ctx, WhereInfo info, int? takeCount) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException("Fast path failed - unknown column");

            var sql = GetSqlTemplate<T>(ctx) + $" WHERE {column.EscCol} = {ctx.Provider.ParamPrefix}p0";
            if (takeCount.HasValue)
                sql += $" LIMIT {takeCount.Value}";

            await ctx.EnsureConnectionAsync(default).ConfigureAwait(false);
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.CommandText = sql;
            var param = cmd.CreateParameter();
            param.ParameterName = ctx.Provider.ParamPrefix + "p0";
            param.Value = info.Value ?? DBNull.Value;
            cmd.Parameters.Add(param);

            var results = new List<T>();
            await using var reader = await cmd.ExecuteReaderAsync(System.Threading.CancellationToken.None).ConfigureAwait(false);

            while (await reader.ReadAsync(default).ConfigureAwait(false))
            {
                var entity = new T();
                for (int i = 0; i < map.Columns.Length; i++)
                {
                    if (reader.IsDBNull(i)) continue;
                    var col = map.Columns[i];
                    var t = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                    object? v;
                    if (t == typeof(int)) v = reader.GetInt32(i);
                    else if (t == typeof(long)) v = reader.GetInt64(i);
                    else if (t == typeof(double)) v = reader.GetDouble(i);
                    else if (t == typeof(float)) v = (float)reader.GetDouble(i);
                    else if (t == typeof(bool)) v = reader.GetInt32(i) != 0;
                    else if (t == typeof(string)) v = reader.GetString(i);
                    else if (t == typeof(DateTime)) v = DateTime.Parse(reader.GetString(i), null, DateTimeStyles.RoundtripKind);
                    else v = reader.GetValue(i);
                    col.Setter(entity, v);
                }
                results.Add(entity);
            }
            return results;
        }

        private static async Task<List<T>> ExecuteSimpleTake<T>(DbContext ctx, int? takeCount) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            var sql = GetSqlTemplate<T>(ctx);
            if (takeCount.HasValue)
                sql += $" LIMIT {takeCount.Value}";

            await ctx.EnsureConnectionAsync(default).ConfigureAwait(false);
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.CommandText = sql;

            var results = new List<T>();
            await using var reader = await cmd.ExecuteReaderAsync(System.Threading.CancellationToken.None).ConfigureAwait(false);
            while (await reader.ReadAsync(default).ConfigureAwait(false))
            {
                var entity = new T();
                for (int i = 0; i < map.Columns.Length; i++)
                {
                    if (reader.IsDBNull(i)) continue;
                    var col = map.Columns[i];
                    var t = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                    object? v;
                    if (t == typeof(int)) v = reader.GetInt32(i);
                    else if (t == typeof(long)) v = reader.GetInt64(i);
                    else if (t == typeof(double)) v = reader.GetDouble(i);
                    else if (t == typeof(float)) v = (float)reader.GetDouble(i);
                    else if (t == typeof(bool)) v = reader.GetInt32(i) != 0;
                    else if (t == typeof(string)) v = reader.GetString(i);
                    else if (t == typeof(DateTime)) v = DateTime.Parse(reader.GetString(i), null, DateTimeStyles.RoundtripKind);
                    else v = reader.GetValue(i);
                    col.Setter(entity, v);
                }
                results.Add(entity);
            }
            return results;
        }

        private static async Task<object> ExecuteSimpleCount<T>(DbContext ctx) where T : class
        {
            var map = ctx.GetMapping(typeof(T));
            var sql = $"SELECT COUNT(*) FROM {map.EscTable}";

            await ctx.EnsureConnectionAsync(default).ConfigureAwait(false);
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.CommandText = sql;

            var result = await cmd.ExecuteScalarAsync(default).ConfigureAwait(false);
            return Convert.ToInt32(result);
        }
    }
}
