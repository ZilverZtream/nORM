using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
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
        private static string ApplyLimit(string sql, int limit, DatabaseProvider provider)
        {
            bool isSqlServer = provider.GetType().Name.Contains("SqlServer", StringComparison.OrdinalIgnoreCase);
            if (isSqlServer)
            {
                return sql.Replace("SELECT ", $"SELECT TOP({limit}) ");
            }
            else
            {
                return sql + $" LIMIT {limit}";
            }
        }
        private static async Task<List<T>> ExecuteSimpleWhere<T>(DbContext ctx, WhereInfo info, int? takeCount) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            if (!map.ColumnsByName.TryGetValue(info.Property, out var column))
                throw new InvalidOperationException("Fast path failed - unknown column");
            string sql = GetSqlTemplate<T>(ctx);
            if (info.Value == null || info.Value == DBNull.Value)
            {
                sql += $" WHERE {column.EscCol} IS NULL";
            }
            else
            {
                sql += $" WHERE {column.EscCol} = {ctx.Provider.ParamPrefix}p0";
            }
            if (takeCount.HasValue)
            {
                sql = ApplyLimit(sql, takeCount.Value, ctx.Provider);
            }
            await ctx.EnsureConnectionAsync(default).ConfigureAwait(false);
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.CommandText = sql;
            if (info.Value != null && info.Value != DBNull.Value)
            {
                cmd.AddOptimizedParam(ctx.Provider.ParamPrefix + "p0", info.Value);
            }
            var results = new List<T>();
            var materializer = new MaterializerFactory().CreateMaterializer(map, typeof(T));
            await using var reader = await cmd.ExecuteReaderAsync(System.Threading.CancellationToken.None).ConfigureAwait(false);
            while (await reader.ReadAsync(default).ConfigureAwait(false))
            {
                results.Add((T)await materializer(reader, default).ConfigureAwait(false));
            }
            return results;
        }
        private static async Task<List<T>> ExecuteSimpleTake<T>(DbContext ctx, int? takeCount) where T : class, new()
        {
            var map = ctx.GetMapping(typeof(T));
            string sql = GetSqlTemplate<T>(ctx);
            if (takeCount.HasValue)
            {
                sql = ApplyLimit(sql, takeCount.Value, ctx.Provider);
            }
            await ctx.EnsureConnectionAsync(default).ConfigureAwait(false);
            await using var cmd = ctx.Connection.CreateCommand();
            cmd.CommandText = sql;
            var results = new List<T>();
            var materializer = new MaterializerFactory().CreateMaterializer(map, typeof(T));
            await using var reader = await cmd.ExecuteReaderAsync(System.Threading.CancellationToken.None).ConfigureAwait(false);
            while (await reader.ReadAsync(default).ConfigureAwait(false))
            {
                results.Add((T)await materializer(reader, default).ConfigureAwait(false));
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
            return result!;
        }
    }
}