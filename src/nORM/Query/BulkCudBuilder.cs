using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using nORM.Core;
using nORM.Mapping;
using Microsoft.Extensions.ObjectPool;

namespace nORM.Query
{
    /// <summary>
    /// Builds SQL fragments for bulk update and delete operations.
    /// </summary>
    internal sealed class BulkCudBuilder
    {
        private readonly DbContext _ctx;

        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public BulkCudBuilder(DbContext ctx) => _ctx = ctx;

        public void ValidateCudPlan(string sql)
        {
            if (sql.IndexOf(" GROUP BY ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" HAVING ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                sql.IndexOf(" JOIN ", StringComparison.OrdinalIgnoreCase) >= 0)
                throw new NotSupportedException("ExecuteUpdate/Delete does not support grouped, ordered, joined or aggregated queries.");
        }

        public string ExtractWhereClause(string sql, string escTable)
        {
            var upper = sql.ToUpperInvariant();
            var fromIndex = upper.IndexOf("FROM " + escTable.ToUpperInvariant(), StringComparison.Ordinal);
            string? alias = null;
            if (fromIndex >= 0)
            {
                var after = sql.Substring(fromIndex + ("FROM " + escTable).Length);
                var tokens = after.TrimStart().Split(new[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length > 0)
                    alias = tokens[0];
            }
            var whereIndex = upper.IndexOf(" WHERE", StringComparison.Ordinal);
            if (whereIndex < 0) return string.Empty;
            var where = sql.Substring(whereIndex);
            if (!string.IsNullOrEmpty(alias))
                where = where.Replace(alias + ".", "");
            return where;
        }

        public (string Sql, Dictionary<string, object> Params) BuildSetClause<T>(TableMapping mapping, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set)
        {
            var assigns = new List<(string Column, object? Value)>();
            var call = set.Body as MethodCallExpression;
            while (call != null)
            {
                var lambda = (LambdaExpression)StripQuotes(call.Arguments[0]);
                var member = (MemberExpression)lambda.Body;
                var column = mapping.Columns.First(c => c.Prop.Name == member.Member.Name).EscCol;
                var value = Expression.Lambda(call.Arguments[1]).Compile().DynamicInvoke();
                assigns.Add((column, value));
                call = call.Object as MethodCallExpression;
            }
            assigns.Reverse();
            var sb = _stringBuilderPool.Get();
            try
            {
                var parameters = new Dictionary<string, object>();
                for (int i = 0; i < assigns.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    var pName = _ctx.Provider.ParamPrefix + "u" + i;
                    sb.Append($"{assigns[i].Column} = {pName}");
                    parameters[pName] = assigns[i].Value ?? DBNull.Value;
                }
                return (sb.ToString(), parameters);
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote) e = ((UnaryExpression)e).Operand;
            return e;
        }
    }
}
