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

        /// <summary>
        /// Validates that the provided SQL fragment represents a simple, non-aggregated query
        /// suitable for bulk update or delete operations. Certain constructs such as grouping,
        /// ordering and joins are explicitly disallowed because they would change the semantics of
        /// the bulk operation.
        /// </summary>
        /// <param name="sql">The SQL statement to validate.</param>
        /// <exception cref="NotSupportedException">Thrown when the statement contains unsupported constructs.</exception>
        /// <remarks>
        /// PERFORMANCE: Uses ReadOnlySpan to avoid allocating uppercase copy of entire SQL string.
        /// </remarks>
        public void ValidateCudPlan(string sql)
        {
            if (sql.AsSpan().Contains(" GROUP BY ".AsSpan(), StringComparison.OrdinalIgnoreCase) ||
                sql.AsSpan().Contains(" ORDER BY ".AsSpan(), StringComparison.OrdinalIgnoreCase) ||
                sql.AsSpan().Contains(" HAVING ".AsSpan(), StringComparison.OrdinalIgnoreCase) ||
                sql.AsSpan().Contains(" JOIN ".AsSpan(), StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException("ExecuteUpdate/Delete does not support grouped, ordered, joined or aggregated queries.");
        }

        /// <summary>
        /// Attempts to extract the <c>WHERE</c> clause from an arbitrary SQL statement while removing
        /// any table aliases that may be present. The resulting clause can be reused in generated
        /// bulk update or delete statements.
        /// </summary>
        /// <param name="sql">The original SQL statement.</param>
        /// <param name="escTable">The escaped table name used in the statement.</param>
        /// <returns>The extracted <c>WHERE</c> clause, or an empty string if no clause exists.</returns>
        /// <remarks>
        /// PERFORMANCE: Reduced string allocations by using IndexOf with OrdinalIgnoreCase
        /// instead of creating uppercase copy of entire SQL string.
        /// </remarks>
        public string ExtractWhereClause(string sql, string escTable)
        {
            var fromPattern = "FROM " + escTable;
            var fromIndex = sql.IndexOf(fromPattern, StringComparison.OrdinalIgnoreCase);
            string? alias = null;
            if (fromIndex >= 0)
            {
                var after = sql.Substring(fromIndex + fromPattern.Length);
                var tokens = after.TrimStart().Split(new[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length > 0)
                    alias = tokens[0];
            }
            var whereIndex = sql.IndexOf(" WHERE", StringComparison.OrdinalIgnoreCase);
            if (whereIndex < 0) return string.Empty;
            var where = sql.Substring(whereIndex);
            where = RemoveAliasFromWhereClause(where, alias);
            return where;
        }

        /// <summary>
        /// PERFORMANCE: Use pooled StringBuilder to reduce allocations.
        /// </summary>
        private static string RemoveAliasFromWhereClause(string where, string? alias)
        {
            var sb = _stringBuilderPool.Get();
            sb.EnsureCapacity(where.Length);
            bool inString = false;
            bool inBracket = false;

            for (int i = 0; i < where.Length; i++)
            {
                char c = where[i];

                if (!inString && c == '[')
                {
                    inBracket = true;
                }
                else if (inBracket)
                {
                    if (c == ']') inBracket = false;
                }
                else if (c == '\'')
                {
                    inString = !inString;
                }

                if (!inString && !inBracket)
                {
                    if (!string.IsNullOrEmpty(alias) &&
                        where.AsSpan(i).StartsWith(alias, StringComparison.OrdinalIgnoreCase) &&
                        i + alias.Length < where.Length && where[i + alias.Length] == '.' &&
                        (i == 0 || !IsIdentifierChar(where[i - 1])))
                    {
                        i += alias.Length; // skip alias and dot
                        continue;
                    }

                    if (c == 'T' && (i == 0 || !IsIdentifierChar(where[i - 1])))
                    {
                        int j = i + 1;
                        while (j < where.Length && char.IsDigit(where[j])) j++;
                        if (j > i + 1 && j < where.Length && where[j] == '.')
                        {
                            i = j; // skip alias and dot
                            continue;
                        }
                    }
                }

                sb.Append(c);
            }

            var result = sb.ToString();
            sb.Clear();
            _stringBuilderPool.Return(sb);
            return result;
        }

        private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_' || c == '$';

        public (string Sql, Dictionary<string, object> Params) BuildSetClause<T>(TableMapping mapping, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set)
        {
            var assigns = new List<(string Column, object? Value)>();
            var call = set.Body as MethodCallExpression;
            while (call != null)
            {
                var lambda = (LambdaExpression)StripQuotes(call.Arguments[0]);
                var member = (MemberExpression)lambda.Body;
                var column = mapping.ColumnsByName[member.Member.Name].EscCol;
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
