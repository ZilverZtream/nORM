using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
        /// Validates a bulk update/delete query from structural translation metadata
        /// instead of scanning generated SQL text.
        /// </summary>
        /// <param name="shape">The query shape captured during LINQ translation.</param>
        /// <exception cref="NormUnsupportedFeatureException">Thrown when the query shape is unsupported for bulk CUD.</exception>
        public void ValidateCudPlan(BulkCudQueryShape? shape)
        {
            if (shape == null)
                throw new NormUnsupportedFeatureException("ExecuteUpdate/Delete requires query-shape metadata.");

            if (shape.HasGroupBy || shape.HasOrderBy || shape.HasHaving || shape.HasJoins || shape.HasDistinct || shape.HasPaging)
                throw new NormUnsupportedFeatureException("ExecuteUpdate/Delete does not support grouped, ordered, joined, distinct, paged or aggregated queries.");
        }

        /// <summary>
        /// Returns the structurally captured WHERE clause for a bulk CUD statement.
        /// </summary>
        /// <param name="shape">The query shape captured during LINQ translation.</param>
        /// <returns>The normalized WHERE clause, or an empty string when the query has no predicate.</returns>
        public string GetWhereClause(BulkCudQueryShape? shape)
        {
            if (shape == null || string.IsNullOrEmpty(shape.WhereClause))
                return string.Empty;

            return RemoveAliasFromWhereClause(shape.WhereClause, alias: null);
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

                if (!inString && !inBracket)
                {
                    if (TrySkipDelimitedAlias(where, i, '"', out var quotedAliasEnd) ||
                        TrySkipDelimitedAlias(where, i, '`', out quotedAliasEnd) ||
                        TrySkipBracketedAlias(where, i, out quotedAliasEnd))
                    {
                        i = quotedAliasEnd;
                        continue;
                    }
                }

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

        private static bool TrySkipDelimitedAlias(string where, int start, char delimiter, out int dotIndex)
        {
            dotIndex = -1;
            if (start >= where.Length || where[start] != delimiter)
                return false;

            var aliasStart = start + 1;
            if (aliasStart >= where.Length || where[aliasStart] != 'T')
                return false;

            var aliasEnd = aliasStart + 1;
            while (aliasEnd < where.Length && char.IsDigit(where[aliasEnd]))
                aliasEnd++;

            if (aliasEnd == aliasStart + 1 ||
                aliasEnd >= where.Length ||
                where[aliasEnd] != delimiter ||
                aliasEnd + 1 >= where.Length ||
                where[aliasEnd + 1] != '.')
            {
                return false;
            }

            dotIndex = aliasEnd + 1;
            return true;
        }

        private static bool TrySkipBracketedAlias(string where, int start, out int dotIndex)
        {
            dotIndex = -1;
            if (start >= where.Length || where[start] != '[')
                return false;

            var aliasStart = start + 1;
            if (aliasStart >= where.Length || where[aliasStart] != 'T')
                return false;

            var aliasEnd = aliasStart + 1;
            while (aliasEnd < where.Length && char.IsDigit(where[aliasEnd]))
                aliasEnd++;

            if (aliasEnd == aliasStart + 1 ||
                aliasEnd >= where.Length ||
                where[aliasEnd] != ']' ||
                aliasEnd + 1 >= where.Length ||
                where[aliasEnd + 1] != '.')
            {
                return false;
            }

            dotIndex = aliasEnd + 1;
            return true;
        }

        public (string Sql, Dictionary<string, object> Params) BuildSetClause<T>(TableMapping mapping, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> set)
        {
            if (set == null) throw new ArgumentNullException(nameof(set));
            var assigns = new List<(string Column, object? Value)>();
            var call = set.Body as MethodCallExpression;
            while (call != null)
            {
                var lambda = (LambdaExpression)StripQuotes(call.Arguments[0]);
                var member = (MemberExpression)lambda.Body;
                var column = mapping.ColumnsByName[member.Member.Name].EscCol;
                if (!TryGetSetValue(call.Arguments[1], out var value))
                    throw new NormUnsupportedFeatureException("ExecuteUpdate set values must be literal constants or precomputed captured local values. Method calls, inline computed values, column-based updates and server expressions are not supported in v1; compute the value before calling ExecuteUpdateAsync.");
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

        private static bool TryGetSetValue(Expression expression, out object? value)
        {
            expression = StripConvert(expression);
            switch (expression)
            {
                case ConstantExpression constant:
                    value = constant.Value;
                    return true;

                case MemberExpression { Member: FieldInfo field } member:
                    if (member.Expression == null)
                    {
                        value = field.GetValue(null);
                        return true;
                    }

                    if (TryGetSetValue(member.Expression, out var owner))
                    {
                        value = field.GetValue(owner);
                        return true;
                    }

                    break;
            }

            value = null;
            return false;
        }

        private static Expression StripConvert(Expression expression)
        {
            while (expression is UnaryExpression unary &&
                   (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                expression = unary.Operand;
            }

            return expression;
        }
    }
}
