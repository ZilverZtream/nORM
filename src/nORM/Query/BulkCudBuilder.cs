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
            // Each assignment is either a bound parameter (literal/closure form) or a
            // pre-translated SQL expression (server-side computed form). Track in one list
            // and let the emit loop choose `col = @p` vs `col = <expr>`.
            var assigns = new List<(string Column, string? ParamName, object? ParamValue, string? ExpressionSql)>();
            var call = set.Body as MethodCallExpression;
            int paramIndex = 0;
            var parameters = new Dictionary<string, object>();
            while (call != null)
            {
                var propLambda = (LambdaExpression)StripQuotes(call.Arguments[0]);
                var member = (MemberExpression)propLambda.Body;
                var column = mapping.ColumnsByName[member.Member.Name].EscCol;

                var valueArg = StripQuotes(call.Arguments[1]);
                if (valueArg is LambdaExpression valueLambda)
                {
                    // Server-side computed form: SetProperty(x => x.Counter, x => x.Counter + 1).
                    // Translate the body against the entity columns (no alias prefix — UPDATE
                    // statements reference columns directly).
                    var exprSql = TranslateUpdateValueExpression(valueLambda, mapping);
                    assigns.Add((column, null, null, exprSql));
                }
                else if (TryGetSetValue(call.Arguments[1], out var value))
                {
                    var pName = _ctx.Provider.ParamPrefix + "u" + paramIndex++;
                    parameters[pName] = value ?? DBNull.Value;
                    assigns.Add((column, pName, value, null));
                }
                else
                {
                    throw new NormUnsupportedFeatureException(
                        "ExecuteUpdate set values must be a literal/captured constant or an " +
                        "Expression<Func<T, TProperty>> evaluated server-side. The given expression " +
                        "could not be reduced to either form.");
                }
                call = call.Object as MethodCallExpression;
            }
            assigns.Reverse();
            var sb = _stringBuilderPool.Get();
            try
            {
                for (int i = 0; i < assigns.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    if (assigns[i].ExpressionSql != null)
                        sb.Append($"{assigns[i].Column} = {assigns[i].ExpressionSql}");
                    else
                        sb.Append($"{assigns[i].Column} = {assigns[i].ParamName}");
                }
                return (sb.ToString(), parameters);
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Translates an UPDATE-side value expression to SQL using unprefixed column names
        /// (UPDATE statements reference columns of the table being updated directly, with
        /// no FROM-aliased table prefix). Supports member access on the row parameter,
        /// constants (inlined or captured), arithmetic / string-concat binary operators,
        /// and primitive Convert. Anything outside that surface throws.
        /// </summary>
        private string TranslateUpdateValueExpression(LambdaExpression valueLambda, TableMapping mapping)
        {
            var rowParam = valueLambda.Parameters[0];
            return Render(valueLambda.Body);

            string Render(Expression e)
            {
                switch (e)
                {
                    case MemberExpression me when me.Expression == rowParam:
                        if (!mapping.ColumnsByName.TryGetValue(me.Member.Name, out var col))
                            throw new NormUnsupportedFeatureException(
                                $"Member '{me.Member.Name}' is not a mapped column on '{mapping.TableName}'.");
                        return col.EscCol;

                    case ConstantExpression ce:
                        return RenderLiteral(ce.Value);

                    case UnaryExpression ue when ue.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked:
                        return Render(ue.Operand);

                    case ConditionalExpression cond:
                        // Ternary -> CASE WHEN test THEN ifTrue ELSE ifFalse END.
                        return $"(CASE WHEN {RenderPredicate(cond.Test)} THEN {Render(cond.IfTrue)} ELSE {Render(cond.IfFalse)} END)";

                    case BinaryExpression be:
                        // String concat: `+` between two strings lowers to a BinaryExpression
                        // with both operand types == string. Use the provider's concat dialect
                        // (`||` on SQLite/PG, `+` on SQL Server, CONCAT on MySQL).
                        if (be.NodeType == ExpressionType.Add
                            && be.Left.Type == typeof(string)
                            && be.Right.Type == typeof(string))
                        {
                            return _ctx.Provider.GetConcatSql(Render(be.Left), Render(be.Right));
                        }
                        // Null-coalesce: `??` lowers to BinaryExpression(Coalesce). Emit COALESCE.
                        if (be.NodeType == ExpressionType.Coalesce)
                        {
                            return $"COALESCE({Render(be.Left)}, {Render(be.Right)})";
                        }
                        var op = be.NodeType switch
                        {
                            ExpressionType.Add => "+",
                            ExpressionType.Subtract => "-",
                            ExpressionType.Multiply => "*",
                            ExpressionType.Divide => "/",
                            ExpressionType.Modulo => "%",
                            _ => throw new NormUnsupportedFeatureException(
                                $"Binary operator {be.NodeType} is not supported inside a SetProperty value expression."),
                        };
                        return $"({Render(be.Left)} {op} {Render(be.Right)})";

                    case MemberExpression captured:
                        // Closure-captured constant — evaluate at translation time and inline.
                        if (nORM.Query.QueryTranslator.TryGetConstantValue(captured, out var capturedValue))
                            return RenderLiteral(capturedValue);
                        throw new NormUnsupportedFeatureException(
                            $"Cannot resolve '{captured.Member.Name}' in a SetProperty value expression.");

                    default:
                        throw new NormUnsupportedFeatureException(
                            $"Expression node {e.NodeType} is not supported inside a SetProperty value expression.");
                }
            }

            string RenderLiteral(object? value) => value switch
            {
                null => "NULL",
                string s => "'" + s.Replace("'", "''") + "'",
                bool b => b ? _ctx.Provider.BooleanTrueLiteral : _ctx.Provider.BooleanFalseLiteral,
                IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
                _ => System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
            };

            // Predicate rendering for conditional `Test` branches. Boolean operators map to
            // SQL keywords; comparisons emit raw operators. Re-uses the value-side Render for
            // operands so column references and literals work consistently.
            string RenderPredicate(Expression p)
            {
                switch (p)
                {
                    case BinaryExpression pb:
                        var pop = pb.NodeType switch
                        {
                            ExpressionType.Equal => "=",
                            ExpressionType.NotEqual => "<>",
                            ExpressionType.GreaterThan => ">",
                            ExpressionType.GreaterThanOrEqual => ">=",
                            ExpressionType.LessThan => "<",
                            ExpressionType.LessThanOrEqual => "<=",
                            ExpressionType.AndAlso or ExpressionType.And => "AND",
                            ExpressionType.OrElse or ExpressionType.Or => "OR",
                            _ => throw new NormUnsupportedFeatureException(
                                $"Predicate operator {pb.NodeType} is not supported inside a SetProperty conditional."),
                        };
                        if (pop is "AND" or "OR")
                            return $"({RenderPredicate(pb.Left)} {pop} {RenderPredicate(pb.Right)})";
                        return $"({Render(pb.Left)} {pop} {Render(pb.Right)})";

                    case UnaryExpression pn when pn.NodeType == ExpressionType.Not:
                        return $"(NOT {RenderPredicate(pn.Operand)})";

                    case MemberExpression pm when pm.Type == typeof(bool):
                        // Bare boolean column: emit `col = TRUE`.
                        return $"({Render(pm)} = {_ctx.Provider.BooleanTrueLiteral})";

                    default:
                        return Render(p);
                }
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
