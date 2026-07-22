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
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
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

            // Ordering and Distinct never change an entity query's row set (rows are
            // key-unique), and paged shapes resolve their window through a keyed
            // subquery — only genuine reshapes (grouping) remain unsupported here;
            // joined shapes route through the joined builder before this check.
            if (shape.HasGroupBy || shape.HasHaving || shape.HasJoins)
                throw new NormUnsupportedFeatureException("ExecuteUpdate/Delete does not support grouped or aggregated queries.");
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

            // DELETE/UPDATE statements have no FROM alias for the target table — but bare
            // `Id` inside a correlated subquery resolves to the INNER table's Id, breaking
            // the correlation. Solution: REWRITE the outer alias T0 to the target table
            // name so `T0.Id` becomes `<EscTable>.Id` which is unambiguous in both the
            // outer and inner scopes. Inner aliases (T1, T2, ...) stay untouched.
            // The replacement string is filled in by ExecuteUpdate/ExecuteDelete after
            // they know the target table; default behavior (no replacement) strips the
            // outer alias entirely for queries with no correlated subquery.
            return RemoveAliasFromWhereClause(shape.WhereClause, alias: "T0");
        }

        /// <summary>
        /// Variant that REPLACES the outer alias with the supplied qualifier (typically the
        /// target table's escaped name) rather than stripping it. Required when the WHERE
        /// clause references the outer alias from within a correlated subquery — bare
        /// column names would otherwise rebind to the inner table.
        /// </summary>
        public string GetWhereClauseWithOuterQualifier(BulkCudQueryShape? shape, string outerQualifier)
        {
            if (shape == null || string.IsNullOrEmpty(shape.WhereClause))
                return string.Empty;
            return ReplaceAliasWithQualifier(shape.WhereClause, alias: "T0", qualifier: outerQualifier);
        }

        private static string ReplaceAliasWithQualifier(string where, string alias, string qualifier)
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
                    // Match delimited form `"T0".`, `` `T0`. ``, or `[T0].`.
                    int dot;
                    if (TrySkipSpecificDelimitedAlias(where, i, '"', alias, out dot) ||
                        TrySkipSpecificDelimitedAlias(where, i, '`', alias, out dot) ||
                        TrySkipSpecificBracketedAlias(where, i, alias, out dot))
                    {
                        sb.Append(qualifier).Append('.');
                        i = dot; // i was at opening delim, dot is at the '.', loop's i++ moves past it
                        continue;
                    }
                    // Bare `T0.`
                    if (where.AsSpan(i).StartsWith(alias, StringComparison.OrdinalIgnoreCase) &&
                        i + alias.Length < where.Length && where[i + alias.Length] == '.' &&
                        (i == 0 || !IsIdentifierChar(where[i - 1])))
                    {
                        sb.Append(qualifier).Append('.');
                        i += alias.Length;
                        continue;
                    }
                }
                if (!inString && c == '[') inBracket = true;
                else if (inBracket && c == ']') inBracket = false;
                else if (!inBracket && c == '\'') inString = !inString;
                sb.Append(c);
            }
            var result = sb.ToString();
            sb.Clear();
            _stringBuilderPool.Return(sb);
            return result;
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

                if (!inString && !inBracket && !string.IsNullOrEmpty(alias))
                {
                    // Strip ONLY the specified outer alias, in delimited or bare form. Inner
                    // aliases (T1, T2, ...) in correlated subqueries stay intact.
                    if (TrySkipSpecificDelimitedAlias(where, i, '"', alias, out var quotedAliasEnd) ||
                        TrySkipSpecificDelimitedAlias(where, i, '`', alias, out quotedAliasEnd) ||
                        TrySkipSpecificBracketedAlias(where, i, alias, out quotedAliasEnd))
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

                if (!inString && !inBracket && !string.IsNullOrEmpty(alias))
                {
                    if (where.AsSpan(i).StartsWith(alias, StringComparison.OrdinalIgnoreCase) &&
                        i + alias.Length < where.Length && where[i + alias.Length] == '.' &&
                        (i == 0 || !IsIdentifierChar(where[i - 1])))
                    {
                        i += alias.Length; // skip alias and dot
                        continue;
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

        /// <summary>
        /// Matches a delimited alias of EXACTLY the given name followed by '.', e.g. `"T0".`
        /// or `\`T0\`.`. Returns the index of the '.' on match.
        /// </summary>
        private static bool TrySkipSpecificDelimitedAlias(string where, int start, char delimiter, string alias, out int dotIndex)
        {
            dotIndex = -1;
            if (start >= where.Length || where[start] != delimiter) return false;
            var aliasStart = start + 1;
            if (aliasStart + alias.Length > where.Length) return false;
            if (string.Compare(where, aliasStart, alias, 0, alias.Length, StringComparison.OrdinalIgnoreCase) != 0)
                return false;
            var aliasEnd = aliasStart + alias.Length;
            if (aliasEnd >= where.Length || where[aliasEnd] != delimiter) return false;
            if (aliasEnd + 1 >= where.Length || where[aliasEnd + 1] != '.') return false;
            dotIndex = aliasEnd + 1;
            return true;
        }

        /// <summary>
        /// Matches a bracketed alias of EXACTLY the given name followed by '.', e.g. `[T0].`.
        /// </summary>
        private static bool TrySkipSpecificBracketedAlias(string where, int start, string alias, out int dotIndex)
        {
            dotIndex = -1;
            if (start >= where.Length || where[start] != '[') return false;
            var aliasStart = start + 1;
            if (aliasStart + alias.Length > where.Length) return false;
            if (string.Compare(where, aliasStart, alias, 0, alias.Length, StringComparison.OrdinalIgnoreCase) != 0)
                return false;
            var aliasEnd = aliasStart + alias.Length;
            if (aliasEnd >= where.Length || where[aliasEnd] != ']') return false;
            if (aliasEnd + 1 >= where.Length || where[aliasEnd + 1] != '.') return false;
            dotIndex = aliasEnd + 1;
            return true;
        }

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
                if (!mapping.TryGetColumnForMemberAccess(member, out var targetColumn))
                    throw new NormQueryException(
                        $"Property path '{member}' on type '{mapping.Type.Name}' is not mapped to a database column.");
                // The tenant column is identity-defining: setting it via ExecuteUpdate would move the
                // matched rows into another tenant (the WHERE clause scopes to the current tenant, but
                // the SET rewrites the tenant), silently removing them from the current tenant's data.
                // Reject it, mirroring the tracked/direct write paths.
                if (_ctx.Options.TenantProvider != null && mapping.TenantColumn != null
                    && string.Equals(targetColumn.Name, mapping.TenantColumn.Name, StringComparison.Ordinal))
                    throw new NormQueryException(
                        $"Cannot set the tenant column '{targetColumn.Name}' via ExecuteUpdate: it would move " +
                        "the matched rows into another tenant. A row's tenant is immutable under tenant isolation.");
                var column = targetColumn.EscCol;

                var valueArg = StripQuotes(call.Arguments[1]);
                if (valueArg is LambdaExpression valueLambda)
                {
                    // A value body that does not reference the row parameter is a client-side value (a
                    // captured constant, or a ternary/arithmetic over captured values). Evaluate it and route
                    // through the literal path so the target column's value converter is applied and the value
                    // is PARAMETERIZED. Inlining such a value via IFormattable renders a DateTime/DateTimeOffset/
                    // Guid/TimeSpan unquoted or culture-formatted (invalid SQL or lost sub-second precision) and
                    // an enum as its unquoted name (invalid or silently wrong), and never applies the converter.
                    if (!BodyReferencesRow(valueLambda.Body, valueLambda.Parameters[0]))
                    {
                        object? literalValue;
                        try
                        {
                            literalValue = System.Linq.Expressions.Expression.Lambda(valueLambda.Body).Compile().DynamicInvoke();
                        }
                        catch (Exception ex) when (ex is InvalidOperationException or System.Reflection.TargetInvocationException)
                        {
                            throw new NormUnsupportedFeatureException(
                                "The SetProperty value could not be evaluated. Provide a literal, a captured value, " +
                                "or an Expression<Func<T, TProperty>> over the row's own columns.", ex);
                        }
                        if (literalValue != null && targetColumn.Converter != null)
                            literalValue = targetColumn.Converter.ConvertToProvider(literalValue);
                        var pName = _ctx.RawProvider.ParamPrefix + "u" + paramIndex++;
                        parameters[pName] = literalValue ?? DBNull.Value;
                        assigns.Add((column, pName, literalValue, null));
                    }
                    else
                    {
                        // Server-side computed form: SetProperty(x => x.Counter, x => x.Counter + 1).
                        // Translate the body against the entity columns (no alias prefix — UPDATE
                        // statements reference columns directly).
                        var exprSql = TranslateUpdateValueExpression(valueLambda, mapping, parameters, targetColumn, ref paramIndex);
                        assigns.Add((column, null, null, exprSql));
                    }
                }
                else if (TryGetSetValue(call.Arguments[1], out var value))
                {
                    // Apply the column's value converter so the SET clause persists the provider
                    // representation, matching the tracked write path. Skipping it silently stores the
                    // raw model value (e.g. an enum's underlying int instead of its mapped name).
                    if (value != null && targetColumn.Converter != null)
                        value = targetColumn.Converter.ConvertToProvider(value);
                    var pName = _ctx.RawProvider.ParamPrefix + "u" + paramIndex++;
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

        // Detects whether a SetProperty value body references the row parameter. A body that does not
        // (a captured constant, or a ternary/arithmetic over captured values) is a client-side value and
        // must be evaluated and bound as a parameter, not translated to column SQL.
        private static bool BodyReferencesRow(Expression body, ParameterExpression rowParam)
        {
            var found = false;
            new RowParameterFinder(rowParam, () => found = true).Visit(body);
            return found;
        }

        private sealed class RowParameterFinder : ExpressionVisitor
        {
            private readonly ParameterExpression _target;
            private readonly Action _onFound;
            public RowParameterFinder(ParameterExpression target, Action onFound) { _target = target; _onFound = onFound; }
            protected override Expression VisitParameter(ParameterExpression node)
            { if (node == _target) _onFound(); return node; }
        }

        /// <summary>
        /// Translates an UPDATE-side value expression to SQL using unprefixed column names
        /// (UPDATE statements reference columns of the table being updated directly, with
        /// no FROM-aliased table prefix). Supports member access on the row parameter,
        /// constants (inlined or captured), arithmetic / string-concat binary operators,
        /// and primitive Convert. Anything outside that surface throws.
        /// </summary>
        private string TranslateUpdateValueExpression(LambdaExpression valueLambda, TableMapping mapping,
            Dictionary<string, object> parameters, Column targetColumn, ref int paramIndex)
        {
            var rowParam = valueLambda.Parameters[0];
            // ref params cannot be captured by the local functions below, so mutate a local copy and write
            // it back after rendering. valuePosition tracks whether a rendered node is the directly-assigned
            // value (the body itself, or a ternary/coalesce branch) as opposed to an arithmetic/function
            // operand — the target column's value converter only applies to the former.
            int localParamIndex = paramIndex;
            var rendered = Render(valueLambda.Body, valuePosition: true);
            paramIndex = localParamIndex;
            return rendered;

            string Render(Expression e, bool valuePosition)
            {
                switch (e)
                {
                    case MemberExpression me when TableMapping.TryGetMemberAccessRoot(me, out var rootParam) && rootParam == rowParam:
                        if (!mapping.TryGetColumnForMemberAccess(me, out var col))
                            throw new NormUnsupportedFeatureException(
                                $"Member '{me.Member.Name}' is not a mapped column on '{mapping.TableName}'.");
                        return col.EscCol;

                    case ConstantExpression ce:
                        return RenderLiteral(ce.Value, valuePosition);

                    case UnaryExpression ue when ue.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked:
                        return Render(ue.Operand, valuePosition);

                    case ConditionalExpression cond:
                        // Ternary -> CASE WHEN test THEN ifTrue ELSE ifFalse END. The branches are the
                        // assigned value, so they inherit the current value position.
                        return $"(CASE WHEN {RenderPredicate(cond.Test)} THEN {Render(cond.IfTrue, valuePosition)} ELSE {Render(cond.IfFalse, valuePosition)} END)";

                    case MethodCallExpression mc when mc.Method.DeclaringType is { } dt &&
                        (dt == typeof(Math) || dt == typeof(string) || dt == typeof(Convert)):
                        // Delegate Math.*, string.*, and Convert.* to the provider's
                        // TranslateFunction so SetProperty value expressions can use
                        // server-side functions like Math.Abs / Math.Min / string.Trim.
                        var fnArgs = new string[mc.Arguments.Count + (mc.Object != null ? 1 : 0)];
                        int ai = 0;
                        if (mc.Object != null) fnArgs[ai++] = Render(mc.Object, valuePosition: false);
                        for (int i = 0; i < mc.Arguments.Count; i++) fnArgs[ai++] = Render(mc.Arguments[i], valuePosition: false);
                        var fnSql = _ctx.RawProvider.TranslateFunction(mc.Method.Name, dt, fnArgs);
                        if (fnSql == null)
                            throw new NormUnsupportedFeatureException(
                                $"{dt.Name}.{mc.Method.Name}({mc.Arguments.Count} args) is not translatable on provider {_ctx.RawProvider.GetType().Name}.");
                        return fnSql;

                    case BinaryExpression be:
                        // String concat: `+` between two strings lowers to a BinaryExpression
                        // with both operand types == string. Use the provider's concat dialect
                        // (`||` on SQLite/PG, `+` on SQL Server, CONCAT on MySQL).
                        if (be.NodeType == ExpressionType.Add
                            && be.Left.Type == typeof(string)
                            && be.Right.Type == typeof(string))
                        {
                            return _ctx.RawProvider.GetNullSafeConcatSql(Render(be.Left, valuePosition: false), Render(be.Right, valuePosition: false));
                        }
                        // Null-coalesce: `??` lowers to BinaryExpression(Coalesce). Emit COALESCE. Both
                        // operands can be the assigned value (col ?? fallback), so keep the value position.
                        if (be.NodeType == ExpressionType.Coalesce)
                        {
                            return $"COALESCE({Render(be.Left, valuePosition)}, {Render(be.Right, valuePosition)})";
                        }
                        var op = be.NodeType switch
                        {
                            ExpressionType.Add => "+",
                            ExpressionType.Subtract => "-",
                            ExpressionType.Multiply => "*",
                            // C# integer division truncates; MySQL's / yields a decimal (which
                            // would then ROUND on assignment to an int column — corrupted write),
                            // so integral-typed division uses the provider's integer-division
                            // operator (DIV there, / elsewhere).
                            ExpressionType.Divide when nORM.Providers.DatabaseProvider.IsIntegralArithmeticType(be.Type)
                                => _ctx.RawProvider.IntegerDivisionOperator,
                            ExpressionType.Divide => "/",
                            ExpressionType.Modulo => "%",
                            // All four supported providers accept native << and >>
                            // bitshift operators with the same semantics as .NET,
                            // so emit directly rather than forcing the deprecated
                            // multiply-rewrite workaround.
                            ExpressionType.LeftShift => "<<",
                            ExpressionType.RightShift => ">>",
                            _ => throw new NormUnsupportedFeatureException(
                                $"Binary operator '{be.NodeType}' has no portable SQL equivalent inside a " +
                                "SetProperty value expression. For Power, use `Math.Pow(x, n)` which lowers " +
                                "to the provider's POWER / POW function."),
                        };
                        return $"({Render(be.Left, valuePosition: false)} {op} {Render(be.Right, valuePosition: false)})";

                    case MemberExpression captured:
                        // Closure-captured constant — evaluate at translation time.
                        if (nORM.Query.QueryTranslator.TryGetConstantValue(captured, out var capturedValue))
                            return RenderLiteral(capturedValue, valuePosition);
                        throw new NormUnsupportedFeatureException(
                            $"Cannot resolve '{captured.Member.Name}' in a SetProperty value expression.");

                    case MethodCallExpression navAgg
                        when (navAgg.Method.DeclaringType == typeof(Enumerable) || navAgg.Method.DeclaringType == typeof(Queryable))
                          && navAgg.Method.Name is nameof(Enumerable.Sum) or nameof(Enumerable.Count) or nameof(Enumerable.LongCount)
                                                or nameof(Enumerable.Min) or nameof(Enumerable.Max) or nameof(Enumerable.Average)
                          && navAgg.Arguments.Count >= 1
                          && navAgg.Arguments[0] is MemberExpression navAggMember
                          && navAggMember.Expression == rowParam
                          && mapping.Relations.TryGetValue(navAggMember.Member.Name, out var navRelation):
                        // Navigation-collection aggregate inside a SET clause (e.g.
                        // `SetProperty(r => r.Total, r => r.Items.Sum(i => i.Amount))`) emits a
                        // correlated subquery against the dependent table, correlated to the outer
                        // row being updated.
                        return RenderNavigationAggregate(navAgg, navAggMember, navRelation);

                    default:
                        throw new NormUnsupportedFeatureException(
                            $"Expression node {e.NodeType} is not supported inside a SetProperty value expression.");
                }
            }

            string RenderLiteral(object? value, bool valuePosition)
            {
                // In value position (the assigned value or a ternary/coalesce branch), a converter column
                // stores the provider representation — apply the converter before rendering, matching the
                // tracked and literal write paths. Not applied to arithmetic/function operands.
                if (valuePosition && value != null && targetColumn.Converter != null)
                    value = targetColumn.Converter.ConvertToProvider(value);
                switch (value)
                {
                    case null:
                        return "NULL";
                    case string s:
                        return "'" + s.Replace("'", "''") + "'";
                    case bool b:
                        return b ? _ctx.RawProvider.BooleanTrueLiteral : _ctx.RawProvider.BooleanFalseLiteral;
                    // Numeric types inline as valid, culture-invariant SQL literals.
                    case byte or sbyte or short or ushort or int or uint or long or ulong or decimal or double or float:
                        return ((IFormattable)value).ToString(null, System.Globalization.CultureInfo.InvariantCulture);
                    // A plain enum column stores the underlying integral value (a converter, if any, was
                    // applied above and would have produced a string/number, not an Enum).
                    case Enum en:
                        var underlying = System.Convert.ChangeType(
                            en, Enum.GetUnderlyingType(en.GetType()), System.Globalization.CultureInfo.InvariantCulture);
                        return ((IFormattable)underlying!).ToString(null, System.Globalization.CultureInfo.InvariantCulture);
                    // DateTime/DateTimeOffset/Guid/TimeSpan/char/byte[]/etc. cannot be inlined safely —
                    // parameterize so the provider binds the correct typed value (quoting, precision, layout).
                    default:
                        var pName = _ctx.RawProvider.ParamPrefix + "u" + localParamIndex++;
                        parameters[pName] = value;
                        return pName;
                }
            }

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
                        return $"({Render(pb.Left, valuePosition: false)} {pop} {Render(pb.Right, valuePosition: false)})";

                    case UnaryExpression pn when pn.NodeType == ExpressionType.Not:
                        return $"(NOT {RenderPredicate(pn.Operand)})";

                    case MemberExpression pm when pm.Type == typeof(bool):
                        // Bare boolean column: emit `col = TRUE`.
                        return $"({Render(pm, valuePosition: false)} = {_ctx.RawProvider.BooleanTrueLiteral})";

                    default:
                        return Render(p, valuePosition: false);
                }
            }

            // Emits a correlated subquery for a navigation-collection aggregate used as a
            // SetProperty value, e.g. SetProperty(p => p.Total, p => p.Items.Sum(i => i.Amount)):
            //   (SELECT COALESCE(SUM(n0."Amount"), 0) FROM "Item" n0 WHERE n0."ParentId" = "Parent"."Id")
            // The dependent table is always aliased so a self-referential aggregate
            // (p.Children.Count()) cannot collide with the outer table reference.
            string RenderNavigationAggregate(MethodCallExpression navAgg, MemberExpression navAggMember, TableMapping.Relation relation)
            {
                var dependent = _ctx.GetMapping(relation.DependentType);
                const string depAlias = "n0";

                var func = navAgg.Method.Name switch
                {
                    nameof(Enumerable.Sum) => "SUM",
                    nameof(Enumerable.Count) or nameof(Enumerable.LongCount) => "COUNT",
                    nameof(Enumerable.Min) => "MIN",
                    nameof(Enumerable.Max) => "MAX",
                    nameof(Enumerable.Average) => "AVG",
                    _ => throw new NormUnsupportedFeatureException(
                        $"Aggregate '{navAgg.Method.Name}' over navigation collection '{navAggMember.Member.Name}' is not supported."),
                };

                string agg;
                if (func == "COUNT")
                {
                    // Count()            -> COUNT(*)
                    // Count(i => pred)   -> COUNT(CASE WHEN pred THEN 1 END)   (NULL rows are not counted)
                    if (navAgg.Arguments.Count == 1)
                        agg = "COUNT(*)";
                    else if (StripQuotes(navAgg.Arguments[^1]) is LambdaExpression countPred)
                        agg = $"COUNT(CASE WHEN {RenderDependentPredicate(countPred.Body, countPred.Parameters[0], dependent, depAlias)} THEN 1 END)";
                    else
                        throw new NormUnsupportedFeatureException(
                            $"Count over '{navAggMember.Member.Name}' has an unsupported argument shape.");
                }
                else
                {
                    if (navAgg.Arguments.Count < 2 || StripQuotes(navAgg.Arguments[^1]) is not LambdaExpression selector)
                        throw new NormUnsupportedFeatureException(
                            $"Aggregate '{navAgg.Method.Name}' over '{navAggMember.Member.Name}' requires a mapped-column selector.");
                    var operand = RenderDependentOperand(selector.Body, selector.Parameters[0], dependent, depAlias);
                    // C# Average over ints is a double; SQL Server's AVG(int) truncates to int,
                    // so its provider hook casts integral operands to FLOAT (identity elsewhere) —
                    // the same hook every other aggregate emit path uses.
                    if (func == "AVG")
                        operand = _ctx.RawProvider.AverageAggregateOperand(operand, selector.Body.Type);
                    agg = $"{func}({operand})";
                    // Enumerable.Sum returns 0 for an empty sequence; SQL SUM yields NULL. Coalesce
                    // so the persisted value matches LINQ semantics (Count already yields 0).
                    if (func == "SUM")
                        agg = $"COALESCE({agg}, 0)";
                }

                if (relation.ForeignKeys.Count != relation.PrincipalKeys.Count || relation.ForeignKeys.Count == 0)
                    throw new NormUnsupportedFeatureException(
                        $"Relationship '{navAggMember.Member.Name}' has no usable key columns for a navigation-aggregate subquery.");

                var conds = new List<string>(relation.ForeignKeys.Count + 1);
                for (var i = 0; i < relation.ForeignKeys.Count; i++)
                    conds.Add($"{depAlias}.{relation.ForeignKeys[i].EscCol} = {mapping.EscTable}.{relation.PrincipalKeys[i].EscCol}");

                // Tenant-scope the dependent aggregate so a multi-tenant UPDATE cannot fold in rows
                // from another tenant that happen to share the same foreign-key value.
                if (_ctx.Options.TenantProvider != null && dependent.TenantColumn is { } depTenant)
                    conds.Add($"{depAlias}.{depTenant.EscCol} = {RenderLiteral(_ctx.GetRequiredTenantId(dependent, "ExecuteUpdate navigation aggregate"), valuePosition: false)}");

                return $"(SELECT {agg} FROM {dependent.EscTable} {depAlias} WHERE {string.Join(" AND ", conds)})";
            }

            // Renders a value expression over the dependent row of a navigation aggregate, prefixing
            // mapped columns with the dependent alias. Supports mapped columns, inlined constants and
            // arithmetic between them — the practical surface of an aggregate selector.
            string RenderDependentOperand(Expression e, ParameterExpression depParam, TableMapping dependent, string alias)
            {
                switch (e)
                {
                    case UnaryExpression ue when ue.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked:
                        return RenderDependentOperand(ue.Operand, depParam, dependent, alias);

                    case MemberExpression me when TableMapping.TryGetMemberAccessRoot(me, out var root) && root == depParam:
                        if (!dependent.TryGetColumnForMemberAccess(me, out var dcol))
                            throw new NormUnsupportedFeatureException(
                                $"Aggregate selector member '{me.Member.Name}' is not a mapped column on '{dependent.TableName}'.");
                        return $"{alias}.{dcol.EscCol}";

                    case MemberExpression captured when nORM.Query.QueryTranslator.TryGetConstantValue(captured, out var capturedValue):
                        return RenderLiteral(capturedValue, valuePosition: false);

                    case ConstantExpression ce:
                        return RenderLiteral(ce.Value, valuePosition: false);

                    case BinaryExpression be:
                        var op = be.NodeType switch
                        {
                            ExpressionType.Add => "+",
                            ExpressionType.Subtract => "-",
                            ExpressionType.Multiply => "*",
                            // Integral division truncates in C#; route through the provider's
                            // integer-division operator (MySQL DIV) like the other emit paths.
                            ExpressionType.Divide when nORM.Providers.DatabaseProvider.IsIntegralArithmeticType(be.Type)
                                => _ctx.RawProvider.IntegerDivisionOperator,
                            ExpressionType.Divide => "/",
                            ExpressionType.Modulo => "%",
                            _ => throw new NormUnsupportedFeatureException(
                                $"Operator '{be.NodeType}' is not supported inside a navigation-aggregate selector."),
                        };
                        return $"({RenderDependentOperand(be.Left, depParam, dependent, alias)} {op} {RenderDependentOperand(be.Right, depParam, dependent, alias)})";

                    default:
                        throw new NormUnsupportedFeatureException(
                            $"Selector expression '{e}' inside a navigation aggregate is not translatable; use a mapped column or arithmetic over mapped columns.");
                }
            }

            // Renders a boolean predicate over the dependent row (for Count(predicate)).
            string RenderDependentPredicate(Expression p, ParameterExpression depParam, TableMapping dependent, string alias)
            {
                switch (p)
                {
                    case UnaryExpression not when not.NodeType == ExpressionType.Not:
                        return $"(NOT {RenderDependentPredicate(not.Operand, depParam, dependent, alias)})";

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
                                $"Predicate operator '{pb.NodeType}' is not supported inside a navigation-aggregate Count."),
                        };
                        if (pop is "AND" or "OR")
                            return $"({RenderDependentPredicate(pb.Left, depParam, dependent, alias)} {pop} {RenderDependentPredicate(pb.Right, depParam, dependent, alias)})";
                        return $"({RenderDependentOperand(pb.Left, depParam, dependent, alias)} {pop} {RenderDependentOperand(pb.Right, depParam, dependent, alias)})";

                    case MemberExpression pm when pm.Type == typeof(bool)
                        && TableMapping.TryGetMemberAccessRoot(pm, out var boolRoot) && boolRoot == depParam:
                        return $"({RenderDependentOperand(pm, depParam, dependent, alias)} = {_ctx.RawProvider.BooleanTrueLiteral})";

                    default:
                        throw new NormUnsupportedFeatureException(
                            $"Predicate '{p}' inside a navigation-aggregate Count is not translatable.");
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

                // A property on a captured value — e.g. SetProperty(p => p.Price, request.Price) where
                // Price is a record/DTO property, not a field. Mirror the field case so any captured
                // property (chain) folds to a bound parameter, not just closure-hoisted locals.
                case MemberExpression { Member: PropertyInfo prop } propMember:
                    if (propMember.Expression == null)
                    {
                        value = prop.GetValue(null);
                        return true;
                    }

                    if (TryGetSetValue(propMember.Expression, out var propOwner))
                    {
                        value = prop.GetValue(propOwner);
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
