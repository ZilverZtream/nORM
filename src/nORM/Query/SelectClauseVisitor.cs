using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.ObjectPool;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed class SelectClauseVisitor : ExpressionVisitor
    {
        private readonly TableMapping _mapping;
        private readonly List<string> _groupBy;
        private readonly DatabaseProvider _provider;
        // Outer-row alias used to reference parent columns from correlated subqueries
        // emitted inside the projection (e.g. `parent.Children.Count()` →
        // `(SELECT COUNT(*) FROM Child WHERE Child.FK = {outerAlias}.PK)`). Defaults to
        // the principal table's escaped name when no JOIN alias is in scope — SQLite
        // accepts table-qualified outer-scope references even without an explicit AS alias.
        private readonly string _outerAlias;
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());
        private StringBuilder? _sb;
        private List<PropertyInfo> _detectedCollections = new();

        /// <summary>SQL aggregate function name for COUNT operations.</summary>
        private const string CountFunctionName = "COUNT";

        public SelectClauseVisitor(TableMapping mapping, List<string> groupBy, DatabaseProvider provider, string? outerAlias = null)
        {
            _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
            _groupBy = groupBy ?? throw new ArgumentNullException(nameof(groupBy));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _outerAlias = outerAlias ?? mapping.EscTable;
        }

        /// <summary>
        /// Gets the list of navigation properties detected during translation that should be
        /// fetched via separate queries to avoid Cartesian explosion.
        /// </summary>
        public IReadOnlyList<PropertyInfo> DetectedCollections => _detectedCollections;

        /// <summary>
        /// Translates a projection expression into a SQL <c>SELECT</c> column list.
        /// Each call resets detected collections, so the result reflects only the
        /// most recent translation.
        /// </summary>
        /// <param name="e">The projection expression to translate.</param>
        /// <returns>SQL representing the projection.</returns>
        public string Translate(Expression e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));

            // Reset per-translation state so consecutive calls don't accumulate stale entries.
            _detectedCollections = new List<PropertyInfo>();

            var sb = _stringBuilderPool.Get();
            _sb = sb;
            try
            {
                Visit(e);
                return sb.ToString();
            }
            finally
            {
                _sb = null;
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var sb = EnsureBuilder();
            bool firstColumn = true;
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                // Members may be null for ValueTuple or similar parameterized constructors;
                // fall back to positional Item name (Item1, Item2, ...).
                var memberName = node.Members?[i]?.Name ?? $"Item{i + 1}";

                // Check if this is a navigation property (collection)
                if (IsNavigationCollection(arg, out var navProperty))
                {
                    // Track it for later split query processing
                    _detectedCollections.Add(navProperty);
                    // Skip adding to SQL SELECT - it will be fetched separately
                    continue;
                }

                if (!firstColumn) sb.Append(", ");
                Visit(arg);
                sb.Append(" AS ").Append(_provider.Escape(memberName));
                firstColumn = false;
            }
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var sb = EnsureBuilder();
            if (node.Expression is ParameterExpression p && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>) && node.Member.Name == "Key")
            {
                for (int i = 0; i < _groupBy.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append(_groupBy[i]);
                }
            }
            else
            {
                if (!_mapping.ColumnsByName.TryGetValue(node.Member.Name, out var col))
                    throw new InvalidOperationException(
                        $"Member '{node.Member.Name}' on type '{node.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                        $"Ensure the property is read/write and not a navigation collection.");
                sb.Append(col.EscCol);
            }
            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var sb = EnsureBuilder();

            // `parent.Children.Count()` (or Any/All/LongCount) inside a Select projection.
            // ExpressionToSqlVisitor recognises this shape and rewrites it into a correlated
            // subquery (`(SELECT COUNT(*) FROM Child WHERE Child.FK = parent.PK)`), but this
            // visitor previously fell through to the generic aggregate path and emitted
            // `COUNT(*)` against the outer table — turning the per-row projection into a
            // single-row aggregate result. Detect the shape and emit the correlated
            // subquery here too so `Select(p => new {p.Name, Count = p.Children.Count()})`
            // returns one row per parent with the right child count.
            if (node.Arguments.Count >= 1
                && node.Arguments[0] is MemberExpression navMember
                && navMember.Expression is ParameterExpression
                && _mapping.Relations.TryGetValue(navMember.Member.Name, out var relation)
                && (node.Method.Name is nameof(Queryable.Count)
                                     or nameof(Queryable.LongCount)
                                     or nameof(Queryable.Any)
                                     or nameof(Queryable.All)))
            {
                EmitNavigationCountSubquery(sb, node, relation);
                return node;
            }

            // Use ToUpperInvariant to avoid locale-sensitive casing (e.g., Turkish-I problem).
            var methodNameUpper = node.Method.Name.ToUpperInvariant();

            // static string.Format("template", args...) — emit as a provider concat of
            // literal pieces and projected argument columns. Format specifiers (`{0:N2}` /
            // `{0,5}`) fall through to the aggregate path (which throws), matching the
            // WHERE-side behaviour: simple positional placeholders translate, anything
            // richer needs client-eval.
            if (node.Method.DeclaringType == typeof(string)
                && node.Object == null
                && node.Method.Name == nameof(string.Format)
                && node.Arguments.Count >= 2
                && node.Arguments[0] is ConstantExpression { Value: string template })
            {
                var segments = TryParseFormatSegments(template);
                if (segments != null)
                {
                    var argExprs = new List<Expression>();
                    for (int i = 1; i < node.Arguments.Count; i++)
                    {
                        var a = node.Arguments[i];
                        if (a is NewArrayExpression arr) argExprs.AddRange(arr.Expressions);
                        else argExprs.Add(a);
                    }
                    var parts = new List<string>();
                    foreach (var seg in segments)
                    {
                        if (seg.IsLiteral)
                        {
                            if (seg.Literal!.Length > 0)
                                parts.Add($"'{seg.Literal.Replace("'", "''")}'");
                        }
                        else
                        {
                            if (seg.ArgIndex >= argExprs.Count) goto fallthrough;
                            var arg = argExprs[seg.ArgIndex];
                            var inner = TranslateProjectionArg(arg);
                            if (arg.Type != typeof(string))
                                inner = _provider.GetToStringSql(inner);
                            parts.Add(inner);
                        }
                    }
                    if (parts.Count == 0) { sb.Append("''"); return node; }
                    sb.Append(parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next)));
                    return node;
                }
            }
            fallthrough:

            sb.Append(methodNameUpper).Append('(');
            if (node.Arguments.Count > 1)
            {
                // StripQuotes handles LINQ's UnaryExpression{Quote()} wrapping around lambdas.
                if (StripQuotes(node.Arguments[1]) is not LambdaExpression lambda)
                    throw new InvalidOperationException(
                        $"Expected a lambda expression as argument 1 of '{node.Method.Name}', but got '{node.Arguments[1].NodeType}'.");
                if (lambda.Body is MemberExpression me)
                {
                    if (!_mapping.ColumnsByName.TryGetValue(me.Member.Name, out var col))
                        throw new InvalidOperationException(
                            $"Member '{me.Member.Name}' on type '{me.Member.DeclaringType?.Name}' is not mapped to a column in table '{_mapping.TableName}'. " +
                            $"Ensure the property is read/write and not a navigation collection.");
                    sb.Append(col.EscCol);
                }
                else
                {
                    // Computed selector: arithmetic, conditional, etc. Visit the body so the
                    // ConditionalExpression / BinaryExpression overrides turn it into SQL.
                    Visit(lambda.Body);
                }
            }
            else if (!string.Equals(methodNameUpper, CountFunctionName, StringComparison.Ordinal))
            {
                sb.Append(_groupBy.FirstOrDefault() ?? "*");
            }
            else
            {
                sb.Append('*');
            }
            sb.Append(')');
            return node;
        }

        private void EmitNavigationCountSubquery(StringBuilder sb, MethodCallExpression node, TableMapping.Relation relation)
        {
            // Resolve the dependent table mapping from the relation's DependentType. The
            // navigation registration on Relations doesn't carry the dependent TableMapping
            // directly, so look it up via the principal mapping's column lookup (Relations is
            // populated post-build, so we can reach DependentType.Mapping through the
            // ForeignKey property — which IS on the dependent type).
            var depType = relation.DependentType;
            // We need an escaped table identifier and column for the dependent. Since SCV
            // doesn't have a TableMappingProvider, build the SQL identifiers manually from
            // attributes — match the same convention TableMapping uses (table name from
            // [Table] attribute or type name; columns from property names escaped by provider).
            var depTable = depType.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), inherit: false)
                .Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>().FirstOrDefault()?.Name ?? depType.Name;
            var depAlias = _provider.Escape("__nav");
            var fkCol = _provider.Escape(relation.ForeignKey.Prop.Name);
            var pkCol = _provider.Escape(relation.PrincipalKey.Prop.Name);
            // Outer alias / table-name reference for parent columns — set via SCV ctor.
            var outerAlias = _outerAlias;

            sb.Append('(').Append("SELECT ");
            if (node.Method.Name is nameof(Queryable.Any))
                sb.Append("CASE WHEN EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol)
                  .Append(") THEN 1 ELSE 0 END");
            else if (node.Method.Name is nameof(Queryable.All))
                sb.Append("CASE WHEN NOT EXISTS(SELECT 1 FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol)
                  .Append(") THEN 1 ELSE 0 END");
            else
                sb.Append("COUNT(*) FROM ").Append(_provider.Escape(depTable)).Append(' ').Append(depAlias)
                  .Append(" WHERE ").Append(depAlias).Append('.').Append(fkCol).Append(" = ").Append(outerAlias).Append('.').Append(pkCol);

            if (node.Method.Name is nameof(Queryable.Any) or nameof(Queryable.All))
            {
                sb.Append(')');
                return;
            }
            sb.Append(')');
        }

        private string TranslateProjectionArg(Expression arg)
        {
            var saved = _sb;
            var tmp = new StringBuilder();
            _sb = tmp;
            try
            {
                Visit(arg);
                return tmp.ToString();
            }
            finally
            {
                _sb = saved;
            }
        }

        private readonly struct FormatSegment
        {
            public readonly bool IsLiteral;
            public readonly string? Literal;
            public readonly int ArgIndex;
            public FormatSegment(string l) { IsLiteral = true; Literal = l; ArgIndex = -1; }
            public FormatSegment(int i) { IsLiteral = false; Literal = null; ArgIndex = i; }
        }

        private static List<FormatSegment>? TryParseFormatSegments(string template)
        {
            var segments = new List<FormatSegment>();
            var literal = new StringBuilder();
            int i = 0;
            while (i < template.Length)
            {
                char c = template[i];
                if (c == '{')
                {
                    if (i + 1 < template.Length && template[i + 1] == '{') { literal.Append('{'); i += 2; continue; }
                    if (literal.Length > 0) { segments.Add(new FormatSegment(literal.ToString())); literal.Clear(); }
                    int end = template.IndexOf('}', i + 1);
                    if (end < 0) return null;
                    var inner = template.Substring(i + 1, end - i - 1);
                    if (inner.Length == 0 || inner.IndexOfAny(new[] { ',', ':' }) >= 0) return null;
                    if (!int.TryParse(inner, out var argIdx) || argIdx < 0) return null;
                    segments.Add(new FormatSegment(argIdx));
                    i = end + 1;
                }
                else if (c == '}')
                {
                    if (i + 1 < template.Length && template[i + 1] == '}') { literal.Append('}'); i += 2; continue; }
                    return null;
                }
                else { literal.Append(c); i++; }
            }
            if (literal.Length > 0) segments.Add(new FormatSegment(literal.ToString()));
            return segments;
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            var sb = EnsureBuilder();
            sb.Append("(CASE WHEN ");
            Visit(node.Test);
            sb.Append(" THEN ");
            Visit(node.IfTrue);
            sb.Append(" ELSE ");
            Visit(node.IfFalse);
            sb.Append(" END)");
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var sb = EnsureBuilder();
            if (node.Value is null)
            {
                sb.Append("NULL");
                return node;
            }
            switch (node.Value)
            {
                case string s:
                    sb.Append('\'').Append(s.Replace("'", "''")).Append('\'');
                    break;
                case bool b:
                    sb.Append(b ? _provider.BooleanTrueLiteral : "0");
                    break;
                case System.Enum e:
                    sb.Append(Convert.ToInt64(e, System.Globalization.CultureInfo.InvariantCulture));
                    break;
                default:
                    sb.Append(System.Convert.ToString(node.Value, System.Globalization.CultureInfo.InvariantCulture));
                    break;
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            var sb = EnsureBuilder();
            // `a ?? b` in a projection lowers to COALESCE(a, b) — emit as a function call so
            // it composes inside `new { Name = r.Name ?? "anon" }` projections.
            if (node.NodeType == ExpressionType.Coalesce)
            {
                sb.Append("COALESCE(");
                Visit(node.Left);
                sb.Append(", ");
                Visit(node.Right);
                sb.Append(')');
                return node;
            }
            sb.Append('(');
            Visit(node.Left);
            sb.Append(' ').Append(node.NodeType switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "<>",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.AndAlso or ExpressionType.And => "AND",
                ExpressionType.OrElse or ExpressionType.Or => "OR",
                ExpressionType.Add => "+",
                ExpressionType.Subtract => "-",
                ExpressionType.Multiply => "*",
                ExpressionType.Divide => "/",
                ExpressionType.Modulo => "%",
                _ => throw new InvalidOperationException(
                    $"Binary operator '{node.NodeType}' has no portable SQL equivalent in a SELECT " +
                    "projection. For LeftShift / RightShift, rewrite as multiply / divide by a power " +
                    "of 2 (`x * 2` for `x << 1`, `x / 4` for `x >> 2`) — the SQL planner produces the " +
                    "same execution plan and the rewrite works on every provider. For Power, use " +
                    "`Math.Pow(x, n)` which lowers to the provider's POWER / POW function."),
            }).Append(' ');
            Visit(node.Right);
            sb.Append(')');
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Numeric / enum / primitive Convert: the SQL value is the operand itself.
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                Visit(node.Operand);
                return node;
            }
            return base.VisitUnary(node);
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            var sb = EnsureBuilder();
            bool firstColumn = true;
            for (int i = 0; i < node.Bindings.Count; i++)
            {
                if (node.Bindings[i] is MemberAssignment assignment)
                {
                    // Check if this is a navigation property (collection)
                    if (IsNavigationCollection(assignment.Expression, out var navProperty))
                    {
                        // Track it for later split query processing
                        _detectedCollections.Add(navProperty);
                        // Skip adding to SQL SELECT - it will be fetched separately
                        continue;
                    }

                    if (!firstColumn) sb.Append(", ");
                    Visit(assignment.Expression);
                    sb.Append(" AS ").Append(_provider.Escape(assignment.Member.Name));
                    firstColumn = false;
                }
            }
            return node;
        }

        /// <summary>
        /// Determines if the expression represents a navigation property that is a collection
        /// (e.g., <c>ICollection&lt;T&gt;</c>, <c>List&lt;T&gt;</c>) rather than a mapped scalar column.
        /// </summary>
        private bool IsNavigationCollection(Expression expr, out PropertyInfo property)
        {
            property = null!;

            // Look for member access like "b.Posts"
            if (expr is MemberExpression memberExpr &&
                memberExpr.Member is PropertyInfo propInfo &&
                memberExpr.Expression is ParameterExpression)
            {
                var propType = propInfo.PropertyType;

                // Check if it's a generic collection type (IEnumerable<T> but not string,
                // which implements IEnumerable but is a scalar column type).
                if (propType != typeof(string) &&
                    typeof(IEnumerable).IsAssignableFrom(propType) &&
                    propType.IsGenericType)
                {
                    // Verify it's actually a navigation property (not a mapped column)
                    if (!_mapping.ColumnsByName.ContainsKey(propInfo.Name))
                    {
                        property = propInfo;
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Returns the active <see cref="StringBuilder"/>, throwing if called outside
        /// a <see cref="Translate"/> invocation.
        /// </summary>
        private StringBuilder EnsureBuilder() =>
            _sb ?? throw new InvalidOperationException("Cannot visit expressions outside of a Translate() call.");

        private static Expression StripQuotes(Expression e) => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
    }
}