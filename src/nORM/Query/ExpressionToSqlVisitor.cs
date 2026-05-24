using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using System.Collections.Frozen;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
#nullable enable
namespace nORM.Query
{
    internal sealed class ExpressionToSqlVisitor : ExpressionVisitor, nORM.Internal.IResettable, IDisposable
    {
        private DbContext _ctx = null!;
        private TableMapping _mapping = null!;
        private DatabaseProvider _provider = null!;
        private readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> _parameterMappings = new();
        private ParameterExpression _parameter = null!;
        private string _tableAlias = string.Empty;
        private OptimizedSqlBuilder _sql = null!;
        private readonly Dictionary<string, object> _params = new();
        // Parameter sink (can be redirected to a shared dictionary)
        private Dictionary<string, object> _paramSink = null!;
        private int _paramIndex = 0;
        private readonly List<string> _ownedCompiledParams = new();
        private readonly Dictionary<ParameterExpression, string> _ownedParamMap = new();
        private List<string> _compiledParams = null!;
        private Dictionary<ParameterExpression, string> _paramMap = null!;
        private bool _suppressNullCheck = false;
        // Outer translator's recursion depth so BuildExists/BuildIn pass depth+1 to sub-translators.
        private int _recursionDepth = 0;
        private const int ConstParamMapLimit = 1024;
        /// <summary>Maximum allowed length for a JSON path in <c>Json.Value()</c>.</summary>
        private const int MaxJsonPathLength = 500;
        /// <summary>SQL false literal used when a local collection is empty.</summary>
        private const string SqlFalseLiteral = "(1=0)";
        /// <summary>
        /// Maximum length (in characters or bytes) for inline string/binary parameters.
        /// Based on SQL Server's non-MAX varchar/varbinary threshold.
        /// </summary>
        private const int MaxInlineParameterLength = 8000;
        private readonly Dictionary<ConstKey, string> _constParamMap = new();
        private readonly Dictionary<(ParameterExpression Param, string Member), string> _memberParamMap = new();
        private static readonly Expression s_emptyExpression = Expression.Empty();
        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();
        // String methods (Contains, StartsWith, EndsWith) are handled via _fastMethodHandlers
        // for better performance, avoiding a redundant _translators dictionary lookup.
        private static readonly Dictionary<MethodInfo, Action<ExpressionToSqlVisitor, MethodCallExpression>> _fastMethodHandlers =
            new()
            {
                { typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!, HandleStringContains },
                { typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) })!, HandleStringStartsWith },
                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };
        internal ExpressionToSqlVisitor() { }
        public ExpressionToSqlVisitor(DbContext ctx, TableMapping mapping, DatabaseProvider provider,
                                      ParameterExpression parameter, string tableAlias,
                                      Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
                                      List<string>? compiledParams = null,
                                      Dictionary<ParameterExpression, string>? paramMap = null)
        {
            var context = new VisitorContext(ctx ?? throw new ArgumentNullException(nameof(ctx)), mapping ?? throw new ArgumentNullException(nameof(mapping)), provider ?? throw new ArgumentNullException(nameof(provider)), parameter ?? throw new ArgumentNullException(nameof(parameter)), tableAlias ?? throw new ArgumentNullException(nameof(tableAlias)), correlated, compiledParams, paramMap);
            Initialize(in context);
        }
        /// <summary>
        /// Initializes the visitor with all context required to translate a LINQ expression into
        /// SQL. This method can be called multiple times to reuse the same instance for different
        /// translations.
        /// </summary>
        /// <param name="context">A structure containing the current <see cref="DbContext"/>,
        /// table mapping, provider and parameter information.</param>
        public void Initialize(in VisitorContext context)
        {
            _paramSink = _params;
            _ctx = context.Context;
            _mapping = context.Mapping;
            _provider = context.Provider;
            _parameter = context.Parameter;
            _tableAlias = context.TableAlias;
            _parameterMappings.Clear();
            if (context.Correlated != null)
            {
                foreach (var kvp in context.Correlated)
                    _parameterMappings[kvp.Key] = kvp.Value;
            }
            _parameterMappings[context.Parameter] = (context.Mapping, context.TableAlias);
            _compiledParams = context.CompiledParams ?? _ownedCompiledParams;
            if (context.CompiledParams == null)
                _ownedCompiledParams.Clear();
            _paramMap = context.ParamMap ?? _ownedParamMap;
            if (context.ParamMap == null)
                _ownedParamMap.Clear();
            _constParamMap.Clear();
            // Start numbering from the caller-supplied offset so that a visitor sharing
            // _compiledParams/_paramMap with a previous visitor does not reuse parameter names
            // (e.g. both inner Where and global-filter Where getting @p0).
            _paramIndex = context.ParamIndexStart;
            _suppressNullCheck = false;
            _recursionDepth = context.RecursionDepth;
            _memberParamMap.Clear();
            _groupingKeys.Clear();
        }
        /// <summary>
        /// Resets the internal state so that the visitor can be returned to an object pool and
        /// reused for subsequent translations.
        /// </summary>
        public void Reset()
        {
            _sql = null!;
            _params.Clear();
            _paramSink = null!;
            _paramIndex = 0;
            _parameterMappings.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
            _compiledParams = null!;
            _paramMap = null!;
            _ctx = null!;
            _mapping = null!;
            _provider = null!;
            _parameter = null!;
            _tableAlias = string.Empty;
            _suppressNullCheck = false;
            _recursionDepth = 0;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _groupingKeys.Clear();
        }
        /// <summary>
        /// Releases resources by resetting the visitor's state. The instance can be reused after
        /// calling this method.
        /// </summary>
        public void Dispose()
        {
            Reset();
        }
        /// <summary>
        /// Translates the supplied LINQ expression tree into an SQL string using the configured
        /// provider and mapping information.
        /// </summary>
        /// <param name="expression">The expression to translate.</param>
        /// <returns>The SQL text corresponding to the expression.</returns>
        public string Translate(Expression expression)
        {
            using var builder = new OptimizedSqlBuilder();
            _sql = builder;
            if (!TryEmitMappedBooleanPredicate(expression, expectedValue: true))
                Visit(expression);
            return builder.ToSqlString();
        }
        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType is ExpressionType.Equal or ExpressionType.NotEqual)
            {
                bool leftNull  = IsNullExpression(node.Left);
                bool rightNull = IsNullExpression(node.Right);
                if (leftNull || rightNull)
                {
                    _sql.Append("(");
                    Visit(leftNull ? node.Right : node.Left);
                    _sql.Append(node.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL");
                    _sql.Append(")");
                    return node;
                }

                // Inline boolean literals (true/false) as SQL literals instead of parameterizing.
                // Parameterized booleans (WHERE col = @p0 with @p0=1) deprive query planners of
                // column selectivity statistics. Providers that prefer bare boolean predicates get
                // WHERE col / WHERE NOT col for non-nullable bools instead.
                if (TryInlineBoolLiteral(node))
                    return node;

                // Nullable column-vs-column comparison needs three-valued logic.
                // A plain = or <> is incorrect when either side can be NULL at runtime.
                // For Nullable<T> value types: always expand (runtime null possible).
                // For reference types (string, class): only expand when BOTH sides could be null
                // at runtime (i.e., neither side is a known non-null constant or parameter).
                // Comparing a string column to a literal "ABC" does not need expansion since "ABC" is never null.
                if (NeedsNullSafeExpansion(node.Left, node.Right, node.NodeType))
                {
                    int ls = _sql.Length;
                    Visit(node.Left);
                    string lf = _sql.ToString(ls, _sql.Length - ls);
                    _sql.TruncateTo(ls);

                    int rs = _sql.Length;
                    Visit(node.Right);
                    string rf = _sql.ToString(rs, _sql.Length - rs);
                    _sql.TruncateTo(rs);

                    if (node.NodeType == ExpressionType.Equal)
                    {
                        _sql.Append(_provider.NullSafeEqual(lf, rf));
                    }
                    else
                    {
                        // For NotEqual, use asymmetric simplified form when the right side is a
                        // known non-null value (e.g. a literal or closure-captured non-null constant).
                        // In that case, "rf IS NULL" can never happen, so the full 3-way expansion
                        // reduces to: (lf IS NULL OR lf <> rf)
                        bool rightCouldBeNull = CouldBeNull(node.Right);
                        if (!rightCouldBeNull)
                            _sql.Append($"({lf} IS NULL OR {lf} <> {rf})");
                        else
                            _sql.Append(_provider.NullSafeNotEqual(lf, rf));
                    }
                    return node;
                }
            }

            if (node.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
            {
                _sql.Append("(");
                if (!TryEmitMappedBooleanPredicate(node.Left, expectedValue: true))
                    Visit(node.Left);
                _sql.Append(node.NodeType == ExpressionType.AndAlso ? " AND " : " OR ");
                if (!TryEmitMappedBooleanPredicate(node.Right, expectedValue: true))
                    Visit(node.Right);
                _sql.Append(")");
                return node;
            }

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
                _ => throw new NormUnsupportedFeatureException($"Binary operator '{node.NodeType}' is not supported.")
            });
            Visit(node.Right);
            _sql.Append(")");
            return node;
        }

        /// <summary>
        /// Checks if an expression is a compile-time boolean constant (true/false).
        /// Handles ConstantExpression and Convert(ConstantExpression) wrappers.
        /// </summary>
        private static bool TryGetBoolConstant(Expression expr, out bool value)
        {
            if (expr is ConstantExpression { Value: bool b })
            {
                value = b;
                return true;
            }
            if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ue)
                return TryGetBoolConstant(ue.Operand, out value);
            value = default;
            return false;
        }

        /// <summary>
        /// Optimizes boolean literal comparisons by emitting SQL literals instead of parameters.
        /// Returns true if the optimization was applied (caller should return node immediately).
        /// </summary>
        private bool TryInlineBoolLiteral(BinaryExpression node)
        {
            if (TryGetBoolConstant(node.Left, out bool boolVal))
            {
                EmitBoolComparison(node.Right, boolVal, node.NodeType);
                return true;
            }
            if (TryGetBoolConstant(node.Right, out boolVal))
            {
                EmitBoolComparison(node.Left, boolVal, node.NodeType);
                return true;
            }
            return false;
        }

        private void EmitBoolComparison(Expression memberSide, bool boolVal, ExpressionType op)
        {
            if (_provider.PrefersBareBooleanPredicates && memberSide.Type == typeof(bool))
            {
                var emitPositivePredicate =
                    op == ExpressionType.Equal && boolVal ||
                    op == ExpressionType.NotEqual && !boolVal;

                _sql.Append("(");
                if (!emitPositivePredicate)
                    _sql.Append("NOT ");
                Visit(memberSide);
                _sql.Append(")");
                return;
            }

            var literal = boolVal ? _provider.BooleanTrueLiteral : _provider.BooleanFalseLiteral;
            _sql.Append("(");
            Visit(memberSide);
            _sql.Append(op == ExpressionType.Equal ? " = " : " <> ");
            _sql.Append(literal);
            _sql.Append(")");
        }

        private static bool IsNullExpression(Expression e)
        {
            if (e is ConstantExpression ce)
                return ce.Value is null;
            if (e is UnaryExpression ue && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked))
                return IsNullExpression(ue.Operand);
            if (e is MemberExpression me && me.Expression is ConstantExpression closure)
            {
                try
                {
                    var val = closure.Value == null ? null :
                        me.Member is FieldInfo fi ? fi.GetValue(closure.Value) :
                        me.Member is PropertyInfo pi ? pi.GetValue(closure.Value) : null;
                    return val is null;
                }
                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures (getter throws, access denied, etc.) — conservatively
                    // report as non-null so the caller does not emit an incorrect IS NULL predicate.
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// Returns <c>true</c> when <paramref name="t"/> is <c>Nullable&lt;T&gt;</c>.
        /// Used to detect when column-vs-column comparisons need three-valued logic.
        /// </summary>
        private static bool IsNullableValueType(Type t) =>
            t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);

        /// <summary>
        /// Returns <c>true</c> when <paramref name="t"/> is either a reference type (string, class, etc.)
        /// or a <c>Nullable&lt;T&gt;</c> value type. Both can be NULL at runtime and require
        /// three-valued SQL logic (IS NULL guards) for equality/inequality comparisons.
        /// </summary>
        private static bool IsNullableOrReferenceType(Type t) =>
            !t.IsValueType ||  // reference types (string, class, etc.)
            (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>));

        /// <summary>
        /// Determines whether an equality/inequality comparison between two expressions needs
        /// three-valued SQL logic (IS NULL expansion). The expansion is required when:
        /// - Either side is <c>Nullable&lt;T&gt;</c>, or
        /// - For <c>Equal</c>: both sides are reference types that could be null (no change needed;
        ///   <c>NULL = 'Alice'</c> is UNKNOWN in SQL and <c>null == "Alice"</c> is false in C#, so plain = is correct).
        /// - For <c>NotEqual</c>: the LEFT side could be null (column reference) even when the right
        ///   side is a known non-null constant. SQL 3-valued logic makes <c>NULL &lt;&gt; 'Alice'</c>
        ///   UNKNOWN (excluded), but C# semantics say <c>null != "Alice"</c> is true (included).
        ///   Fix: emit <c>(col IS NULL OR col &lt;&gt; @p)</c> whenever left could be null.
        /// </summary>
        private bool NeedsNullSafeExpansion(Expression left, Expression right, ExpressionType nodeType = ExpressionType.Equal)
        {
            // Nullable<T> value types always need expansion (runtime null is possible on either side)
            if (IsNullableValueType(left.Type) || IsNullableValueType(right.Type))
                return true;

            // Reference types: asymmetric rule for Equal vs NotEqual.
            // At least one side is a reference type (the condition below is true when
            // either left or right is not a value type, via De Morgan on the negation).
            // A "known non-null" side is one that is:
            //   - A non-null compile-time constant (ConstantExpression with non-null value)
            //   - A closure capture whose value we can verify is non-null at expression-build time
            if (!left.Type.IsValueType || !right.Type.IsValueType)
            {
                bool leftCouldBeNull  = CouldBeNull(left);
                bool rightCouldBeNull = CouldBeNull(right);

                if (nodeType == ExpressionType.NotEqual)
                    // For NotEqual: expand when EITHER side could be null.
                    // Specifically, a nullable column vs a non-null constant still needs
                    // (col IS NULL OR col <> @p) to preserve C# null != "literal" → true semantics.
                    return leftCouldBeNull || rightCouldBeNull;
                else
                    // For Equal: expand only when BOTH sides could be null.
                    // null == "Alice" is false in both SQL (UNKNOWN→false) and C#, so no expansion needed.
                    return leftCouldBeNull && rightCouldBeNull;
            }

            return false;
        }

        /// <summary>
        /// Returns <c>true</c> when <paramref name="expr"/> might evaluate to SQL NULL at runtime.
        /// Constants and member accesses on closures whose current value is non-null are "safe"
        /// (they will never produce SQL NULL on the right-hand side of a comparison).
        /// Column references (member accesses on query parameters) are always potentially nullable.
        /// </summary>
        /// <remarks>
        /// This method does not distinguish C# nullable reference types (NRTs) from non-nullable
        /// reference types. The NRT annotation (e.g., <c>string?</c> vs <c>string</c>) is erased
        /// at runtime and is not present in expression trees. At the CLR level all reference types
        /// can be null, so this method conservatively returns <c>true</c> for any reference-typed
        /// column or method-call expression.
        /// </remarks>
        private bool CouldBeNull(Expression expr)
        {
            // Non-null compile-time constant: cannot be null
            if (expr is ConstantExpression ce)
                return ce.Value is null;

            // Unwrap casts/conversions
            if (expr is UnaryExpression ue && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked))
                return CouldBeNull(ue.Operand);

            if (expr is MemberExpression columnMember &&
                columnMember.Expression is ParameterExpression columnParameter &&
                _parameterMappings.TryGetValue(columnParameter, out var mappedParameter) &&
                mappedParameter.Mapping.ColumnsByName.TryGetValue(columnMember.Member.Name, out var column))
            {
                return column.IsNullable;
            }

            // Closure-captured member whose value is non-null at expression-build time
            if (expr is MemberExpression me && me.Expression is ConstantExpression closure)
            {
                try
                {
                    var val = closure.Value == null ? null :
                        me.Member is FieldInfo fi ? fi.GetValue(closure.Value) :
                        me.Member is PropertyInfo pi ? pi.GetValue(closure.Value) : null;
                    return val is null;  // non-null captured value → not nullable
                }
                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures (getter throws, access denied, etc.) — assume nullable
                    // to preserve correctness.
                    return true;
                }
            }

            // Everything else (column references, method calls, etc.) could be null
            return true;
        }

        private bool TryEmitMappedBooleanPredicate(Expression expression, bool expectedValue)
        {
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                expression = convert.Operand;

            if (expression is UnaryExpression { NodeType: ExpressionType.Not } not)
                return TryEmitMappedBooleanPredicate(not.Operand, !expectedValue);

            if (expression.Type != typeof(bool) ||
                expression is not MemberExpression member ||
                member.Expression is not ParameterExpression parameter ||
                !_parameterMappings.TryGetValue(parameter, out var info) ||
                !info.Mapping.ColumnsByName.ContainsKey(member.Member.Name))
            {
                return false;
            }

            var columnSql = GetSql(member);
            _sql.Append(_provider.FormatBooleanPredicate(columnSql, expectedValue));
            return true;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Nullable<T> structural members: HasValue -> IS NOT NULL, Value -> operand itself.
            // GetValueOrDefault is a method, handled in VisitMethodCall.
            if (node.Expression != null
                && node.Expression.Type.IsGenericType
                && node.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                if (node.Member.Name == "HasValue")
                {
                    _sql.Append('(');
                    Visit(node.Expression);
                    _sql.Append(" IS NOT NULL)");
                    return node;
                }
                if (node.Member.Name == "Value")
                {
                    Visit(node.Expression);
                    return node;
                }
            }
            if (node.Expression is ParameterExpression pe && _parameterMappings.TryGetValue(pe, out var info))
            {
                if (_groupingKeys.TryGetValue(pe, out var groupKey) && node.Member.Name == "Key")
                {
                    _sql.Append(groupKey);
                    return node;
                }
                if (info.Mapping.ColumnsByName.TryGetValue(node.Member.Name, out var column))
                {
                    // Table aliases are generated internally and escaped when created,
                    // allowing them to be used safely without additional validation.
                    _sql.Append($"{info.Alias}.{column.EscCol}");
                    return node;
                }
            }
            if (node.Expression is ParameterExpression p && !_parameterMappings.ContainsKey(p))
            {
                var key = (p, node.Member.Name);
                if (!_memberParamMap.TryGetValue(key, out var paramName))
                {
                    paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                    _params[paramName] = DBNull.Value;
                    _compiledParams.Add(paramName);
                    _memberParamMap[key] = paramName;
                }
                _sql.Append(paramName);
                return node;
            }
            if (TryGetConstantValue(node, out var value))
            {
                // Closure-captured variable: emit a compiled parameter so the live value is
                // re-extracted from the expression tree on every plan-cache hit.  Baking the
                // value into _params at translation time causes stale values to be used when
                // the captured variable changes between calls that share the same cached plan.
                //
                // Use the current size of the SHARED _compiledParams list as the index so that
                // parameter names are globally unique across all visitor instances within one
                // query translation.  The "cp" prefix prevents collisions with inline-constant
                // parameters which use the "p" prefix (visitor-local _paramIndex).
                var paramName = $"{_provider.ParamPrefix}cp{_compiledParams.Count}";
                _params[paramName] = DBNull.Value; // placeholder; actual value supplied at execution time
                _compiledParams.Add(paramName);
                _sql.Append(paramName);
                return node;
            }
            if (node.Expression != null)
            {
                var exprSql = GetSql(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType!, exprSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
            }
            throw new NormUnsupportedFeatureException($"Member '{node.Member.Name}' is not supported in this context.");
        }
        protected override Expression VisitConstant(ConstantExpression node)
        {
            AppendConstant(node.Value, node.Type);
            return node;
        }
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (_parameterMappings.ContainsKey(node))
                return base.VisitParameter(node);
            if (_paramMap.TryGetValue(node, out var existing))
            {
                _sql.Append(existing);
                return node;
            }
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _params[paramName] = DBNull.Value;
            _compiledParams.Add(paramName);
            _paramMap[node] = paramName;
            _sql.Append(paramName);
            return node;
        }
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                if (TryEmitMappedBooleanPredicate(node.Operand, expectedValue: false))
                    return node;

                _sql.Append("(NOT(");
                Visit(node.Operand);
                _sql.Append("))");
                return node;
            }
            // Numeric / enum conversions in projections: (int)entity.Status, (long)e.Count, etc.
            // SQL columns are already typed, so just emit the operand. Reference-type Convert
            // (interface casts, base→derived) has no SQL meaning and falls through to default.
            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                var operandType = node.Operand.Type;
                var targetType = node.Type;
                var operandUnderlying = Nullable.GetUnderlyingType(operandType) ?? operandType;
                var targetUnderlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
                bool operandIsPrimitive = operandUnderlying.IsPrimitive || operandUnderlying.IsEnum
                    || operandUnderlying == typeof(decimal) || operandUnderlying == typeof(string);
                bool targetIsPrimitive = targetUnderlying.IsPrimitive || targetUnderlying.IsEnum
                    || targetUnderlying == typeof(decimal) || targetUnderlying == typeof(string);
                if (operandIsPrimitive && targetIsPrimitive)
                {
                    Visit(node.Operand);
                    return node;
                }
            }
            return base.VisitUnary(node);
        }
        protected override Expression VisitConditional(ConditionalExpression node)
        {
            // x ? a : b -> (CASE WHEN x THEN a ELSE b END). Nested conditionals naturally
            // recurse, producing CASE WHEN ... WHEN ... ELSE ... END.
            _sql.Append("(CASE WHEN ");
            Visit(node.Test);
            _sql.Append(" THEN ");
            Visit(node.IfTrue);
            _sql.Append(" ELSE ");
            Visit(node.IfFalse);
            _sql.Append(" END)");
            return node;
        }
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Fast path: common string methods (Contains, StartsWith, EndsWith) are handled
            // directly via pre-built delegates, bypassing the general method translation pipeline.
            if (_fastMethodHandlers.TryGetValue(node.Method, out var handler))
            {
                handler(this, node);
                return node;
            }
            // Nullable<T>.GetValueOrDefault() / GetValueOrDefault(fallback) — COALESCE.
            if (node.Object != null
                && node.Method.Name == nameof(Nullable<int>.GetValueOrDefault)
                && node.Object.Type.IsGenericType
                && node.Object.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var inner = GetSql(node.Object);
                if (node.Arguments.Count == 0)
                {
                    var underlying = Nullable.GetUnderlyingType(node.Object.Type)!;
                    var fallback = underlying == typeof(string) ? "''"
                        : underlying.IsValueType ? "0"
                        : "NULL";
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fallback).Append(')');
                }
                else
                {
                    var fbSql = GetSql(node.Arguments[0]);
                    _sql.Append("COALESCE(").Append(inner).Append(", ").Append(fbSql).Append(')');
                }
                return node;
            }
            if (!IsTranslatableMethod(node.Method))
                // ErrorMessages.QueryTranslationFailed is "Failed to translate LINQ query to SQL: {0}".
                // The {0} argument below is a detail message, not a duplicate prefix.
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Method '{node.Method.Name}' cannot be translated to SQL"));
            if (!_suppressNullCheck && RequiresNullCheck(node))
            {
                return TranslateWithNullCheck(node);
            }
            if (TryGetConstantValueSafe(node, out var constVal))
            {
                return CreateSafeParameter(constVal);
            }
            if (node.Method.DeclaringType == typeof(Json) && node.Method.Name == nameof(Json.Value))
            {
                var columnSql = GetSql(node.Arguments[0]);
                if (TryGetConstantValue(node.Arguments[1], out var path) && path is string jsonPath)
                {
                    // TYPE SAFETY FIX: Validate JSON path format to prevent SQL injection and runtime errors
                    if (string.IsNullOrWhiteSpace(jsonPath))
                    {
                        throw new NormQueryException("JSON path cannot be null or whitespace.");
                    }

                    // Q1 fix: validate JSON path for SQL-injection-capable chars only.
                    // Chars that can break out of the enclosing SQL string literal:
                    //   ' → closes string literal
                    //   " → identifier-quote escape
                    //   ; → statement terminator
                    //   \ → SQL/escape sequence in some dialects
                    // Chars that are VALID in JSON property names and must be allowed:
                    //   - (hyphen) → e.g. $.order-items[0].id (RFC 7159 §7 allows in key names)
                    //   * (wildcard) → e.g. $.* for JSONPath wildcard selection
                    //   / (path separator) → e.g. JSON Pointer RFC 6901 paths
                    if (jsonPath.IndexOfAny(new[] { '\'', '"', ';', '\\' }) >= 0)
                    {
                        throw new NormQueryException(
                            $"JSON path '{jsonPath}' contains invalid characters. " +
                            "JSON paths must not contain single-quote, double-quote, semicolon, or backslash.");
                    }

                    // Limit path length to prevent potential DoS via pathological JSON path strings.
                    if (jsonPath.Length > MaxJsonPathLength)
                    {
                        throw new NormQueryException(
                            $"JSON path exceeds maximum length of {MaxJsonPathLength} characters (actual: {jsonPath.Length}).");
                    }

                    var jsonSql = _provider.TranslateJsonPathAccess(columnSql, jsonPath);
                    _sql.Append(jsonSql);
                    return node;
                }
                else
                {
                    throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "JSONPath argument in Json.Value must be a constant string."));
                }
            }
            // String methods are handled by _fastMethodHandlers (see above).
            if (node.Method.DeclaringType == typeof(string))
            {
                // static string.IsNullOrEmpty(x) -> (x IS NULL OR x = '')
                // static string.IsNullOrWhiteSpace(x) -> (x IS NULL OR LTRIM(RTRIM(x)) = '')
                if (node.Object == null && node.Arguments.Count == 1 &&
                    (node.Method.Name == nameof(string.IsNullOrEmpty) || node.Method.Name == nameof(string.IsNullOrWhiteSpace)))
                {
                    var inner = GetSql(node.Arguments[0]);
                    var trimmed = _provider.TranslateFunction(nameof(string.Trim), typeof(string), inner) ?? inner;
                    var target = node.Method.Name == nameof(string.IsNullOrWhiteSpace) ? trimmed : inner;
                    _sql.Append('(').Append(inner).Append(" IS NULL OR ").Append(target).Append(" = '')");
                    return node;
                }
                // static string.Compare(a, b) and instance a.CompareTo(b) -> CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END
                if (node.Method.Name == nameof(string.Compare) || node.Method.Name == nameof(string.CompareTo))
                {
                    string lhs, rhs;
                    if (node.Object == null && node.Arguments.Count == 2)
                    {
                        lhs = GetSql(node.Arguments[0]);
                        rhs = GetSql(node.Arguments[1]);
                    }
                    else if (node.Object != null && node.Arguments.Count == 1)
                    {
                        lhs = GetSql(node.Object);
                        rhs = GetSql(node.Arguments[0]);
                    }
                    else
                    {
                        throw new NormUnsupportedFeatureException(
                            $"Overload of {node.Method.Name} with {node.Arguments.Count} arguments is not supported.");
                    }
                    _sql.Append("(CASE WHEN ").Append(lhs).Append(" < ").Append(rhs)
                        .Append(" THEN -1 WHEN ").Append(lhs).Append(" > ").Append(rhs)
                        .Append(" THEN 1 ELSE 0 END)");
                    return node;
                }
                // static string.Concat(a, b, ...) -> provider-specific concat
                if (node.Object == null && node.Method.Name == nameof(string.Concat) && node.Arguments.Count >= 1)
                {
                    var parts = new List<string>();
                    foreach (var a in node.Arguments)
                    {
                        // string.Concat(params object[]) - skip the array wrapper if present
                        if (a is NewArrayExpression arr)
                        {
                            foreach (var item in arr.Expressions) parts.Add(GetSql(item));
                        }
                        else
                        {
                            parts.Add(GetSql(a));
                        }
                    }
                    var concatSql = parts.Count == 1
                        ? parts[0]
                        : parts.Aggregate((acc, next) => _provider.GetConcatSql(acc, next));
                    _sql.Append(concatSql);
                    return node;
                }
                var strArgs = new List<string>();
                if (node.Object != null)
                    strArgs.Add(GetSql(node.Object));
                foreach (var a in node.Arguments)
                    strArgs.Add(GetSql(a));
                var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, strArgs.ToArray());
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }
                throw new NormUnsupportedFeatureException($"String method '{node.Method.Name}' is not supported.");
            }
            if (node.Method.DeclaringType == typeof(Convert) && node.Arguments.Count == 1)
            {
                var inner = GetSql(node.Arguments[0]);
                var sqlType = node.Method.Name switch
                {
                    nameof(Convert.ToInt32) or nameof(Convert.ToInt16) or nameof(Convert.ToByte) or nameof(Convert.ToSByte) => "INTEGER",
                    nameof(Convert.ToInt64) => "BIGINT",
                    nameof(Convert.ToString) => "TEXT",
                    nameof(Convert.ToDouble) or nameof(Convert.ToSingle) => "REAL",
                    nameof(Convert.ToDecimal) => "DECIMAL",
                    nameof(Convert.ToBoolean) => "BOOLEAN",
                    _ => null
                };
                if (sqlType != null)
                {
                    _sql.Append("CAST(").Append(inner).Append(" AS ").Append(sqlType).Append(')');
                    return node;
                }
            }
            if (node.Method.DeclaringType == typeof(Enumerable) || node.Method.DeclaringType == typeof(Queryable))
            {
                // Detect nested aggregates on a mapped navigation collection in a predicate context,
                // e.g. `parent.Children.Any(c => c.Foo > x)`. The receiver `parent.Children` is a
                // CLR List/IEnumerable, not an IQueryable, so it normally cannot translate. We
                // rewrite it into a correlated subquery on the dependent table joined by the
                // relation's FK, then let the existing Queryable.Any/All/Count handlers run.
                if (node.Arguments.Count >= 1 &&
                    node.Arguments[0] is MemberExpression navMember &&
                    navMember.Expression is ParameterExpression navParent &&
                    _parameterMappings.TryGetValue(navParent, out var navParentInfo) &&
                    navParentInfo.Mapping.Relations.TryGetValue(navMember.Member.Name, out var relation) &&
                    (node.Method.Name is nameof(Queryable.Any)
                                      or nameof(Queryable.All)
                                      or nameof(Queryable.Count)
                                      or nameof(Queryable.LongCount)))
                {
                    var rewritten = RewriteNavigationAggregate(node, navParent, relation);
                    Visit(rewritten);
                    return node;
                }
                switch (node.Method.Name)
                {
                    case nameof(Queryable.GroupBy):
                        HandleGroupByMethod(node);
                        return node;
                    case "Count":
                    case "LongCount":
                        if (node.Arguments.Count >= 1 && node.Arguments[0] is ParameterExpression cp && _parameterMappings.ContainsKey(cp))
                        {
                            if (node.Arguments.Count == 2 && StripQuotes(node.Arguments[1]) is LambdaExpression countSelector)
                            {
                                var info = _parameterMappings[cp];
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, countSelector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var predSql = visitor.Translate(countSelector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                _sql.Append($"COUNT(CASE WHEN {predSql} THEN 1 ELSE NULL END)");
                                FastExpressionVisitorPool.Return(visitor);
                            }
                            else
                            {
                                _sql.Append("COUNT(*)");
                            }
                            return node;
                        }
                        // Count/LongCount over a query source in a predicate context: emit a
                        // correlated scalar subquery (SELECT COUNT(*) FROM child WHERE ...).
                        if (node.Arguments.Count >= 1)
                        {
                            var countLambda = node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null;
                            BuildScalarCountSubquery(node.Arguments[0], countLambda);
                            return node;
                        }
                        break;
                    case "Sum":
                    case "Average":
                    case "Min":
                    case "Max":
                        if (node.Arguments.Count >= 2 && node.Arguments[0] is ParameterExpression gp && _parameterMappings.ContainsKey(gp))
                        {
                            var selector = StripQuotes(node.Arguments[1]) as LambdaExpression;
                            if (selector != null)
                            {
                                var info = _parameterMappings[gp];
                                var vctx = new VisitorContext(_ctx, info.Mapping, _provider, selector.Parameters[0], info.Alias, _parameterMappings, _compiledParams, _paramMap, _recursionDepth);
                                var visitor = FastExpressionVisitorPool.Get(in vctx);
                                var colSql = visitor.Translate(selector.Body);
                                foreach (var kvp in visitor.GetParameters())
                                    _params[kvp.Key] = kvp.Value;
                                var fn = node.Method.Name switch
                                {
                                    "Sum" => "SUM",
                                    "Average" => "AVG",
                                    "Min" => "MIN",
                                    "Max" => "MAX",
                                    _ => "",
                                };
                                _sql.Append($"{fn}({colSql})");
                                FastExpressionVisitorPool.Return(visitor);
                                return node;
                            }
                        }
                        break;
                }
            }
            if (node.Method.Name == nameof(List<int>.Contains))
            {
                Expression? collectionExpr = null;
                Expression? valueExpr = null;
                if (node.Method.DeclaringType == typeof(Enumerable))
                {
                    if (node.Arguments.Count == 2)
                    {
                        collectionExpr = node.Arguments[0];
                        valueExpr = node.Arguments[1];
                    }
                }
                else if (node.Object != null && node.Arguments.Count == 1)
                {
                    collectionExpr = node.Object;
                    valueExpr = node.Arguments[0];
                }
                if (collectionExpr != null && valueExpr != null && TryGetConstantValue(collectionExpr, out var colVal) && colVal is IEnumerable en && colVal is not string)
                {
                    var items = new List<object?>();
                    foreach (var item in en)
                        items.Add(item);
                    if (items.Count == 0)
                    {
                        _sql.Append(SqlFalseLiteral);
                        return node;
                    }

                    // Separate nulls from non-nulls: SQL `col IN (NULL, @p1)` never matches null
                    // rows — only `col IS NULL` does. Emit (col IN (...) OR col IS NULL) when needed.
                    bool hasNulls = items.Any(x => x is null);
                    var nonNullItems = hasNulls ? items.Where(x => x != null).ToList() : items;

                    // All-nulls case: emit col IS NULL with no parameters (IN () is invalid SQL).
                    if (nonNullItems.Count == 0)
                    {
                        Visit(valueExpr);
                        _sql.Append(" IS NULL");
                        return node;
                    }

                    // Exact accounting: _paramIndex tracks all params added so far.
                    // Nulls cost no parameters, so use nonNullItems.Count for the budget check.
                    var remainingParams = _provider.MaxParameters - _paramIndex;
                    if (nonNullItems.Count > remainingParams)
                        throw new NormQueryException(
                            $"IN clause with {nonNullItems.Count} items exceeds remaining parameter budget " +
                            $"({remainingParams} available, {_paramIndex} already used, limit {_provider.MaxParameters}). " +
                            "Consider using a temporary table or reducing the number of items.");

                    // Optimizer batching (1000 items per IN clause for DB plan efficiency).
                    // This is decoupled from parameter limits.
                    // NOTE: When the collection exceeds MaxInClauseItems, each batch re-visits
                    // valueExpr to emit the column reference (e.g., "T0.[Col] IN (...) OR T0.[Col] IN (...)").
                    // This means the SQL string grows linearly with the number of batches (one column
                    // reference per batch). For very large collections this is a deliberate tradeoff:
                    // multiple smaller IN clauses let the query optimizer produce better plans than a
                    // single massive IN list, at the cost of a slightly larger SQL string and plan
                    // cache variance (different collection sizes produce different SQL shapes).
                    const int MaxInClauseItems = 1000;
                    if (nonNullItems.Count > MaxInClauseItems)
                    {
                        if (hasNulls) _sql.Append("(");
                        _sql.Append("(");
                        for (int batch = 0; batch < nonNullItems.Count; batch += MaxInClauseItems)
                        {
                            if (batch > 0) _sql.Append(" OR ");
                            var batchItems = nonNullItems.Skip(batch).Take(MaxInClauseItems);
                            Visit(valueExpr);
                            _sql.Append(" IN (");
                            bool first = true;
                            foreach (var item in batchItems)
                            {
                                if (!first) _sql.Append(", ");
                                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                                _sql.AppendParameterizedValue(paramName, item, _paramSink);
                                first = false;
                            }
                            _sql.Append(")");
                        }
                        _sql.Append(")");
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    else
                    {
                        if (hasNulls) _sql.Append("(");
                        Visit(valueExpr);
                        _sql.Append(" IN (");
                        for (int i = 0; i < nonNullItems.Count; i++)
                        {
                            if (i > 0) _sql.Append(", ");
                            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                            _sql.AppendParameterizedValue(paramName, nonNullItems[i], _paramSink);
                        }
                        _sql.Append(")");
                        if (hasNulls)
                        {
                            _sql.Append(" OR ");
                            Visit(valueExpr);
                            _sql.Append(" IS NULL)");
                        }
                    }
                    return node;
                }
            }
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Any):
                        BuildExists(node.Arguments[0], node.Arguments.Count > 1 ? StripQuotes(node.Arguments[1]) as LambdaExpression : null, negate: false);
                        return node;
                    case nameof(Queryable.All):
                        if (node.Arguments.Count < 2)
                            throw new NormQueryException("All() requires a predicate argument.");
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new NormQueryException("All() requires a predicate lambda expression.");
                        var param = pred.Parameters[0];
                        var notBody = Expression.Not(pred.Body);
                        var lambda = Expression.Lambda(notBody, param);
                        BuildExists(node.Arguments[0], lambda, negate: true);
                        return node;
                    case nameof(Queryable.Contains):
                        BuildIn(node.Arguments[0], node.Arguments[1]);
                        return node;
                    default:
                        throw new NormUnsupportedFeatureException($"Queryable method '{node.Method.Name}' is not supported.");
                }
            }
            var args = new List<string>();
            if (node.Object != null)
                args.Add(GetSql(node.Object));
            foreach (var a in node.Arguments)
                args.Add(GetSql(a));
            var fnSql = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, args.ToArray());
            if (fnSql != null)
            {
                _sql.Append(fnSql);
                return node;
            }
            var custom = node.Method.GetCustomAttribute<SqlFunctionAttribute>();
            if (custom != null)
            {
                var formatted = string.Format(custom.Format, args.ToArray());
                _sql.Append(formatted);
                return node;
            }
            throw new NormUnsupportedFeatureException($"Method '{node.Method.Name}' is not supported.");
        }
        /// <summary>
        /// Rewrites a navigation-aware aggregate call (e.g. <c>parent.Children.Any(...)</c>)
        /// into the equivalent <c>Queryable</c> shape that the existing translator can consume:
        /// <c>NormQueryable.Query&lt;Child&gt;(ctx).Where(c => c.FK == parent.PK).Any(...)</c>.
        /// The outer parameter reference inside the FK join condition is preserved so the
        /// translator emits a correlated subquery instead of an independent SELECT.
        /// </summary>
        private MethodCallExpression RewriteNavigationAggregate(
            MethodCallExpression originalCall,
            ParameterExpression parentParam,
            TableMapping.Relation relation)
        {
            var depType = relation.DependentType;

            // Materialize the dependent IQueryable upfront so the sub-translator sees a
            // ConstantExpression<IQueryable<Child>> at the root - that is the shape its
            // VisitConstant recognizes as the query source. Building the expression as
            // `Expression.Call(NormQueryable.Query, ctxConstant)` would emit the DbContext
            // as a SQL parameter instead.
            var queryMethod = typeof(NormQueryable).GetMethod(nameof(NormQueryable.Query))!
                .MakeGenericMethod(depType);
            var dependentQuery = queryMethod.Invoke(null, new object[] { _ctx })!;
            var sourceExpr = (Expression)Expression.Constant(dependentQuery, typeof(IQueryable<>).MakeGenericType(depType));

            // Build the FK = PK predicate against the dependent's property.
            var childParam = Expression.Parameter(depType, "__nav_" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Expression fkAccess = Expression.Property(childParam, relation.ForeignKey.Prop);
            Expression pkAccess = Expression.Property(parentParam, relation.PrincipalKey.Prop);
            // Promote nullable mismatches so Expression.Equal type-checks (e.g. nullable FK
            // referring to a non-nullable PK).
            if (fkAccess.Type != pkAccess.Type)
            {
                var common = Nullable.GetUnderlyingType(fkAccess.Type) ?? fkAccess.Type;
                if (fkAccess.Type != common) fkAccess = Expression.Convert(fkAccess, common);
                if (pkAccess.Type != common) pkAccess = Expression.Convert(pkAccess, common);
            }
            var fkPredicate = Expression.Lambda(Expression.Equal(fkAccess, pkAccess), childParam);

            sourceExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                new[] { depType }, sourceExpr, Expression.Quote(fkPredicate));

            // Re-emit the aggregate call (Any/All/Count/LongCount) against the synthesized
            // Queryable source. The original predicate (if any) is the second arg.
            var methodName = originalCall.Method.Name;
            if (originalCall.Arguments.Count > 1)
            {
                var innerPredicate = StripQuotes(originalCall.Arguments[1]) as LambdaExpression
                    ?? throw new NormQueryException(
                        $"{methodName}() on a navigation collection requires a lambda predicate argument.");
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 2)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr, Expression.Quote(innerPredicate));
            }
            else
            {
                var queryableMethod = typeof(Queryable).GetMethods()
                    .First(m => m.Name == methodName && m.GetParameters().Length == 1)
                    .MakeGenericMethod(depType);
                return Expression.Call(queryableMethod, sourceExpr);
            }
        }

        private void BuildExists(Expression source, LambdaExpression? predicate, bool negate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            // Separate tempParams dict so subTranslator.Dispose() (which Resets its Parameters)
            // does not wipe params shared with the outer query. Copy collected params back before dispose.
            var tempParams = new Dictionary<string, object>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;
            _sql.Append(negate ? "NOT EXISTS(" : "EXISTS(");
            _sql.Append(subPlan.Sql);
            _sql.Append(")");
        }
        private void BuildScalarCountSubquery(Expression source, LambdaExpression? predicate)
        {
            if (predicate != null)
            {
                var et = GetElementType(source);
                source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { et }, source, Expression.Quote(predicate));
            }
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var tempParams = new Dictionary<string, object>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex, _parameterMappings, new HashSet<string>(), _compiledParams, _paramMap, _parameterMappings.Count, recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;

            // Rewrite the entity SELECT into SELECT COUNT(*) by replacing everything before the
            // first ` FROM `. Strip any trailing ORDER BY which is meaningless inside a scalar
            // subquery and disallowed by some providers.
            var sql = subPlan.Sql;
            var fromIdx = sql.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIdx < 0)
                throw new NormQueryException("Could not rewrite Count() subquery: no FROM clause found.");
            var tail = sql.Substring(fromIdx);
            var orderIdx = tail.LastIndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderIdx >= 0)
                tail = tail.Substring(0, orderIdx);
            _sql.Append("(SELECT COUNT(*)");
            _sql.Append(tail);
            _sql.Append(")");
        }
        private void BuildIn(Expression source, Expression value)
        {
            // Extract expression from closure-captured IQueryable.
            if (TryGetConstantValue(source, out var srcConstValue) && srcConstValue is System.Linq.IQueryable iqSrc)
                source = iqSrc.Expression!;

            // Compile-time null: ConstantExpression{null} OR Convert(null, T?) (UnaryExpression).
            bool isNullValue = (value is ConstantExpression { Value: null })
                || (value is UnaryExpression { NodeType: ExpressionType.Convert } ueNull
                    && ueNull.Operand is ConstantExpression { Value: null });
            if (isNullValue)
            {
                BuildNullExistsForContains(source);
                return;
            }

            // Build the IN (subquery) with a fresh correlated dict so inner lambda params get T0
            // regardless of what the outer _parameterMappings contains.
            // Use a separate tempParams dict: QueryTranslator.Dispose() clears its Parameters dict —
            // if shared with _params, collected params are lost.
            var rootType = GetRootElementType(source);
            var mapping = _ctx.GetMapping(rootType);
            var freshCorrelatedForIn = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var tempParams = new Dictionary<string, object>();
            using var subTranslator = QueryTranslator.Create(_ctx, mapping, tempParams, _paramIndex,
                freshCorrelatedForIn, new HashSet<string>(), _compiledParams, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var subPlan = subTranslator.Translate(source);
            _paramIndex = subTranslator.ParameterIndex;
            // Copy collected params to outer _params BEFORE subTranslator.Dispose() clears tempParams.
            foreach (var kvp in tempParams)
                _params[kvp.Key] = kvp.Value;

            // SQL NULL IN (...) is UNKNOWN (not TRUE); emit null-safe OR pattern for nullable value types.
            bool isNullable = !value.Type.IsValueType || Nullable.GetUnderlyingType(value.Type) != null;

            if (!isNullable)
            {
                Visit(value);
                _sql.Append(" IN (");
                _sql.Append(subPlan.Sql);
                _sql.Append(")");
                return;
            }

            // Runtime nullable: (val IN (subq) OR (val IS NULL AND EXISTS(null-filtered subq)))
            var valueSql = GetSql(value);
            _sql.Append("(");
            _sql.Append(valueSql);
            _sql.Append(" IN (");
            _sql.Append(subPlan.Sql);
            _sql.Append(") OR (");
            _sql.Append(valueSql);
            _sql.Append(" IS NULL AND ");
            BuildNullExistsForContains(source);
            _sql.Append("))");
        }

        // Emits EXISTS(SELECT ... FROM source WHERE col IS NULL).
        // Uses a fresh correlated dict so the EXISTS translator starts clean with T0 for all params.
        private void BuildNullExistsForContains(Expression source)
        {
            // Walk back through Select calls to find the entity-level query and selector.
            LambdaExpression? innerSelector = null;
            var cursor = source;
            while (cursor is MethodCallExpression mce && mce.Method.Name == "Select")
            {
                innerSelector = StripQuotes(mce.Arguments[1]) as LambdaExpression;
                cursor = mce.Arguments[0];
            }

            // Append a WHERE col IS NULL filter at the entity level.
            Expression filteredSource;
            Type entityType;
            if (innerSelector == null)
            {
                // No Select: the source IS the entity query; filter entities where they are null.
                entityType = GetElementType(source);
                var p = Expression.Parameter(entityType, "__nc");
                var isNull = Expression.Equal(p, Expression.Constant(null, entityType));
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, source, Expression.Quote(Expression.Lambda(isNull, p)));
            }
            else
            {
                // Has Select: add a null-check on the projected column at the entity level.
                // WhereTranslator does not increment _joinCounter, so innerSelector.Parameters[0]
                // and cursor's lambda params all get T0 in the fresh-dict EXISTS translator.
                entityType = innerSelector.Parameters[0].Type;
                var nullCheck = Expression.Lambda(
                    Expression.Equal(innerSelector.Body, Expression.Constant(null, innerSelector.ReturnType)),
                    innerSelector.Parameters[0]);
                filteredSource = Expression.Call(typeof(Queryable), nameof(Queryable.Where),
                    new[] { entityType }, cursor, Expression.Quote(nullCheck));
            }

            // Fresh correlated dict and separate tempParams so EXISTS translator never sees
            // stale aliases, and Dispose() clearing its Parameters dict doesn't affect _params.
            var freshCorrelated = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
            var rootType = GetRootElementType(filteredSource);
            var mapping = _ctx.GetMapping(rootType);
            var existsTempParams = new Dictionary<string, object>();
            using var existsTranslator = QueryTranslator.Create(
                _ctx, mapping, existsTempParams, _paramIndex,
                freshCorrelated, new HashSet<string>(),
                _compiledParams, _paramMap, 0,
                recursionDepth: _recursionDepth + 1);
            var existsPlan = existsTranslator.Translate(filteredSource);
            _paramIndex = existsTranslator.ParameterIndex;
            // Copy collected params to outer _params BEFORE existsTranslator.Dispose() clears existsTempParams.
            foreach (var kvp in existsTempParams)
                _params[kvp.Key] = kvp.Value;
            _sql.Append("EXISTS(");
            _sql.Append(existsPlan.Sql);
            _sql.Append(")");
        }

        /// <summary>
        /// Declaring types whose methods can be translated to SQL. Frozen at startup
        /// to avoid per-call allocation and to enable O(1) lookup.
        /// </summary>
        private static readonly FrozenSet<Type> s_safeDeclaringTypes = new HashSet<Type>
        {
            typeof(string), typeof(Math), typeof(DateTime), typeof(Convert),
            typeof(Enumerable), typeof(Queryable), typeof(Json)
        }.ToFrozenSet();

        /// <summary>
        /// Object-identity methods that must never be translated to SQL because they
        /// rely on CLR runtime semantics with no SQL equivalent.
        /// </summary>
        private static readonly FrozenSet<string> s_untranslatableMethods = new HashSet<string>
        {
            nameof(object.GetType), nameof(object.ToString), nameof(object.GetHashCode)
        }.ToFrozenSet();

        private static bool IsTranslatableMethod(MethodInfo method)
        {
            if (method.GetCustomAttribute<SqlFunctionAttribute>() != null)
                return true;
            if (method.DeclaringType == null || !s_safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            // System.Convert.ToString is intentionally translatable as CAST(... AS TEXT);
            // it's only the default object.ToString that has no SQL equivalent.
            if (method.DeclaringType == typeof(Convert) && method.Name == nameof(Convert.ToString))
                return true;
            return !s_untranslatableMethods.Contains(method.Name);
        }
        private Expression TranslateWithNullCheck(MethodCallExpression node)
        {
            if (node.Object == null) return base.VisitMethodCall(node);
            _sql.Append("(CASE WHEN ");
            Visit(node.Object);
            _sql.Append(" IS NULL THEN NULL ELSE ");
            _suppressNullCheck = true;
            var result = VisitMethodCall(node);
            _suppressNullCheck = false;
            _sql.Append(" END)");
            return result;
        }
        private bool RequiresNullCheck(MethodCallExpression node)
        {
            if (node.Object == null)
                return false;
            if (node.Method.DeclaringType == typeof(string))
                return false;
            return !node.Object.Type.IsValueType || Nullable.GetUnderlyingType(node.Object.Type) != null;
        }
        /// <summary>
        /// Attempts to extract a compile-time constant from the expression, catching
        /// expected reflection failures without propagating them to the caller.
        /// </summary>
        private static bool TryGetConstantValueSafe(Expression expr, out object? value)
        {
            try
            {
                return TryGetConstantValue(expr, out value);
            }
            catch (Exception ex) when (ex is TargetInvocationException or ArgumentException)
            {
                value = null;
                return false;
            }
        }
        private void AppendConstant(object? value, Type type)
        {
            var key = new ConstKey(value, type);
            if (_constParamMap.TryGetValue(key, out var existing))
            {
                _sql.Append(existing);
                return;
            }
            var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
            _sql.AppendParameterizedValue(paramName, value, _paramSink);
            if (_constParamMap.Count >= ConstParamMapLimit)
                _constParamMap.Clear();
            _constParamMap[key] = paramName;
        }
        private Expression CreateSafeParameter(object? value)
        {
            if (value is string str && str.Length > MaxInlineParameterLength)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"String parameter exceeds maximum length of {MaxInlineParameterLength} characters"));
            if (value is byte[] bytes && bytes.Length > MaxInlineParameterLength)
                throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed,
                    $"Binary parameter exceeds maximum length of {MaxInlineParameterLength} bytes"));
            AppendConstant(value, value?.GetType() ?? typeof(object));
            // Returning a cached empty expression avoids allocating a new
            // Expression instance for each constant value translated. The
            // actual value has already been written directly to the
            // parameter collection in AppendConstant, so no further
            // expression tree representation is required here.
            return s_emptyExpression;
        }
        private enum LikeOperation
        {
            Contains,
            StartsWith,
            EndsWith
        }
        private readonly struct ConstKey : IEquatable<ConstKey>
        {
            public readonly object? Value;
            public readonly Type? Type;
            public ConstKey(object? value, Type? type)
            {
                Value = value;
                Type = type;
            }

            /// <summary>
            /// Determines equality with another <see cref="ConstKey"/> based on both the value and
            /// the associated type.
            /// </summary>
            public bool Equals(ConstKey other) => Equals(Value, other.Value) && Type == other.Type;

            /// <summary>
            /// Determines whether the specified object is equal to the current <see cref="ConstKey"/>.
            /// </summary>
            public override bool Equals(object? obj) => obj is ConstKey other && Equals(other);

            /// <summary>
            /// Generates a hash code combining the value and type components.
            /// </summary>
            public override int GetHashCode() => HashCode.Combine(Value, Type);
        }
        private string CreateSafeLikePattern(string value, LikeOperation operation)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;

            // DOS PROTECTION FIX: Validate pattern length to prevent database CPU spike
            // Extremely long LIKE patterns (millions of '%' chars) can cause severe performance issues
            const int MaxLikePatternLength = 5000;
            if (value.Length > MaxLikePatternLength)
            {
                throw new NormQueryException(
                    $"LIKE pattern too long ({value.Length} characters). Maximum allowed: {MaxLikePatternLength}. " +
                    $"Long LIKE patterns can cause database performance issues.");
            }

            var escaped = _provider.EscapeLikePattern(value);
            return operation switch
            {
                LikeOperation.Contains => $"%{escaped}%",
                LikeOperation.StartsWith => $"{escaped}%",
                LikeOperation.EndsWith => $"%{escaped}",
                _ => escaped
            };
        }
        private static Type GetRootElementType(Expression source)
        {
            while (source is MethodCallExpression mce)
            {
                // ctx.Query<T>() shows up as either an instance-style call (zero arguments) or
                // as the static extension-style NormQueryable.Query<T>(ctx) (one argument).
                // Both are the root of a query expression - stop walking and return T.
                if (mce.Method.Name == "Query"
                    && (mce.Arguments.Count == 0
                        || mce.Method.DeclaringType == typeof(NormQueryable)))
                    return GetElementType(mce);
                source = mce.Arguments[0];
            }
            return GetElementType(source);
        }
        /// <summary>
        /// Retrieves the parameter dictionary that has been populated while
        /// translating an expression tree to its SQL representation.
        /// </summary>
        /// <remarks>
        /// The returned dictionary contains parameter names and values that are
        /// emitted during translation.  The caller can reuse this collection when
        /// executing the generated SQL.
        /// </remarks>
        /// <returns>
        /// A reference to the internal dictionary of SQL parameters.  The
        /// contents should be treated as read-only by callers to avoid
        /// interfering with further translations.
        /// </returns>
        public Dictionary<string, object> GetParameters() => _params;
        private string GetSql(Expression expression)
        {
            var start = _sql.Length;
            Visit(expression);
            var segment = _sql.ToString(start, _sql.Length - start);
            _sql.Remove(start, _sql.Length - start);
            return segment;
        }
        /// <summary>
        /// Delegates to the shared ExpressionValueExtractor utility for consistent behavior.
        /// </summary>
        private static bool TryGetConstantValue(Expression e, out object? value, HashSet<Expression>? visited = null)
            => ExpressionValueExtractor.TryGetConstantValue(e, out value, visited);
        private static Expression StripQuotes(Expression e)
            => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;
        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }
            var iface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            if (iface != null) return iface.GetGenericArguments()[0];
            throw new ArgumentException($"Cannot determine element type from expression of type {type}");
        }
        private static void HandleStringContains(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var contains) && contains is string cs)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(cs, LikeOperation.Contains), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(visitor._provider.GetConcatSql("'%'", visitor._provider.GetConcatSql(escapedSql, "'%'")));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        private static void HandleStringStartsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var starts) && starts is string ss)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(ss, LikeOperation.StartsWith), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(visitor._provider.GetConcatSql(escapedSql, "'%'"));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        private static void HandleStringEndsWith(ExpressionToSqlVisitor visitor, MethodCallExpression node)
        {
            visitor.Visit(node.Object!);
            visitor._sql.Append(" LIKE ");

            // Support both constant and variable values
            if (TryGetConstantValue(node.Arguments[0], out var ends) && ends is string es)
            {
                // Fast path for constants: pre-build the pattern
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                visitor.AppendConstant(visitor.CreateSafeLikePattern(es, LikeOperation.EndsWith), typeof(string));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
            else
            {
                // Variable path: escape the value at runtime to prevent SQL injection
                var escChar = NormValidator.ValidateLikeEscapeChar(visitor._provider.LikeEscapeChar);
                var escapedSql = visitor._provider.GetLikeEscapeSql(visitor.GetSql(node.Arguments[0]));
                visitor._sql.Append(visitor._provider.GetConcatSql("'%'", escapedSql));
                visitor._sql.Append($" ESCAPE '{escChar}'");
            }
        }
        // ContainsTranslator, StartsWithTranslator, and EndsWithTranslator were consolidated into
        // _fastMethodHandlers. String methods are exclusively handled there.

        /// <summary>
        /// Directs the visitor to use the provided dictionary for parameter
        /// collection, allowing multiple visitors to share a common parameter
        /// store.
        /// </summary>
        /// <param name="shared">The dictionary to populate with parameters, or
        /// <c>null</c> to revert to the visitor's internal dictionary.</param>
        public void UseSharedParameterDictionary(Dictionary<string, object> shared)
        {
            _paramSink = shared ?? _params;
        }

        /// <summary>
        /// Registers a SQL expression for the <c>Key</c> property of a grouping parameter,
        /// so that subsequent <c>g.Key</c> accesses emit the correct SQL column reference.
        /// </summary>
        public void RegisterGroupingKey(ParameterExpression parameter, string keySql)
        {
            _groupingKeys[parameter] = keySql;
        }

        /// <summary>
        /// Translates a <c>GroupBy</c> method call into the corresponding SQL <c>GROUP BY</c>
        /// clause, registering grouping key bindings for downstream aggregate expressions.
        /// </summary>
        private void HandleGroupByMethod(MethodCallExpression node)
        {
            var keySelector = StripQuotes(node.Arguments[1]) as LambdaExpression
                ?? throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, "GroupBy key selector must be a lambda expression"));

            var keySql = GetSql(keySelector.Body);

            // Register grouping scope for downstream accesses to g.Key
            if (node.Arguments.Count > 2 && StripQuotes(node.Arguments[2]) is LambdaExpression resultSelector)
            {
                if (resultSelector.Parameters.Count > 1)
                {
                    RegisterGroupingKey(resultSelector.Parameters[1], keySql);
                    Visit(resultSelector.Body);
                }
                else
                {
                    Visit(resultSelector.Body);
                }
            }
            else
            {
                _sql.Append(keySql);
            }

            _sql.AppendGroupBy(keySql);
        }



        /// <summary>
        /// Quickly resets the visitor to a clean state so that it can be reused
        /// without allocating a new instance.
        /// </summary>
        /// <remarks>
        /// This method clears accumulated SQL, parameters, and internal caches
        /// while preserving preallocated buffers when possible.
        /// </remarks>
        public void FastReset()
        {
            if (_sql != null) _sql.Clear();
            _params.Clear();
            if (_paramSink != null && !ReferenceEquals(_paramSink, _params))
                _paramSink.Clear();
            _paramIndex = 0;
            _suppressNullCheck = false;
            _constParamMap.Clear();
            _memberParamMap.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
        }

    }
}
