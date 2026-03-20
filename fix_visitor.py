#!/usr/bin/env python3
import sys

with open('src/nORM/Query/ExpressionToSqlVisitor.cs', 'r', encoding='utf-8-sig') as f:
    content = f.read()

original = content

# Fix 1: Remove unused System.Diagnostics import if present
content = content.replace('using System.Diagnostics;\n', '')

# Fix 2: Add _paramSink = null! to Reset()
content = content.replace(
    '            _sql = null!;\n            _params.Clear();\n            _paramIndex = 0;',
    '            _sql = null!;\n            _params.Clear();\n            _paramSink = null!;\n            _paramIndex = 0;'
)

# Fix 3: Rename _constParamMapLimit to ConstParamMapLimit (C# const naming convention)
content = content.replace('_constParamMapLimit', 'ConstParamMapLimit')

# Fix 4: Rename _emptyExpression to s_emptyExpression (static field convention)
content = content.replace('_emptyExpression', 's_emptyExpression')

# Fix 5: Fix bare catch in IsNullExpression -> narrow to specific reflection exceptions
content = content.replace(
    '                catch { return false; }',
    """                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures -- conservatively report as non-null so the
                    // caller does not emit an incorrect IS NULL predicate.
                    return false;
                }"""
)

# Fix 6: Fix bare catch in CouldBeNull -> narrow to specific reflection exceptions
content = content.replace(
    '                catch { return true; }',
    """                catch (Exception ex) when (ex is TargetInvocationException or MemberAccessException or InvalidOperationException or ArgumentException)
                {
                    // Reflection failures -- assume nullable to preserve correctness.
                    return true;
                }"""
)

# Fix 7: Fix ALL-CAPS comment
content = content.replace('// ADD FAST PATH FOR COMMON METHODS', '// Fast path for common string methods (Contains, StartsWith, EndsWith)')

# Fix 8: Fix bare Q1 ticket reference
content = content.replace('// Q1: Nullable column-vs-column', '// Nullable column-vs-column')

# Fix 9: Fix bare Q1 ticket reference in doc comment
content = content.replace('Used by Q1 to detect', 'Used to detect')

# Fix 10: Fix misleading comment about string methods
content = content.replace(
    '// String methods are handled by _fastMethodHandlers (see above).',
    '// Additional string methods not in _fastMethodHandlers (e.g. ToUpper, ToLower, Trim)\n            // are translated here via the provider\'s TranslateFunction.'
)

# Fix 11: Remove stale comment about consolidated translators
content = content.replace(
    '        // ContainsTranslator, StartsWithTranslator, and EndsWithTranslator were consolidated into\n        // _fastMethodHandlers. String methods are exclusively handled there.\n',
    ''
)

# Fix 12: Fix null-forgiving on DeclaringType in VisitMember
old_member = """                var exprSql = GetSql(node.Expression);
                var fn = _provider.TranslateFunction(node.Member.Name, node.Member.DeclaringType!, exprSql);
                if (fn != null)
                {
                    _sql.Append(fn);
                    return node;
                }"""
new_member = """                var exprSql = GetSql(node.Expression);
                var declaringType = node.Member.DeclaringType;
                if (declaringType != null)
                {
                    var fn = _provider.TranslateFunction(node.Member.Name, declaringType, exprSql);
                    if (fn != null)
                    {
                        _sql.Append(fn);
                        return node;
                    }
                }"""
content = content.replace(old_member, new_member)

# Fix 13: Fix null-forgiving on DeclaringType in generic method handler
old_generic = """            var fnSql = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, args.ToArray());
            if (fnSql != null)
            {
                _sql.Append(fnSql);
                return node;
            }"""
new_generic = """            var declaringType2 = node.Method.DeclaringType;
            if (declaringType2 != null)
            {
                var fnSql = _provider.TranslateFunction(node.Method.Name, declaringType2, args.ToArray());
                if (fnSql != null)
                {
                    _sql.Append(fnSql);
                    return node;
                }
            }"""
content = content.replace(old_generic, new_generic)

# Fix 14: Extract IsTranslatableMethod sets to static fields, make method static
old_translatable = """        private bool IsTranslatableMethod(MethodInfo method)
        {
            if (method.GetCustomAttribute<SqlFunctionAttribute>() != null)
                return true;
            var safeDeclaringTypes = new HashSet<Type>
            {
                typeof(string), typeof(Math), typeof(DateTime), typeof(Convert), typeof(Enumerable), typeof(Queryable), typeof(Json)
            };
            if (method.DeclaringType == null || !safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            var dangerousMethods = new HashSet<string>
            {
                "GetType", "ToString", "GetHashCode"
            };
            return !dangerousMethods.Contains(method.Name);
        }"""
new_translatable = """        private static bool IsTranslatableMethod(MethodInfo method)
        {
            if (method.GetCustomAttribute<SqlFunctionAttribute>() != null)
                return true;
            if (method.DeclaringType == null || !s_safeDeclaringTypes.Contains(method.DeclaringType))
                return false;
            return !s_untranslatableMethods.Contains(method.Name);
        }"""
content = content.replace(old_translatable, new_translatable)

# Fix 15: Make TranslateWithNullCheck exception-safe with try/finally
old_null_check = """            _suppressNullCheck = true;
            var result = VisitMethodCall(node);
            _suppressNullCheck = false;
            _sql.Append(" END)");
            return result;"""
new_null_check = """            _suppressNullCheck = true;
            try
            {
                var result = VisitMethodCall(node);
                _sql.Append(" END)");
                return result;
            }
            finally
            {
                _suppressNullCheck = false;
            }"""
content = content.replace(old_null_check, new_null_check)

# Fix 16: Make RequiresNullCheck static (no instance state used)
content = content.replace('private bool RequiresNullCheck', 'private static bool RequiresNullCheck')

# Fix 17: Remove dead maxDepth parameter from TryGetConstantValueSafe, make static
old_safe = """        private bool TryGetConstantValueSafe(Expression expr, out object? value, int maxDepth = 5)
        {
            if (maxDepth <= 0)
            {
                value = null;
                return false;
            }
            try"""
new_safe = """        private static bool TryGetConstantValueSafe(Expression expr, out object? value)
        {
            try"""
content = content.replace(old_safe, new_safe)

# Fix 18: Magic number 8000 -> named constant
content = content.replace(
    'if (value is string str && str.Length > 8000)',
    'if (value is string str && str.Length > MaxSafeParameterLength)'
)
content = content.replace(
    'string.Format(ErrorMessages.QueryTranslationFailed, "String parameter exceeds maximum length")',
    'string.Format(ErrorMessages.QueryTranslationFailed, $"String parameter exceeds maximum length of {MaxSafeParameterLength}")'
)
content = content.replace(
    'if (value is byte[] bytes && bytes.Length > 8000)',
    'if (value is byte[] bytes && bytes.Length > MaxSafeParameterLength)'
)
content = content.replace(
    'string.Format(ErrorMessages.QueryTranslationFailed, "Binary parameter exceeds maximum length")',
    'string.Format(ErrorMessages.QueryTranslationFailed, $"Binary parameter exceeds maximum length of {MaxSafeParameterLength}")'
)

# Fix 19: Magic number 500 -> named constant for JSON path length
content = content.replace(
    'if (jsonPath.Length > 500)',
    'if (jsonPath.Length > MaxJsonPathLength)'
)
content = content.replace(
    '$"JSON path exceeds maximum length of 500 characters (actual: {jsonPath.Length})."',
    '$"JSON path exceeds maximum length of {MaxJsonPathLength} characters (actual: {jsonPath.Length})."'
)

# Fix 20: Extract JSON forbidden chars to static array
content = content.replace(
    """if (jsonPath.IndexOfAny(new[] { '\\'', '"', ';', '\\\\' }) >= 0)""",
    'if (jsonPath.IndexOfAny(s_jsonPathForbiddenChars) >= 0)'
)

# Fix 21: Remove local MaxInClauseItems constant (now class-level)
content = content.replace(
    """                    // Optimizer batching (1000 items per IN clause for DB plan efficiency).
                    // This is decoupled from parameter limits.
                    const int MaxInClauseItems = 1000;""",
    '                    // Optimizer batching: split large IN clauses for better DB plan efficiency.'
)

# Fix 22: Replace LINQ Skip/Take with index-based batching for performance
old_batch = """                            var batchItems = nonNullItems.Skip(batch).Take(MaxInClauseItems);
                            Visit(valueExpr);
                            _sql.Append(" IN (");
                            bool first = true;
                            foreach (var item in batchItems)
                            {
                                if (!first) _sql.Append(", ");
                                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                                _sql.AppendParameterizedValue(paramName, item, _paramSink);
                                first = false;
                            }"""
new_batch = """                            int batchEnd = Math.Min(batch + MaxInClauseItems, nonNullItems.Count);
                            Visit(valueExpr);
                            _sql.Append(" IN (");
                            for (int bi = batch; bi < batchEnd; bi++)
                            {
                                if (bi > batch) _sql.Append(", ");
                                var paramName = $"{_provider.ParamPrefix}p{_paramIndex++}";
                                _sql.AppendParameterizedValue(paramName, nonNullItems[bi], _paramSink);
                            }"""
content = content.replace(old_batch, new_batch)

# Fix 23: Fix All() guard and error message consistency
old_all = """                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new ArgumentException("All requires a predicate");"""
new_all = """                        if (node.Arguments.Count < 2)
                            throw new NormQueryException("All() requires a predicate argument.");
                        var pred = StripQuotes(node.Arguments[1]) as LambdaExpression;
                        if (pred == null) throw new NormQueryException("All() requires a predicate lambda expression.");"""
content = content.replace(old_all, new_all)

# Fix 24: Add null guard to FastReset for _paramSink (could be null after Reset)
content = content.replace(
    'if (!ReferenceEquals(_paramSink, _params))',
    'if (_paramSink != null && !ReferenceEquals(_paramSink, _params))'
)

# Fix 25: FastReset missing _groupingKeys.Clear()
old_fast_reset_end = """            _constParamMap.Clear();
            _memberParamMap.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
        }"""
new_fast_reset_end = """            _constParamMap.Clear();
            _memberParamMap.Clear();
            _groupingKeys.Clear();
            _ownedCompiledParams.Clear();
            _ownedParamMap.Clear();
        }"""
content = content.replace(old_fast_reset_end, new_fast_reset_end)

# Fix 26: Remove double blank lines
while '\n\n\n' in content:
    content = content.replace('\n\n\n', '\n\n')

# Fix 27: Add List capacity hints
content = content.replace('var strArgs = new List<string>();', 'var strArgs = new List<string>(node.Arguments.Count + 1);')
content = content.replace('var args = new List<string>();', 'var args = new List<string>(node.Arguments.Count + 1);')

# Fix 28: Add unreachable-arm guard in aggregate switch
content = content.replace(
    """                                    "Max" => "MAX",
                                    _ => "",""",
    """                                    "Max" => "MAX",
                                    _ => throw new NotSupportedException($"Aggregate method '{node.Method.Name}' is not supported."),"""
)

# Fix 29: Fix JSON path comment - remove bare Q1 reference
content = content.replace('// Q1 fix: validate JSON path for SQL-injection-capable chars only.', '// Validate JSON path for SQL-injection-capable characters.')

# Fix 30: Remove bare label comment
content = content.replace('                    // TYPE SAFETY FIX: Validate JSON path format to prevent SQL injection and runtime errors\n', '')

# Fix 31: Remove inline comment with const
content = content.replace(
    """            // DOS PROTECTION FIX: Validate pattern length to prevent database CPU spike
            // Extremely long LIKE patterns (millions of '%' chars) can cause severe performance issues
            const int MaxLikePatternLength = 5000;""",
    ''
)

# Now add the static field declarations and named constants
# Insert after _groupingKeys
insert_marker = '        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();'
new_fields = """        private readonly Dictionary<ParameterExpression, string> _groupingKeys = new();
        /// <summary>Maximum length for string/byte[] safe parameters (SQL Server non-MAX threshold).</summary>
        private const int MaxSafeParameterLength = 8000;
        /// <summary>Maximum JSON path length to prevent DoS via deeply nested path expressions.</summary>
        private const int MaxJsonPathLength = 500;
        /// <summary>Items per IN clause batch for query optimizer plan efficiency.</summary>
        private const int MaxInClauseItems = 1000;
        /// <summary>Maximum LIKE pattern length to prevent database CPU spikes.</summary>
        private const int MaxLikePatternLength = 5000;
        /// <summary>Characters forbidden in JSON paths to prevent SQL injection.</summary>
        private static readonly char[] s_jsonPathForbiddenChars = { '\\'', '"', ';', '\\\\' };"""
content = content.replace(insert_marker, new_fields, 1)

# Add static sets after _fastMethodHandlers closing };
# Find the right location - after HandleStringEndsWith }
insert_after = """                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };"""
new_sets = """                { typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!, HandleStringEndsWith }
            };
        /// <summary>Types whose methods are permitted for SQL translation.</summary>
        private static readonly HashSet<Type> s_safeDeclaringTypes = new()
        {
            typeof(string), typeof(Math), typeof(DateTime), typeof(Convert),
            typeof(Enumerable), typeof(Queryable), typeof(Json)
        };
        /// <summary>Method names never translated to SQL (expose CLR internals with no SQL equivalent).</summary>
        private static readonly HashSet<string> s_untranslatableMethods = new()
        {
            "GetType", "ToString", "GetHashCode"
        };"""
content = content.replace(insert_after, new_sets, 1)

# Fix 32: Fix string method DeclaringType null-forgiving
content = content.replace(
    "var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType!, strArgs.ToArray());",
    "var fn = _provider.TranslateFunction(node.Method.Name, node.Method.DeclaringType, strArgs.ToArray());"
)

with open('src/nORM/Query/ExpressionToSqlVisitor.cs', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'Done. Original: {len(original)} chars, New: {len(content)} chars')
