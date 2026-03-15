using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.ObjectPool;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using nORM.Providers;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides runtime validation helpers for entities, bulk operations and raw SQL statements
    /// to guard against misuse and excessively large payloads.
    /// </summary>
    public static class NormValidator
    {
        private const int MaxEntityDepth = 10;
        private const int MaxCollectionSize = 10000;
        private const int MaxBulkOperationSize = 50000;
        private const int MaxParameterCount = 2000;
        private const int MaxSqlLength = 100000;

        private static readonly ObjectPool<HashSet<object>> HashSetPool =
            new DefaultObjectPool<HashSet<object>>(new HashSetPolicy(), Environment.ProcessorCount * 2);
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertyCache = new();

        /// <summary>
        /// Validates a single entity instance to ensure it does not contain excessively deep
        /// or cyclic graphs that could lead to stack overflows or performance issues.
        /// </summary>
        /// <typeparam name="T">Type of the entity being validated.</typeparam>
        /// <param name="entity">Entity instance to validate.</param>
        /// <param name="parameterName">Name of the parameter for exception messages.</param>
        public static void ValidateEntity<T>(T entity, string parameterName = "entity") where T : class
        {
            if (entity == null)
                throw new ArgumentNullException(parameterName);

            var visited = RentHashSet();
            try
            {
                ValidateEntityGraph(entity!, visited, parameterName);
            }
            finally
            {
                ReturnHashSet(visited);
            }
        }

        /// <summary>
        /// Retrieves a <see cref="HashSet{Object}"/> instance from the object pool
        /// used to track visited entities during validation. The set is configured for
        /// reference equality to correctly handle duplicate object references.
        /// </summary>
        /// <returns>A rented hash set instance.</returns>
        private static HashSet<object> RentHashSet() => HashSetPool.Get();

        /// <summary>
        /// Returns a previously rented hash set to the pool for reuse.
        /// </summary>
        /// <param name="set">The hash set to return.</param>
        private static void ReturnHashSet(HashSet<object> set) => HashSetPool.Return(set);

        private sealed class HashSetPolicy : PooledObjectPolicy<HashSet<object>>
        {
            /// <summary>
            /// Creates a new <see cref="HashSet{T}"/> configured with reference equality for tracking visited entities.
            /// </summary>
            /// <returns>A fresh hash set instance.</returns>
            public override HashSet<object> Create()
                => new HashSet<object>(ReferenceEqualityComparer.Instance);

            /// <summary>
            /// Resets the given hash set so it can be reused by the pool.
            /// </summary>
            /// <param name="obj">The hash set to reset.</param>
            /// <returns>Always <c>true</c> to indicate the object may be reused.</returns>
            public override bool Return(HashSet<object> obj)
            {
                obj.Clear();
                return true;
            }
        }

        private static PropertyInfo[] GetCachedProperties(Type type)
            => PropertyCache.GetOrAdd(type, t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance));

        /// <summary>
        /// PERF: Cache of properties whose declared type could contain entity graph references.
        /// Excludes value types and strings, avoiding ~90% of GetValue reflection calls
        /// for typical flat entities (e.g., BenchmarkUser with int/string/DateTime/bool/double).
        /// </summary>
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> NavigablePropertyCache = new();
        private static PropertyInfo[] GetNavigableProperties(Type type)
            => NavigablePropertyCache.GetOrAdd(type, t =>
                t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.PropertyType.IsClass && p.PropertyType != typeof(string) && p.PropertyType != typeof(byte[]))
                    .ToArray());

        private static void ValidateEntityGraph(object rootEntity, HashSet<object> visited, string rootPath)
        {
            var stack = new Stack<(object Entity, int Depth, string Path)>();
            stack.Push((rootEntity, 0, rootPath));

            while (stack.Count > 0)
            {
                var (entity, depth, path) = stack.Pop();

                if (depth > MaxEntityDepth)
                    throw new ArgumentException($"Entity graph exceeds maximum depth of {MaxEntityDepth} at {path}");

                // LOGIC FIX (TASK 12): Check if the popped entity itself is IEnumerable
                // This ensures nested collections are properly validated
                if (entity is IEnumerable enumerable && entity is not string)
                {
                    ValidateCollection(enumerable, path);

                    foreach (var item in enumerable)
                    {
                        if (item == null) continue;

                        var itemType = item.GetType();
                        if (itemType.IsClass && itemType != typeof(string))
                        {
                            // Don't increment depth for collection items at same level
                            stack.Push((item, depth + 1, $"{path}[{itemType.Name}]"));
                        }
                    }
                    continue;
                }

                // Allow circular references without throwing errors by stopping
                // validation when an entity has already been visited. This prevents
                // infinite loops in graphs with cycles while still validating the
                // remainder of the object graph.
                if (!visited.Add(entity))
                    continue;

                // PERF: Only inspect properties whose declared type is a reference type
                // (excluding string/byte[]). For flat entities with only value-type + string
                // properties, this array is empty — skipping all GetValue reflection calls.
                var properties = GetNavigableProperties(entity.GetType());

                foreach (var prop in properties)
                {
                    var value = prop.GetValue(entity);
                    if (value == null) continue;

                    var propPath = $"{path}.{prop.Name}";

                    // Push all non-null class values to stack for validation
                    // IEnumerable check will happen when item is popped
                    stack.Push((value, depth + 1, propPath));
                }
            }
        }

        private static void ValidateCollection(IEnumerable collection, string path)
        {
            // PERFORMANCE FIX (TASK 11): Use ICollection.Count when available for O(1) check
            if (collection is ICollection coll)
            {
                if (coll.Count > MaxCollectionSize)
                    throw new ArgumentException($"Collection at {path} exceeds maximum size of {MaxCollectionSize}");
                return;
            }

            // Fallback to iteration for non-ICollection types
            var count = 0;
            foreach (var _ in collection)
            {
                if (++count > MaxCollectionSize)
                    throw new ArgumentException($"Collection at {path} exceeds maximum size of {MaxCollectionSize}");
            }
        }

        /// <summary>
        /// Validates that a bulk operation contains a reasonable number of entities and none are null.
        /// </summary>
        /// <typeparam name="T">Type of the entities involved in the operation.</typeparam>
        /// <param name="entities">Collection of entities to validate.</param>
        /// <param name="operation">Name of the bulk operation (for error messages).</param>
        public static void ValidateBulkOperation<T>(IEnumerable<T> entities, string operation) where T : class
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            var count = 0;
            foreach (var entity in entities)
            {
                if (entity == null)
                    throw new ArgumentException($"Null entity found in {operation} operation at index {count}");

                if (++count > MaxBulkOperationSize)
                    throw new ArgumentException($"Bulk {operation} operation exceeds maximum size of {MaxBulkOperationSize}");
            }

            if (count == 0)
                throw new ArgumentException($"Bulk {operation} operation cannot be empty");
        }

        /// <summary>
        /// Validates a raw SQL string to ensure it does not contain dangerous patterns or an excessive
        /// number of parameters.
        /// </summary>
        /// <param name="sql">The SQL statement to validate.</param>
        /// <param name="parameters">Optional parameters used with the SQL statement.</param>
        /// <exception cref="ArgumentException">Thrown when the SQL is deemed unsafe.</exception>
        public static void ValidateRawSql(string sql, IReadOnlyDictionary<string, object>? parameters = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("SQL cannot be null or whitespace");

            if (sql.Length > MaxSqlLength)
                throw new ArgumentException($"SQL exceeds maximum length of {MaxSqlLength}");

            var upperSql = sql.ToUpperInvariant();
            var dangerousPatterns = new[]
            {
                "XP_CMDSHELL", "SP_CONFIGURE", "OPENROWSET", "OPENDATASOURCE",
                "INTO OUTFILE", "LOAD_FILE", "SCRIPT", "EXECUTE IMMEDIATE"
            };

            foreach (var pattern in dangerousPatterns)
            {
                if (upperSql.Contains(pattern))
                    throw new ArgumentException($"SQL contains dangerous pattern: {pattern}");
            }

            if (parameters != null && parameters.Count > MaxParameterCount)
                throw new ArgumentException($"Parameter count {parameters.Count} exceeds maximum of {MaxParameterCount}");

            // SECURITY FIX (TASK 6): Enhanced SQL injection detection
            // Check for common injection patterns that bypass keyword detection
            DetectInjectionPatterns(sql, parameters);
        }

        /// <summary>
        /// Detects common SQL injection patterns in raw SQL, including UNION attacks,
        /// comment-based injection, and embedded quotes without proper parameterization.
        /// SECURITY FIX (TASK 6): Added comprehensive injection pattern detection.
        /// </summary>
        /// <param name="sql">SQL string to validate.</param>
        /// <param name="parameters">Optional parameters dictionary.</param>
        /// <exception cref="NormUsageException">Thrown when suspicious patterns are detected.</exception>
        private static void DetectInjectionPatterns(string sql, IReadOnlyDictionary<string, object>? parameters)
        {
            var upperSql = sql.ToUpperInvariant();

            // Pattern 1: UNION-based injection attempts
            // Look for UNION SELECT that's not in a legitimate subquery context
            if (upperSql.Contains("UNION") && upperSql.Contains("SELECT"))
            {
                // Allow legitimate UNION queries but check for suspicious patterns
                var unionIndex = upperSql.IndexOf("UNION");
                var beforeUnion = upperSql.Substring(0, unionIndex);

                // Suspicious if there's a single quote near UNION (likely injected)
                if (beforeUnion.LastIndexOf('\'') > Math.Max(beforeUnion.LastIndexOf("WHERE"), 0))
                {
                    throw new NormUsageException(
                        "Potential SQL injection detected: UNION with embedded quotes. " +
                        "Use parameterized queries instead of string concatenation.");
                }
            }

            // Pattern 2: Comment-based injection (-- or /* */)
            // S1 fix: a standalone inline -- comment is legitimate SQL (e.g. SELECT * FROM T WHERE Id=1 -- note).
            // The injection vector is a statement terminator (;) BEFORE the --, not the -- itself.
            // Only reject when -- appears outside a string literal AND a ; precedes it outside string literals,
            // indicating a potential "'; malicious SQL --" injection pattern.
            var doubleHyphenIndex = sql.IndexOf("--");
            if (doubleHyphenIndex >= 0)
            {
                var beforeComment = sql.Substring(0, doubleHyphenIndex);
                var singleQuoteCount = beforeComment.Count(c => c == '\'');

                // Only reject if: -- is outside a string literal AND a semicolon precedes it
                // (a semicolon outside a string before a -- comment is the injection indicator)
                if (singleQuoteCount % 2 == 0 && beforeComment.Contains(';'))
                {
                    throw new NormUsageException(
                        "Potential SQL injection detected: SQL comment (--) following a statement terminator. " +
                        "Use parameterized queries instead of string concatenation.");
                }
            }

            // Pattern 3: Block comments used for injection
            if (sql.Contains("/*") && sql.Contains("*/"))
            {
                var commentStart = sql.IndexOf("/*");
                var commentEnd = sql.IndexOf("*/", commentStart);

                // Check if there are suspicious keywords inside the comment
                var commentContent = sql.Substring(commentStart + 2, commentEnd - commentStart - 2).ToUpperInvariant();
                if (commentContent.Contains("UNION") || commentContent.Contains("SELECT") ||
                    commentContent.Contains("INSERT") || commentContent.Contains("DELETE"))
                {
                    throw new NormUsageException(
                        "Potential SQL injection detected: SQL keywords inside block comment. " +
                        "This pattern is commonly used in injection attacks.");
                }
            }

            // Pattern 4: Embedded quotes without proper parameterization
            // Count parameter markers (@, $, :) and compare with quote usage
            var paramMarkerCount = CountParameterMarkers(sql);
            var whereIndex = upperSql.IndexOf("WHERE");

            if (whereIndex >= 0)
            {
                var wherePart = sql.Substring(whereIndex);
                var singleQuotes = wherePart.Count(c => c == '\'');

                // If there are quotes in WHERE clause but no parameters passed, suspicious
                if (singleQuotes >= 2 && (parameters == null || parameters.Count == 0) && paramMarkerCount == 0)
                {
                    // Exception: allow simple constant queries like WHERE Status = 'Active'
                    // But warn if the value looks dynamic (contains spaces, numbers suggesting user input)
                    if (ContainsSuspiciousLiteralPattern(wherePart))
                    {
                        throw new NormUsageException(
                            "Potential SQL injection detected: WHERE clause with string literals but no parameters. " +
                            "Use parameterized queries (e.g., WHERE Name = @name) instead of embedding values directly. " +
                            "If this query uses only constant values, add a comment to document this.");
                    }
                }
            }

            // Pattern 5: Semicolon-based multi-statement injection.
            // S1 fix: use lexer-aware semicolon positions so that semicolons inside
            // string literals, double-quoted identifiers, line comments, and block
            // comments are not counted as statement terminators.
            var semiPositions = FindSemicolonPositionsOutsideTokens(sql);
            if (semiPositions.Count >= 1)
            {
                // Multiple real statement terminators — extract statements and check for DDL.
                int prev = 0;
                foreach (var pos in semiPositions)
                {
                    var stmt = sql.Substring(prev, pos - prev).Trim();
                    var upper = stmt.ToUpperInvariant();
                    // If any statement is DDL or system procedure, it's suspicious in this context
                    if (upper.StartsWith("DROP ") || upper.StartsWith("ALTER ") ||
                        upper.StartsWith("CREATE ") || upper.StartsWith("EXEC "))
                    {
                        throw new NormUsageException(
                            "Potential SQL injection detected: Multiple statements with DDL/EXEC commands. " +
                            "FromSqlRaw should only execute SELECT queries, not administrative commands.");
                    }
                    prev = pos + 1;
                }
                // Check the last fragment after the final semicolon.
                if (prev < sql.Length)
                {
                    var last = sql.Substring(prev).Trim();
                    if (last.Length > 0)
                    {
                        var upper = last.ToUpperInvariant();
                        if (upper.StartsWith("DROP ") || upper.StartsWith("ALTER ") ||
                            upper.StartsWith("CREATE ") || upper.StartsWith("EXEC "))
                        {
                            throw new NormUsageException(
                                "Potential SQL injection detected: Multiple statements with DDL/EXEC commands. " +
                                "FromSqlRaw should only execute SELECT queries, not administrative commands.");
                        }
                    }
                }
            }

            // Pattern 6: Encoding-based attacks (char/varchar/nchar concatenation)
            if (upperSql.Contains("CHAR(") && (upperSql.Contains("+") || upperSql.Contains("||")))
            {
                throw new NormUsageException(
                    "Potential SQL injection detected: CHAR() concatenation often used to obfuscate injection. " +
                    "If this is legitimate, use stored procedures or refactor the query.");
            }
        }

        /// <summary>
        /// Counts parameter markers in SQL string (@, $, :) to detect if parameterization is used.
        /// Uses a mini SQL lexer to skip string literals, double-quoted identifiers, line comments,
        /// and block comments so that @ inside 'user@example.com' is not counted.
        /// </summary>
        private static int CountParameterMarkers(string sql)
        {
            var count = 0;
            var i = 0;
            while (i < sql.Length)
            {
                var c = sql[i];
                // Skip single-quoted literals ('...' with '' escape)
                if (c == '\'')
                {
                    i++;
                    while (i < sql.Length)
                    {
                        if (sql[i] == '\'') { i++; if (i < sql.Length && sql[i] == '\'') i++; else break; }
                        else i++;
                    }
                    continue;
                }
                // Skip double-quoted identifiers ("..." with "" escape)
                if (c == '"')
                {
                    i++;
                    while (i < sql.Length)
                    {
                        if (sql[i] == '"') { i++; if (i < sql.Length && sql[i] == '"') i++; else break; }
                        else i++;
                    }
                    continue;
                }
                // Skip line comments (-- to newline)
                if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\n') i++;
                    continue;
                }
                // Skip block comments (/* ... */)
                if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i < sql.Length - 1 && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i += 2;
                    continue;
                }
                // Count parameter markers outside quoted/comment regions.
                // P1/X1 fix: exclude PostgreSQL :: cast syntax.
                // `::type` has the second `:` followed by a type-name (alphanumeric), which
                // would otherwise be counted as a `:name` parameter marker.
                // Guard: only count `:` when the PRECEDING character is NOT also `:`.
                if ((c == '@' || c == '$' || c == ':') && i + 1 < sql.Length
                    && (char.IsLetterOrDigit(sql[i + 1]) || sql[i + 1] == '_'))
                {
                    if (c != ':' || i == 0 || sql[i - 1] != ':')
                        count++;
                }
                i++;
            }
            return count;
        }

        /// <summary>
        /// Returns a list of character positions of ';' tokens that appear outside string
        /// literals, double-quoted identifiers, line comments, and block comments.
        /// S1 fix: replaces naive <c>sql.Count(c == ';')</c> / <c>sql.Split(';',...)</c>
        /// with a mini SQL lexer so embedded semicolons in literals/comments are ignored.
        /// </summary>
        private static List<int> FindSemicolonPositionsOutsideTokens(string sql)
        {
            var positions = new List<int>();
            var i = 0;
            while (i < sql.Length)
            {
                var c = sql[i];
                // Skip single-quoted literals ('...' with '' escape)
                if (c == '\'')
                {
                    i++;
                    while (i < sql.Length)
                    {
                        if (sql[i] == '\'') { i++; if (i < sql.Length && sql[i] == '\'') i++; else break; }
                        else i++;
                    }
                    continue;
                }
                // Skip double-quoted identifiers ("..." with "" escape)
                if (c == '"')
                {
                    i++;
                    while (i < sql.Length)
                    {
                        if (sql[i] == '"') { i++; if (i < sql.Length && sql[i] == '"') i++; else break; }
                        else i++;
                    }
                    continue;
                }
                // Skip line comments (-- to newline)
                if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\n') i++;
                    continue;
                }
                // Skip block comments (/* ... */)
                if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i < sql.Length - 1 && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i += 2;
                    continue;
                }
                if (c == ';')
                    positions.Add(i);
                i++;
            }
            return positions;
        }

        /// <summary>
        /// Checks if a WHERE clause contains suspicious literal patterns that suggest
        /// concatenated user input rather than legitimate constants.
        /// </summary>
        private static bool ContainsSuspiciousLiteralPattern(string wherePart)
        {
            // Extract string literals (content between quotes)
            var literals = new List<string>();
            var inString = false;
            var currentLiteral = new System.Text.StringBuilder();

            for (int i = 0; i < wherePart.Length; i++)
            {
                if (wherePart[i] == '\'')
                {
                    if (inString)
                    {
                        literals.Add(currentLiteral.ToString());
                        currentLiteral.Clear();
                    }
                    inString = !inString;
                }
                else if (inString)
                {
                    currentLiteral.Append(wherePart[i]);
                }
            }

            // Check each literal for suspicious patterns
            foreach (var literal in literals)
            {
                // Suspicious: email-like pattern (user@domain)
                if (literal.Contains('@') && literal.Contains('.'))
                    return true;

                // Suspicious: URL-like pattern
                if (literal.Contains("://") || literal.StartsWith("http"))
                    return true;

                // Suspicious: very long strings (likely user input, not constants)
                if (literal.Length > 50)
                    return true;

                // Suspicious: mix of special characters suggesting user input
                var specialCharCount = literal.Count(c => !char.IsLetterOrDigit(c) && c != ' ' && c != '_' && c != '-');
                if (specialCharCount > 3)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Normalizes a SQL string for safe keyword matching by:
        /// 1. Removing line comments (-- to end of line)
        /// 2. Removing block comments (/* ... */, including nested)
        /// 3. Replacing all whitespace variants (tabs, newlines, \r, Unicode non-breaking spaces
        ///    \u00A0, \u2003, etc.) with a single space
        /// 4. Collapsing multiple spaces into one
        /// 5. Lowercasing the result
        ///
        /// This defeats obfuscation techniques such as:
        ///   DR/**/OP TABLE users  → "drop table users"
        ///   INSERT\nINTO users    → "insert into users"
        ///   DROP\u00A0TABLE       → "drop table"
        /// </summary>
        internal static string NormalizeSql(string sql)
        {
            if (string.IsNullOrEmpty(sql))
                return string.Empty;

            var sb = new System.Text.StringBuilder(sql.Length);
            int i = 0;
            int len = sql.Length;

            while (i < len)
            {
                // Block comment removal (supports nesting).
                // Comments are removed WITHOUT inserting a space so that tokens
                // immediately adjacent to a comment are concatenated:
                //   DR/**/OP  → DROP   (rejected as DDL)
                //   SEL/**/ECT → SELECT (accepted as DML-safe)
                if (i + 1 < len && sql[i] == '/' && sql[i + 1] == '*')
                {
                    i += 2;
                    int depth = 1;
                    while (i < len && depth > 0)
                    {
                        if (i + 1 < len && sql[i] == '/' && sql[i + 1] == '*')
                        {
                            depth++;
                            i += 2;
                        }
                        else if (i + 1 < len && sql[i] == '*' && sql[i + 1] == '/')
                        {
                            depth--;
                            i += 2;
                        }
                        else
                        {
                            i++;
                        }
                    }
                    // No space added — adjacent tokens merge (DR/**/OP → DROP)
                    continue;
                }

                // Line comment removal (-- to end of line)
                if (i + 1 < len && sql[i] == '-' && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n' && sql[i] != '\r')
                        i++;
                    // Replace with a space so the next token is still separated
                    sb.Append(' ');
                    continue;
                }

                // Whitespace normalization — collapse all whitespace variants to space
                char c = sql[i];
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n' ||
                    c == '\u00A0' || c == '\u2003' || c == '\u2002' ||
                    c == '\u2000' || c == '\u2001' || c == '\u2004' ||
                    c == '\u2005' || c == '\u2006' || c == '\u2007' ||
                    c == '\u2008' || c == '\u2009' || c == '\u200A' ||
                    c == '\u3000' || char.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.SpaceSeparator)
                {
                    sb.Append(' ');
                    i++;
                    continue;
                }

                // Regular character — append lowercased
                sb.Append(char.ToLowerInvariant(c));
                i++;
            }

            // Collapse multiple spaces into one and trim
            var result = sb.ToString();
            // Use a fast path: replace sequences of spaces
            while (result.Contains("  "))
                result = result.Replace("  ", " ");
            return result.Trim();
        }

        /// <summary>
        /// P1: Provider-aware SQL safety gate for query APIs (SELECT-only).
        /// Non-SQL-Server providers skip the TSQL AST parser (which produces false
        /// negatives for dialect-specific syntax like SQLite PRAGMA) and rely on
        /// the keyword denylist PLUS a structural SELECT-only allowlist.
        /// SQL Server still uses the full AST allowlist path.
        /// All providers run keyword checks against the NORMALIZED form to defeat
        /// comment-injection and whitespace-based obfuscation attacks.
        ///
        /// S2-1/S9-1: For query APIs the gate is not "not in denylist" but
        /// "IS a SELECT statement". After denylist check, we verify the normalized
        /// SQL actually starts with SELECT (or WITH ... SELECT for CTEs).
        /// </summary>
        internal static bool IsSafeRawSql(string sql, DatabaseProvider? provider = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return false;

            // SEC-1: Normalize BEFORE keyword checks to defeat comment/whitespace obfuscation
            var normalized = NormalizeSql(sql);

            // X1 + P1: keyword denylist is always applied first for all providers
            if (!IsSafeByKeywords(normalized)) return false;

            // P1: skip expensive TSQL parse for non-SQL-Server providers
            if (provider is not null && provider is not SqlServerProvider)
            {
                // S2-1/S9-1: SELECT-only structural gate for non-SQL-Server providers.
                // The denylist alone is insufficient — commands like ATTACH DATABASE,
                // DETACH DATABASE, LOAD EXTENSION, USE are not in the denylist.
                // Require the statement to actually BE a SELECT (or CTE starting with WITH).
                return IsSelectStatement(normalized);
            }

            // Feed the ORIGINAL sql to the TSQL parser (it handles its own syntax);
            // the keyword check above already caught any obfuscated keywords.
            using var reader = new StringReader(sql);
            var parser = new TSql150Parser(false);
            var fragment = parser.Parse(reader, out var errors);

            if (errors != null && errors.Count > 0)
            {
                // Parse failed (non-TSQL syntax) — apply SELECT-only structural gate
                return IsSelectStatement(normalized);
            }

            var allowed = new HashSet<Type> { typeof(SelectStatement), typeof(SetVariableStatement) };

            if (fragment is TSqlScript script)
            {
                foreach (var batch in script.Batches)
                    foreach (var statement in batch.Statements)
                        if (!allowed.Contains(statement.GetType()))
                            return false;
                return true;
            }

            return false;
        }

        /// <summary>
        /// S2-1/S9-1: Returns true if the normalized SQL is a SELECT statement or a CTE
        /// (WITH ... AS (...) SELECT ...). This is the structural SELECT-only gate used
        /// for query APIs on non-SQL-Server providers.
        /// </summary>
        /// <param name="normalizedSql">Normalized SQL (lowercased, whitespace-collapsed, comment-stripped).</param>
        private static bool IsSelectStatement(string normalizedSql)
        {
            if (normalizedSql.StartsWith("select ", StringComparison.Ordinal)) return true;
            // Handle "select" with no trailing space (e.g., "select" at end of string — unlikely but safe to handle)
            if (normalizedSql == "select") return true;

            // Handle CTEs: WITH name AS (...) SELECT ...
            if (normalizedSql.StartsWith("with ", StringComparison.Ordinal))
            {
                int idx = FindSelectAfterCte(normalizedSql);
                return idx >= 0;
            }
            return false;
        }

        /// <summary>
        /// S2-1/S9-1: Finds the position of SELECT after a CTE preamble.
        /// Tracks parenthesis depth so nested parens inside the CTE body are skipped correctly.
        /// Returns -1 if no SELECT is found after the CTE.
        /// </summary>
        private static int FindSelectAfterCte(string normalizedSql)
        {
            // We need to find "select " that appears outside of all parentheses after "with "
            // Example: "with cte as (select * from t) select * from cte"
            // Walk through tracking paren depth; when depth returns to 0 look for "select "
            int depth = 0;
            int len = normalizedSql.Length;

            for (int i = 0; i < len; i++)
            {
                char c = normalizedSql[i];
                if (c == '(')
                {
                    depth++;
                }
                else if (c == ')')
                {
                    if (depth > 0) depth--;
                }
                else if (depth == 0 && i + 7 <= len &&
                         normalizedSql[i] == 's' && normalizedSql[i + 1] == 'e' &&
                         normalizedSql[i + 2] == 'l' && normalizedSql[i + 3] == 'e' &&
                         normalizedSql[i + 4] == 'c' && normalizedSql[i + 5] == 't' &&
                         normalizedSql[i + 6] == ' ')
                {
                    // Verify this select is at start of a token (preceded by space or is at position 0)
                    if (i == 0 || normalizedSql[i - 1] == ' ' || normalizedSql[i - 1] == ')')
                        return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// X1: Keyword-based denylist that catches side-effect and DDL commands.
        /// IMPORTANT: This method expects an already-normalized (lowercased, whitespace-
        /// collapsed, comment-stripped) SQL string from <see cref="NormalizeSql"/>.
        /// Checks are done against the normalized form to defeat obfuscation.
        /// </summary>
        private static bool IsSafeByKeywords(string normalizedSql)
        {
            // DML / DDL denylist — checked against normalized (comment-stripped) SQL
            // We check for word boundaries by requiring space or start/end around keywords.
            // Use Contains with a trailing space for keywords that must precede something,
            // or check for the keyword at end-of-string as well.
            if (ContainsDeniedKeyword(normalizedSql, "drop") ||
                ContainsDeniedKeyword(normalizedSql, "alter") ||
                ContainsDeniedKeyword(normalizedSql, "truncate") ||
                ContainsDeniedKeyword(normalizedSql, "exec") ||
                ContainsDeniedKeyword(normalizedSql, "execute") ||
                ContainsDeniedKeyword(normalizedSql, "delete") ||
                ContainsDeniedKeyword(normalizedSql, "update") ||
                ContainsDeniedKeyword(normalizedSql, "insert") ||
                ContainsDeniedKeyword(normalizedSql, "merge") ||
                ContainsDeniedKeyword(normalizedSql, "create") ||
                ContainsDeniedKeyword(normalizedSql, "grant") ||
                ContainsDeniedKeyword(normalizedSql, "revoke"))
                return false;

            // X1: Additional side-effect commands that bypass the AST path
            if (ContainsDeniedKeyword(normalizedSql, "pragma") ||
                ContainsDeniedKeyword(normalizedSql, "vacuum") ||
                ContainsDeniedKeyword(normalizedSql, "reindex") ||
                ContainsDeniedKeyword(normalizedSql, "analyze") ||
                ContainsDeniedKeyword(normalizedSql, "call"))
                return false;

            // S2-1/S9-1: Defense-in-depth denylist additions for SQLite/MySQL side-effect commands
            // not previously covered. These bypass the old denylist-only check.
            // ATTACH DATABASE 'x.db' AS x — attaches external SQLite database
            // DETACH DATABASE x — detaches external SQLite database
            // LOAD EXTENSION 'evil.dll' — SQLite extension loading
            // USE other_database — MySQL database switch
            // IMPORT — various DB import commands
            if (ContainsDeniedKeyword(normalizedSql, "attach") ||
                ContainsDeniedKeyword(normalizedSql, "detach") ||
                ContainsDeniedKeyword(normalizedSql, "load") ||
                ContainsDeniedKeyword(normalizedSql, "import") ||
                ContainsDeniedKeyword(normalizedSql, "use"))
                return false;

            // Reject stacked queries — semicolon separating statements
            // A single trailing semicolon is acceptable, but a semicolon followed by
            // any non-whitespace content indicates a stacked query.
            var semiIdx = normalizedSql.IndexOf(';');
            if (semiIdx >= 0 && semiIdx < normalizedSql.Length - 1)
                return false;

            return true;
        }

        /// <summary>
        /// Returns true if the normalized SQL contains the given keyword as a whole token
        /// (i.e., surrounded by spaces, start-of-string, or end-of-string).
        /// This prevents false positives like "reindex" matching "index".
        /// </summary>
        private static bool ContainsDeniedKeyword(string normalizedSql, string keyword)
        {
            int idx = 0;
            while (true)
            {
                idx = normalizedSql.IndexOf(keyword, idx, StringComparison.Ordinal);
                if (idx < 0) return false;

                // Check character before the keyword (must be start-of-string or space)
                bool beforeOk = idx == 0 || normalizedSql[idx - 1] == ' ';
                // Check character after the keyword (must be end-of-string or space)
                int afterIdx = idx + keyword.Length;
                bool afterOk = afterIdx >= normalizedSql.Length || normalizedSql[afterIdx] == ' ';

                if (beforeOk && afterOk)
                    return true;

                idx += keyword.Length;
            }
        }

        private static readonly HashSet<char> AllowedLikeEscapeChars = new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&()*+,-./:;<=>?@[]^_{|}~\\");

        /// <summary>
        /// Validates that the given character is permitted for use as an SQL LIKE escape character.
        /// </summary>
        /// <param name="escapeChar">The character to validate.</param>
        /// <returns>The validated escape character.</returns>
        /// <exception cref="ArgumentException">Thrown when the character is not allowed.</exception>
        public static char ValidateLikeEscapeChar(char escapeChar)
        {
            // SECURITY FIX: Explicitly reject SQL-dangerous characters that could cause injection
            // even if someone modifies AllowedLikeEscapeChars at runtime
            if (escapeChar == '\'' || escapeChar == '"' || escapeChar == ';' || escapeChar == '\0')
                throw new ArgumentException(
                    $"LIKE escape character '{escapeChar}' is not allowed for security reasons. " +
                    "Use a safe character like backslash (\\), tilde (~), or caret (^).");

            if (!AllowedLikeEscapeChars.Contains(escapeChar))
                throw new ArgumentException(
                    $"Invalid LIKE escape character: {escapeChar}. " +
                    "Must be an alphanumeric or safe symbol character.");

            return escapeChar;
        }

        /// <summary>
        /// Validates the supplied connection string for the specified provider, throwing if it is malformed.
        /// </summary>
        /// <param name="connectionString">The connection string to validate.</param>
        /// <param name="provider">Normalized provider name (e.g., "sqlserver").</param>
        /// <exception cref="ArgumentException">Thrown when the connection string fails validation.</exception>
        public static void ValidateConnectionString(string connectionString, string provider)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty");

            if (connectionString.Length > 8192)
                throw new ArgumentException("Connection string exceeds maximum length");

            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

                switch (provider.ToLowerInvariant())
                {
                    case "sqlserver":
                        ValidateSqlServerConnectionString(builder);
                        break;
                    case "sqlite":
                        ValidateSqliteConnectionString(builder);
                        break;
                }
            }
            catch (Exception ex)
            {
                var safeConnStr = MaskSensitiveConnectionStringData(connectionString);
                throw new ArgumentException($"Invalid connection string format: {safeConnStr}", ex);
            }
        }

        internal static string MaskSensitiveConnectionStringData(string connectionString)
        {
            var builder = new DbConnectionStringBuilder();
            try
            {
                builder.ConnectionString = connectionString;
                if (builder.ContainsKey("Password")) builder["Password"] = "***";
                if (builder.ContainsKey("Pwd")) builder["Pwd"] = "***";
                if (builder.ContainsKey("User Password")) builder["User Password"] = "***";
                // SECURITY FIX: Mask additional sensitive parameters beyond just passwords
                // API keys, tokens, and secrets in extended properties can leak in logs
                if (builder.ContainsKey("API Key")) builder["API Key"] = "***";
                if (builder.ContainsKey("ApiKey")) builder["ApiKey"] = "***";
                if (builder.ContainsKey("Token")) builder["Token"] = "***";
                if (builder.ContainsKey("AccessToken")) builder["AccessToken"] = "***";
                if (builder.ContainsKey("Access Token")) builder["Access Token"] = "***";
                if (builder.ContainsKey("Secret")) builder["Secret"] = "***";
                if (builder.ContainsKey("SecretKey")) builder["SecretKey"] = "***";
                if (builder.ContainsKey("Secret Key")) builder["Secret Key"] = "***";
                if (builder.ContainsKey("AccessKey")) builder["AccessKey"] = "***";
                if (builder.ContainsKey("Access Key")) builder["Access Key"] = "***";
                if (builder.ContainsKey("PrivateKey")) builder["PrivateKey"] = "***";
                if (builder.ContainsKey("Private Key")) builder["Private Key"] = "***";
                if (builder.ContainsKey("ClientSecret")) builder["ClientSecret"] = "***";
                if (builder.ContainsKey("Client Secret")) builder["Client Secret"] = "***";
                return builder.ConnectionString;
            }
            catch
            {
                return "[INVALID_CONNECTION_STRING]";
            }
        }

        private static void ValidateSqlServerConnectionString(DbConnectionStringBuilder builder)
        {
            if (!builder.ContainsKey("Server") && !builder.ContainsKey("Data Source"))
                throw new ArgumentException("SQL Server connection string must specify Server or Data Source");

            if (builder.TryGetValue("Connection Timeout", out var timeoutObj) &&
                int.TryParse(timeoutObj?.ToString(), out var timeout) &&
                (timeout < 0 || timeout > 300))
            {
                throw new ArgumentException("Connection Timeout must be between 0 and 300 seconds");
            }
        }

        private static void ValidateSqliteConnectionString(DbConnectionStringBuilder builder)
        {
            if (!builder.ContainsKey("Data Source"))
                throw new ArgumentException("SQLite connection string must specify Data Source");

            if (builder.TryGetValue("Data Source", out var dataSource))
            {
                var path = dataSource?.ToString();
                if (!string.IsNullOrEmpty(path) && path != ":memory:")
                {
                    if (path!.Contains("..") || (Path.IsPathRooted(path) && !IsValidDatabasePath(path)))
                        throw new ArgumentException("Invalid SQLite database path");
                }
            }
        }

        private static bool IsValidDatabasePath(string path)
        {
            try
            {
                var fullPath = Path.GetFullPath(path);
                var directory = Path.GetDirectoryName(fullPath);
                return Directory.Exists(directory) || Directory.Exists(Path.GetDirectoryName(directory));
            }
            catch
            {
                return false;
            }
        }
    }
}
