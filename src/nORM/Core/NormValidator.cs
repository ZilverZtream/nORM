using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
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
        private const int MaxConnectionStringLength = 8192;
        private const int MaxConnectionTimeoutSeconds = 300;

        /// <summary>
        /// Dangerous SQL patterns that indicate system-level commands or file operations.
        /// Checked against the uppercased SQL input in <see cref="ValidateRawSql"/>.
        /// </summary>
        private static readonly string[] DangerousPatterns =
        {
            "XP_CMDSHELL", "SP_CONFIGURE", "OPENROWSET", "OPENDATASOURCE",
            "INTO OUTFILE", "LOAD_FILE", "SCRIPT", "EXECUTE IMMEDIATE"
        };

        /// <summary>
        /// Statement types allowed through the TSQL AST allowlist in <see cref="IsSafeRawSql"/>.
        /// SelectStatement covers all SELECT queries; SetVariableStatement allows DECLARE/SET.
        /// </summary>
        private static readonly HashSet<Type> AllowedTSqlStatementTypes = new()
        {
            typeof(SelectStatement),
            typeof(SetVariableStatement)
        };

        /// <summary>
        /// Keys in a connection string that contain sensitive data and must be masked
        /// before including the connection string in error messages or logs.
        /// </summary>
        private static readonly string[] SensitiveConnectionStringKeys =
        {
            "Password", "Pwd", "User Password",
            "API Key", "ApiKey",
            "Token", "AccessToken", "Access Token",
            "Secret", "SecretKey", "Secret Key",
            "AccessKey", "Access Key",
            "PrivateKey", "Private Key",
            "ClientSecret", "Client Secret"
        };

        private const string MaskedValue = "***";

        private static readonly ObjectPool<HashSet<object>> HashSetPool =
            new DefaultObjectPool<HashSet<object>>(new HashSetPolicy(), Environment.ProcessorCount * 2);

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

        /// <summary>
        /// Cache of properties whose declared type could contain entity graph references.
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

                // Check if the popped entity itself is IEnumerable to validate nested collections.
                if (entity is IEnumerable enumerable && entity is not string)
                {
                    ValidateCollection(enumerable, path);

                    foreach (var item in enumerable)
                    {
                        if (item == null) continue;

                        var itemType = item.GetType();
                        if (itemType.IsClass && itemType != typeof(string))
                        {
                            // Increment depth for collection items to count them as a deeper level in the graph
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

                // Only inspect properties whose declared type is a reference type
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
            // Use ICollection.Count when available for an O(1) check instead of enumerating.
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

            foreach (var pattern in DangerousPatterns)
            {
                if (upperSql.Contains(pattern))
                    throw new ArgumentException($"SQL contains dangerous pattern: {pattern}");
            }

            if (parameters != null && parameters.Count > MaxParameterCount)
                throw new ArgumentException($"Parameter count {parameters.Count} exceeds maximum of {MaxParameterCount}");

            // Check for common injection patterns that bypass keyword detection.
            // Pass pre-computed upperSql to avoid redundant ToUpperInvariant call.
            DetectInjectionPatterns(sql, upperSql, parameters);
        }

        /// <summary>
        /// Detects common SQL injection patterns in raw SQL, including UNION attacks,
        /// comment-based injection, and embedded quotes without proper parameterization.
        /// </summary>
        /// <param name="sql">SQL string to validate.</param>
        /// <param name="upperSql">Pre-computed uppercased SQL to avoid redundant ToUpperInvariant call.</param>
        /// <param name="parameters">Optional parameters dictionary.</param>
        /// <exception cref="NormUsageException">Thrown when suspicious patterns are detected.</exception>
        private static void DetectInjectionPatterns(string sql, string upperSql, IReadOnlyDictionary<string, object>? parameters)
        {

            // Pattern 1: UNION-based injection attempts
            // Look for UNION SELECT that's not in a legitimate subquery context.
            // Limitation: This heuristic only detects the most common UNION injection
            // pattern — a single quote appearing after the last WHERE keyword before
            // the UNION keyword. It does NOT detect:
            //   - UNION injections that avoid embedded quotes (e.g., numeric columns),
            //   - UNION injections where the quote is inside a legitimate string literal,
            //   - Multiple UNION clauses (only the first occurrence is inspected).
            // For defense in depth, always use parameterized queries and the TSql AST
            // allowlist (ValidateRawSql) in addition to this pattern-based check.
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

            // Pattern 2: Comment-based injection (-- or /* */).
            // A standalone inline -- comment is legitimate SQL (e.g. SELECT * FROM T WHERE Id=1 -- note).
            // The injection vector is a statement terminator (;) BEFORE the --, not the -- itself.
            // Only reject when -- appears outside a string literal AND a ; precedes it outside string literals,
            // indicating a potential "'; malicious SQL --" injection pattern.
            // Limitation: The escaped-quote detection below uses a simple parity check
            // (odd count = inside string literal). This does NOT handle escaped single
            // quotes within string literals (e.g., 'O''Brien' counts as 2 quotes, which
            // looks like "outside a string" to the parity check). A full SQL lexer would
            // be needed to handle all edge cases. The semicolon requirement provides an
            // additional safety gate that reduces false negatives from this limitation.
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

                if (commentEnd < 0)
                    throw new NormUsageException(
                        "Malformed SQL: block comment opened with '/*' but no matching '*/' found after it. " +
                        "Ensure block comments are properly closed.");

                // Check if there are suspicious keywords inside the comment.
                // Guard against overlapping /* and */ (e.g. "/*/") where commentEnd < commentStart + 2,
                // which would produce a negative Substring length.
                var commentLength = Math.Max(0, commentEnd - commentStart - 2);
                var commentContent = sql.Substring(commentStart + 2, commentLength).ToUpperInvariant();
                if (commentContent.Contains("UNION") || commentContent.Contains("SELECT") ||
                    commentContent.Contains("INSERT") || commentContent.Contains("DELETE"))
                {
                    throw new NormUsageException(
                        "Potential SQL injection detected: SQL keywords inside block comment. " +
                        "This pattern is commonly used in injection attacks.");
                }
            }

            // Pattern 4: Embedded quotes without proper parameterization
            var whereIndex = upperSql.IndexOf("WHERE");

            if (whereIndex >= 0)
            {
                var wherePart = sql.Substring(whereIndex);
                var singleQuotes = wherePart.Count(c => c == '\'');

                // If there are quotes in WHERE clause but no parameters passed, suspicious
                if (singleQuotes >= 2 && (parameters == null || parameters.Count == 0) && CountParameterMarkers(sql) == 0)
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
            // Use lexer-aware semicolon positions so that semicolons inside
            // string literals, double-quoted identifiers, line comments, and block
            // comments are not counted as statement terminators.
            var semiPositions = FindSemicolonPositionsOutsideTokens(sql);
            if (semiPositions.Count >= 1)
            {
                // Real statement terminators found — extract statements and check for DDL.
                int prev = 0;
                foreach (var pos in semiPositions)
                {
                    CheckStatementFragmentForDdl(sql.Substring(prev, pos - prev));
                    prev = pos + 1;
                }
                // Check the last fragment after the final semicolon.
                if (prev < sql.Length)
                {
                    CheckStatementFragmentForDdl(sql.Substring(prev));
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
        /// Checks a single statement fragment (text between semicolons) for DDL/EXEC commands
        /// and throws <see cref="NormUsageException"/> if found.
        /// </summary>
        /// <param name="fragment">Raw SQL fragment to inspect.</param>
        private static void CheckStatementFragmentForDdl(string fragment)
        {
            var trimmed = fragment.Trim();
            if (trimmed.Length == 0) return;

            var upper = trimmed.ToUpperInvariant();
            if (upper.StartsWith("DROP ") || upper.StartsWith("ALTER ") ||
                upper.StartsWith("CREATE ") || upper.StartsWith("EXEC "))
            {
                throw new NormUsageException(
                    "Potential SQL injection detected: Multiple statements with DDL/EXEC commands. " +
                    "FromSqlRaw should only execute SELECT queries, not administrative commands.");
            }
        }

        /// <summary>
        /// Advances the index past a SQL token (string literal, double-quoted identifier,
        /// line comment, or block comment) starting at position <paramref name="i"/>.
        /// Returns true if a token was skipped, false if the character at <paramref name="i"/>
        /// is not the start of any skippable token.
        /// </summary>
        /// <param name="sql">The SQL string being lexed.</param>
        /// <param name="i">Current position; updated to the position after the skipped token.</param>
        /// <returns>True if a token was consumed and <paramref name="i"/> was advanced.</returns>
        private static bool TrySkipSqlToken(string sql, ref int i)
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
                return true;
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
                return true;
            }

            // Skip line comments (-- to newline)
            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                i += 2;
                while (i < sql.Length && sql[i] != '\n') i++;
                return true;
            }

            // Skip block comments (/* ... */)
            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                i += 2;
                while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                if (i + 1 < sql.Length) i += 2; // skip past */
                else i = sql.Length; // unterminated block comment — advance to end
                return true;
            }

            return false;
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
                if (TrySkipSqlToken(sql, ref i))
                    continue;

                var c = sql[i];
                // Count parameter markers outside quoted/comment regions.
                // Exclude PostgreSQL :: cast syntax.
                // `::type` has the second `:` followed by a type-name (alphanumeric), which
                // would otherwise be counted as a `:name` parameter marker.
                // Only count `:` when the preceding character is NOT also `:`.
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
        /// Replaces naive <c>sql.Count(c == ';')</c> / <c>sql.Split(';',...)</c>
        /// with a mini SQL lexer so embedded semicolons in literals/comments are ignored.
        /// </summary>
        private static List<int> FindSemicolonPositionsOutsideTokens(string sql)
        {
            var positions = new List<int>();
            var i = 0;
            while (i < sql.Length)
            {
                if (TrySkipSqlToken(sql, ref i))
                    continue;

                if (sql[i] == ';')
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
            var currentLiteral = new StringBuilder();

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
                const int MaxSafeLiteralLength = 50;
                if (literal.Length > MaxSafeLiteralLength)
                    return true;

                // Suspicious: mix of special characters suggesting user input
                const int MaxSpecialCharCount = 3;
                var specialCharCount = literal.Count(c => !char.IsLetterOrDigit(c) && c != ' ' && c != '_' && c != '-');
                if (specialCharCount > MaxSpecialCharCount)
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

            var sb = new StringBuilder(sql.Length);
            int i = 0;
            int len = sql.Length;
            // Tracks whether the last character appended to sb was a space,
            // allowing consecutive whitespace runs to be collapsed in a single pass
            // without a separate O(n²) post-processing loop.
            bool lastWasSpace = true; // start true to suppress leading spaces

            while (i < len)
            {
                // Block comment removal (supports nesting).
                // NOTE: Nested block comments (/* outer /* inner */ still in outer */)
                // are a PostgreSQL-specific extension. Standard SQL and most other
                // databases (SQL Server, MySQL, SQLite) treat the first */ as closing
                // the entire comment. We handle nesting here defensively so that
                // PostgreSQL-targeted SQL is normalized correctly; for other providers
                // the depth counter simply never exceeds 1.
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
                    // Separate the next token with one space (skipped if already at a space)
                    if (!lastWasSpace)
                    {
                        sb.Append(' ');
                        lastWasSpace = true;
                    }
                    continue;
                }

                // Whitespace normalization — collapse all whitespace variants to a single space
                char c = sql[i];
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n' ||
                    c == '\u00A0' || c == '\u2003' || c == '\u2002' ||
                    c == '\u2000' || c == '\u2001' || c == '\u2004' ||
                    c == '\u2005' || c == '\u2006' || c == '\u2007' ||
                    c == '\u2008' || c == '\u2009' || c == '\u200A' ||
                    c == '\u3000' || char.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.SpaceSeparator)
                {
                    if (!lastWasSpace)
                    {
                        sb.Append(' ');
                        lastWasSpace = true;
                    }
                    i++;
                    continue;
                }

                // Regular character — append lowercased
                sb.Append(char.ToLowerInvariant(c));
                lastWasSpace = false;
                i++;
            }

            // Strip any trailing space produced by a line comment or whitespace at end of input
            if (lastWasSpace && sb.Length > 0)
                sb.Length--;

            return sb.ToString();
        }

        /// <summary>
        /// Provider-aware SQL safety gate for query APIs (SELECT-only).
        /// Non-SQL-Server providers skip the TSQL AST parser (which produces false
        /// negatives for dialect-specific syntax like SQLite PRAGMA) and rely on
        /// the keyword denylist PLUS a structural SELECT-only allowlist.
        /// SQL Server still uses the full AST allowlist path.
        /// All providers run keyword checks against the NORMALIZED form to defeat
        /// comment-injection and whitespace-based obfuscation attacks.
        /// For query APIs the gate is not "not in denylist" but "IS a SELECT statement":
        /// after denylist check, the normalized SQL must start with SELECT (or WITH ... SELECT for CTEs).
        /// </summary>
        internal static bool IsSafeRawSql(string sql, DatabaseProvider? provider = null)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return false;

            // Normalize BEFORE keyword checks to defeat comment/whitespace obfuscation.
            var normalized = NormalizeSql(sql);

            // Keyword denylist is always applied first for all providers.
            if (!IsSafeByKeywords(normalized)) return false;

            // Skip expensive TSQL parse for non-SQL-Server providers.
            if (provider is not null && provider is not SqlServerProvider)
            {
                // SELECT-only structural gate for non-SQL-Server providers.
                // The denylist alone is insufficient — commands like ATTACH DATABASE,
                // DETACH DATABASE, LOAD EXTENSION, USE are not in the denylist.
                // Require the statement to actually BE a SELECT (or CTE starting with WITH).
                return IsSelectStatement(normalized);
            }

            // Feed the ORIGINAL sql to the TSQL parser (it handles its own syntax);
            // the keyword check above already caught any obfuscated keywords.
            // ScriptDom is an optional dependency (PrivateAssets="all") — fall back to the
            // structural SELECT-only gate when it is not installed in the consumer's application.
            try
            {
                using var reader = new StringReader(sql);
                var parser = new TSql150Parser(false);
                var fragment = parser.Parse(reader, out var errors);

                if (errors != null && errors.Count > 0)
                {
                    // Parse failed (non-TSQL syntax) — apply SELECT-only structural gate
                    return IsSelectStatement(normalized);
                }

                if (fragment is TSqlScript script)
                {
                    foreach (var batch in script.Batches)
                        foreach (var statement in batch.Statements)
                            if (!AllowedTSqlStatementTypes.Contains(statement.GetType()))
                                return false;
                    return true;
                }

                return false;
            }
            catch (FileNotFoundException)
            {
                // Microsoft.SqlServer.TransactSql.ScriptDom is not available.
                // Fall back to the keyword denylist + SELECT-only structural gate.
                return IsSelectStatement(normalized);
            }
        }

        /// <summary>
        /// Returns true if the normalized SQL is a SELECT statement or a CTE
        /// (WITH ... AS (...) SELECT ...). Used as the structural SELECT-only gate
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
        /// Finds the position of SELECT after a CTE preamble.
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
        /// Keyword-based denylist that catches side-effect and DDL commands.
        /// Expects an already-normalized (lowercased, whitespace-collapsed, comment-stripped) SQL
        /// string from <see cref="NormalizeSql"/>. Checks are done against the normalized form to defeat obfuscation.
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

            // Additional side-effect commands that bypass the AST path.
            if (ContainsDeniedKeyword(normalizedSql, "pragma") ||
                ContainsDeniedKeyword(normalizedSql, "vacuum") ||
                ContainsDeniedKeyword(normalizedSql, "reindex") ||
                ContainsDeniedKeyword(normalizedSql, "analyze") ||
                ContainsDeniedKeyword(normalizedSql, "call"))
                return false;

            // Defense-in-depth denylist for SQLite/MySQL side-effect commands:
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
        /// This prevents false positives like "updated" matching "update" or "executor" matching "exec".
        /// </summary>
        private static bool ContainsDeniedKeyword(string normalizedSql, string keyword)
        {
            int idx = 0;
            while (true)
            {
                idx = normalizedSql.IndexOf(keyword, idx, StringComparison.Ordinal);
                if (idx < 0) return false;

                // Check character before the keyword (must be start-of-string or a SQL word boundary).
                // Alphanumerics, underscores, and quotes are "word" chars — quotes prevent matching
                // keywords inside string literals (e.g. 'DROP' should NOT trigger the denylist).
                bool beforeOk = idx == 0 || IsWordBoundary(normalizedSql[idx - 1]);
                // Check character after the keyword (must be end-of-string or a SQL word boundary)
                int afterIdx = idx + keyword.Length;
                bool afterOk = afterIdx >= normalizedSql.Length || IsWordBoundary(normalizedSql[afterIdx]);

                if (beforeOk && afterOk)
                    return true;

                idx += keyword.Length;
            }
        }

        /// <summary>
        /// Returns true if the character is a SQL word boundary (i.e. NOT part of an identifier or string literal).
        /// Alphanumerics and underscores are SQL identifier characters; quotes indicate string literal context.
        /// Everything else (spaces, semicolons, parentheses, etc.) is a valid word boundary.
        /// </summary>
        private static bool IsWordBoundary(char c)
        {
            return !char.IsLetterOrDigit(c) && c != '_' && c != '\'' && c != '"';
        }

        /// <summary>
        /// Characters permitted for use as an SQL LIKE escape character.
        /// Excludes SQL-dangerous characters (semicolons, quotes, null) which are
        /// also rejected by the explicit guard in <see cref="ValidateLikeEscapeChar"/>.
        /// </summary>
        private static readonly HashSet<char> AllowedLikeEscapeChars = new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&()*+,-./:<=>?@[]^_{|}~\\");

        /// <summary>
        /// Validates that the given character is permitted for use as an SQL LIKE escape character.
        /// </summary>
        /// <param name="escapeChar">The character to validate.</param>
        /// <returns>The validated escape character.</returns>
        /// <exception cref="ArgumentException">Thrown when the character is not allowed.</exception>
        public static char ValidateLikeEscapeChar(char escapeChar)
        {
            // Explicitly reject SQL-dangerous characters that could cause injection
            // even if AllowedLikeEscapeChars is modified at runtime.
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

            if (connectionString.Length > MaxConnectionStringLength)
                throw new ArgumentException($"Connection string exceeds maximum length of {MaxConnectionStringLength}");

            if (string.IsNullOrWhiteSpace(provider))
                throw new ArgumentException("Provider name cannot be null or empty", nameof(provider));

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
            catch (ArgumentException)
            {
                // Re-throw ArgumentException directly to avoid double-wrapping
                // (the inner validation methods already throw ArgumentException).
                throw;
            }
            catch (DbException ex)
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
                foreach (var key in SensitiveConnectionStringKeys)
                {
                    if (builder.ContainsKey(key))
                        builder[key] = MaskedValue;
                }
                return builder.ConnectionString;
            }
            catch (ArgumentException)
            {
                // Connection string is malformed and cannot be parsed
                return "[INVALID_CONNECTION_STRING]";
            }
            catch (FormatException)
            {
                // Connection string has invalid format
                return "[INVALID_CONNECTION_STRING]";
            }
        }

        private static void ValidateSqlServerConnectionString(DbConnectionStringBuilder builder)
        {
            if (!builder.ContainsKey("Server") && !builder.ContainsKey("Data Source"))
                throw new ArgumentException("SQL Server connection string must specify Server or Data Source");

            if (builder.TryGetValue("Connection Timeout", out var timeoutObj) &&
                int.TryParse(timeoutObj?.ToString(), out var timeout) &&
                (timeout < 0 || timeout > MaxConnectionTimeoutSeconds))
            {
                throw new ArgumentException($"Connection Timeout must be between 0 and {MaxConnectionTimeoutSeconds} seconds");
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
            catch (ArgumentException)
            {
                // Path contains invalid characters
                return false;
            }
            catch (NotSupportedException)
            {
                // Path contains a colon in an invalid position (Windows)
                return false;
            }
            catch (PathTooLongException)
            {
                // Path exceeds system maximum length
                return false;
            }
        }
    }
}
