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

                var properties = GetCachedProperties(entity.GetType());

                foreach (var prop in properties)
                {
                    if (!prop.CanRead) continue;

                    var value = prop.GetValue(entity);
                    if (value == null) continue;

                    var propPath = $"{path}.{prop.Name}";
                    var valueType = value.GetType();

                    // Push all non-null class values to stack for validation
                    // IEnumerable check will happen when item is popped
                    if (valueType.IsClass && valueType != typeof(string))
                    {
                        stack.Push((value, depth + 1, propPath));
                    }
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
            // These can be used to terminate legitimate SQL and inject malicious code
            var doubleHyphenIndex = sql.IndexOf("--");
            if (doubleHyphenIndex >= 0)
            {
                // Allow if it's in a string literal or legitimate comment at end
                var beforeComment = sql.Substring(0, doubleHyphenIndex);
                var singleQuoteCount = beforeComment.Count(c => c == '\'');

                // If odd number of quotes, we're inside a string (allowed)
                // Otherwise it's a comment in SQL code (suspicious)
                if (singleQuoteCount % 2 == 0 && doubleHyphenIndex < sql.Length - 10)
                {
                    throw new NormUsageException(
                        "Potential SQL injection detected: SQL comment (--) in suspicious context. " +
                        "If this is legitimate, consider using /* */ style comments.");
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

            // Pattern 5: Semicolon-based multi-statement injection
            var semicolonCount = sql.Count(c => c == ';');
            if (semicolonCount > 1)
            {
                // Multiple statements can be legitimate (batching) but check context
                var statements = sql.Split(';', StringSplitOptions.RemoveEmptyEntries);
                foreach (var stmt in statements)
                {
                    var trimmed = stmt.Trim().ToUpperInvariant();
                    // If any statement is DDL or system procedure, it's suspicious in this context
                    if (trimmed.StartsWith("DROP ") || trimmed.StartsWith("ALTER ") ||
                        trimmed.StartsWith("CREATE ") || trimmed.StartsWith("EXEC "))
                    {
                        throw new NormUsageException(
                            "Potential SQL injection detected: Multiple statements with DDL/EXEC commands. " +
                            "FromSqlRaw should only execute SELECT queries, not administrative commands.");
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
        /// </summary>
        private static int CountParameterMarkers(string sql)
        {
            var count = 0;
            for (int i = 0; i < sql.Length - 1; i++)
            {
                var c = sql[i];
                var next = sql[i + 1];

                // Parameter marker followed by alphanumeric or underscore
                if ((c == '@' || c == '$' || c == ':') && (char.IsLetterOrDigit(next) || next == '_'))
                {
                    count++;
                }
            }
            return count;
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

        internal static bool IsSafeRawSql(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return false;

            using var reader = new StringReader(sql);
            var parser = new TSql150Parser(false);
            var fragment = parser.Parse(reader, out var errors);

            if (errors != null && errors.Count > 0)
            {
                var lowerSql = sql.ToLowerInvariant();
                return !(lowerSql.Contains("drop ") || lowerSql.Contains("alter ") ||
                         lowerSql.Contains("truncate ") || lowerSql.Contains("exec "));
            }

            if (fragment is TSqlScript script)
            {
                foreach (var batch in script.Batches)
                {
                    foreach (var statement in batch.Statements)
                    {
                        var typeName = statement.GetType().Name;
                        if (typeName.Contains("Drop", StringComparison.OrdinalIgnoreCase) ||
                            typeName.Contains("Alter", StringComparison.OrdinalIgnoreCase) ||
                            typeName.Contains("Truncate", StringComparison.OrdinalIgnoreCase) ||
                            typeName.Contains("Execute", StringComparison.OrdinalIgnoreCase))
                        {
                            return false;
                        }
                    }
                }
            }
            else
            {
                var typeName = fragment.GetType().Name;
                if (typeName.Contains("Drop", StringComparison.OrdinalIgnoreCase) ||
                    typeName.Contains("Alter", StringComparison.OrdinalIgnoreCase) ||
                    typeName.Contains("Truncate", StringComparison.OrdinalIgnoreCase) ||
                    typeName.Contains("Execute", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
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
