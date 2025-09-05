using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;

#nullable enable

namespace nORM.Core
{
    public static class NormValidator
    {
        private const int MaxEntityDepth = 10;
        private const int MaxCollectionSize = 10000;
        private const int MaxBulkOperationSize = 50000;
        private const int MaxParameterCount = 2000;
        private const int MaxSqlLength = 100000;

        public static void ValidateEntity<T>(T entity, string parameterName = "entity") where T : class
        {
            if (entity == null)
                throw new ArgumentNullException(parameterName);

            ValidateEntityGraph(entity!, new HashSet<object>(ReferenceEqualityComparer.Instance), 0, parameterName);
        }

        private static void ValidateEntityGraph(object entity, HashSet<object> visited, int depth, string path)
        {
            if (depth > MaxEntityDepth)
                throw new ArgumentException($"Entity graph exceeds maximum depth of {MaxEntityDepth} at {path}");

            // Allow circular references without throwing errors by stopping
            // recursion when an entity has already been visited. This prevents
            // infinite loops in graphs with cycles while still validating the
            // remainder of the object graph.
            if (!visited.Add(entity))
                return;

            try
            {
                var properties = entity.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

                foreach (var prop in properties)
                {
                    if (!prop.CanRead) continue;

                    var value = prop.GetValue(entity);
                    if (value == null) continue;

                    var propPath = $"{path}.{prop.Name}";

                    if (value is IEnumerable enumerable && value is not string)
                    {
                        ValidateCollection(enumerable, propPath);

                        foreach (var item in enumerable)
                        {
                            if (item != null && item.GetType().IsClass && item.GetType() != typeof(string))
                            {
                                ValidateEntityGraph(item, new HashSet<object>(visited), depth + 1, propPath);
                            }
                        }
                    }
                    else if (value.GetType().IsClass && value.GetType() != typeof(string))
                    {
                        ValidateEntityGraph(value, new HashSet<object>(visited), depth + 1, propPath);
                    }
                }
            }
            finally
            {
                visited.Remove(entity);
            }
        }

        private static void ValidateCollection(IEnumerable collection, string path)
        {
            var count = 0;
            foreach (var _ in collection)
            {
                if (++count > MaxCollectionSize)
                    throw new ArgumentException($"Collection at {path} exceeds maximum size of {MaxCollectionSize}");
            }
        }

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
        }

        internal static bool IsSafeRawSql(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return false;

            var lowerSql = sql.ToLowerInvariant();
            if (lowerSql.Contains("drop ") || lowerSql.Contains("alter ") ||
                lowerSql.Contains("truncate ") || lowerSql.Contains("exec "))
                return false;

            return true;
        }

        private static readonly HashSet<char> AllowedLikeEscapeChars = new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&()*+,-./:;<=>?@[]^_{|}~\\");

        public static char ValidateLikeEscapeChar(char escapeChar)
        {
            if (!AllowedLikeEscapeChars.Contains(escapeChar))
                throw new ArgumentException($"Invalid LIKE escape character: {escapeChar}");

            return escapeChar;
        }

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
