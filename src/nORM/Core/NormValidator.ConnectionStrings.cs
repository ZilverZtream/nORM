using System;
using System.Data.Common;
using System.IO;

#nullable enable

namespace nORM.Core
{
    public static partial class NormValidator
    {
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
