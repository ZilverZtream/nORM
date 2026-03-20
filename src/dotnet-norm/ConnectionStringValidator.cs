using System;
using System.Data.Common;

namespace nORM.Security
{
    public static class ConnectionStringValidator
    {
        private static readonly string[] ForbiddenKeywords =
        {
            "xp_cmdshell", "sp_configure", "openrowset", "opendatasource",
            "bulk insert", "xp_regread", "xp_regwrite", "sp_oacreate"
        };

        private static readonly string[] RequiredSqlServerSettings =
        {
            "Encrypt=True", "TrustServerCertificate=False"
        };

        public static string ValidateAndSanitize(string connectionString, string provider)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty");

            // Check for injection patterns
            var upperConnectionString = connectionString.ToUpperInvariant();
            foreach (var forbidden in ForbiddenKeywords)
            {
                if (upperConnectionString.Contains(forbidden.ToUpperInvariant()))
                    throw new ArgumentException($"Connection string contains forbidden keyword: {forbidden}");
            }

            // Validate connection string format
            try
            {
                var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

                // Remove sensitive information from potential logs
                var sanitized = new DbConnectionStringBuilder();
                foreach (string key in builder.Keys)
                {
                    if (!IsSensitiveKey(key))
                        sanitized[key] = builder[key];
                    else
                        sanitized[key] = "***";
                }

                // Enforce security requirements for SQL Server
                if (provider.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
                {
                    EnforceSqlServerSecurity(builder);
                }

                return sanitized.ConnectionString;
            }
            catch (ArgumentException)
            {
                throw; // Re-throw our own forbidden keyword exceptions
            }
            catch (Exception ex) when (ex is FormatException or InvalidOperationException or System.Collections.Generic.KeyNotFoundException)
            {
                throw new ArgumentException("Invalid connection string format", ex);
            }
        }

        private static bool IsSensitiveKey(string key)
        {
            return key.ToLowerInvariant() switch
            {
                "password" or "pwd" or "user id" or "uid"
                    or "user password" or "access token" or "accesstoken"
                    or "token" or "secret" => true,
                _ => false
            };
        }

        private static void EnforceSqlServerSecurity(DbConnectionStringBuilder builder)
        {
            // Enforce encryption
            if (!builder.ContainsKey("Encrypt") ||
                !bool.TryParse(builder["Encrypt"]?.ToString(), out var encrypt) || !encrypt)
            {
                builder["Encrypt"] = "True";
            }

            // Enforce certificate validation in production
            if (!string.Equals(
                    Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT"),
                    "Development",
                    StringComparison.OrdinalIgnoreCase))
            {
                builder["TrustServerCertificate"] = "False";
            }
        }
    }
}

