using System;
using System.Data.Common;

namespace nORM.Security
{
    public static class ConnectionStringValidator
    {
        private static readonly string[] ForbiddenKeywords =
        {
            "xp_cmdshell", "sp_configure", "openrowset", "opendatasource"
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

                return builder.ConnectionString;
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Invalid connection string format", ex);
            }
        }

        private static bool IsSensitiveKey(string key)
        {
            return key.ToLowerInvariant() switch
            {
                "password" or "pwd" or "user id" or "uid" => true,
                _ => false
            };
        }

        private static void EnforceSqlServerSecurity(DbConnectionStringBuilder builder)
        {
            // Enforce encryption
            if (!builder.ContainsKey("Encrypt") || !bool.Parse(builder["Encrypt"].ToString()!))
            {
                builder["Encrypt"] = "True";
            }

            // Enforce certificate validation in production
            if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") != "Development")
            {
                builder["TrustServerCertificate"] = "False";
            }
        }
    }
}

