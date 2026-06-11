using System;
using System.Linq;
using System.Text;

partial class Program
{
    static string? NullIfWhiteSpace(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    static string? FirstNonBlank(params string?[] values)
        => values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))?.Trim();

    static string NormalizeProviderName(string providerName)
        => providerName.Trim().ToLowerInvariant() switch
        {
            "microsoft.entityframeworkcore.sqlserver" or "sqlserver" or "mssql" => "sqlserver",
            "microsoft.entityframeworkcore.sqlite" or "sqlite" => "sqlite",
            "npgsql.entityframeworkcore.postgresql" or "npgsql" or "postgres" or "postgresql" => "postgres",
            "pomelo.entityframeworkcore.mysql" or "mysql.entityframeworkcore" or "mysql" or "mariadb" => "mysql",
            var normalized => normalized
        };

    /// <summary>
    /// Replaces any recognizable connection string segments containing sensitive key=value pairs
    /// with [REDACTED] so that passwords and secrets are not printed to stderr on error.
    /// </summary>
    static string RedactConnectionStrings(string message)
    {
        if (string.IsNullOrEmpty(message))
            return message;

        // Sensitive keys whose values must be redacted (case-insensitive).
        var sensitiveKeys = new[] { "password", "pwd", "user password", "access token", "accesstoken", "token", "secret" };

        // Replace patterns of the form  key=somevalue;  or  key=somevalue (end of string).
        // Use a simple approach: for each sensitive key, replace the value portion.
        var result = message;
        foreach (var key in sensitiveKeys)
        {
            var pattern = key + "=";
            int idx = 0;
            while (true)
            {
                var pos = result.IndexOf(pattern, idx, StringComparison.OrdinalIgnoreCase);
                if (pos < 0) break;
                var valStart = pos + pattern.Length;
                var valEnd = result.IndexOf(';', valStart);
                if (valEnd < 0) valEnd = result.Length;
                result = result.Substring(0, valStart) + "[REDACTED]" + result.Substring(valEnd);
                idx = valStart + "[REDACTED]".Length;
            }
        }
        return result;
    }

    static string ToCSharpIdentifier(string value)
    {
        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++)
        {
            var ch = value[i];
            builder.Append(i == 0
                ? (char.IsLetter(ch) || ch == '_' ? ch : '_')
                : (char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_'));
        }

        return builder.Length == 0 ? "_" : builder.ToString();
    }
}
