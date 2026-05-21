using System;

namespace nORM.Tests;

internal static class LiveProviderEnvironment
{
    public static string? GetConnectionString(string kind)
        => kind.ToLowerInvariant() switch
        {
            "sqlserver" => GetByCanonicalName("NORM_TEST_SQLSERVER"),
            "mysql" => GetByCanonicalName("NORM_TEST_MYSQL"),
            "postgres" or "postgresql" => GetByCanonicalName("NORM_TEST_POSTGRES"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown live provider kind.")
        };

    public static string? GetByCanonicalName(string envVar)
    {
        var value = Environment.GetEnvironmentVariable(envVar);
        var aliasValue = envVar.EndsWith("_CS", StringComparison.Ordinal)
            ? Environment.GetEnvironmentVariable(envVar[..^3])
            : Environment.GetEnvironmentVariable(envVar + "_CS");

        if (!string.IsNullOrEmpty(value) && !IsEnableFlag(value))
            return value;

        if (!string.IsNullOrEmpty(aliasValue))
            return aliasValue;

        return string.IsNullOrEmpty(value) ? null : value;
    }

    private static bool IsEnableFlag(string value)
        => value.Equals("1", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("enabled", StringComparison.OrdinalIgnoreCase);

    public static bool IsConfigured(string kind) => !string.IsNullOrEmpty(GetConnectionString(kind));

    public static int ConfiguredProviderCount()
    {
        var count = 0;
        if (IsConfigured("sqlserver")) count++;
        if (IsConfigured("mysql")) count++;
        if (IsConfigured("postgres")) count++;
        return count;
    }
}
