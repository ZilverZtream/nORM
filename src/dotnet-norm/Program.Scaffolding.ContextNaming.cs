using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;

partial class Program
{
    static string InferScaffoldContextName(string connectionString, string connectionReference, string provider)
    {
        var baseName = TryGetNamedConnectionLeaf(connectionReference)
                       ?? TryGetScaffoldDatabaseName(connectionString, provider)
                       ?? "AppDb";
        var identifier = ToPascalIdentifier(baseName);
        return identifier.EndsWith("Context", StringComparison.Ordinal)
            ? identifier
            : identifier + "Context";
    }

    static string? TryGetScaffoldDatabaseName(string connectionString, string provider)
    {
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            var normalizedProvider = NormalizeProviderName(provider);
            if (normalizedProvider == "sqlite")
            {
                var dataSource = TryGetConnectionStringValue(builder, "Data Source", "DataSource", "Filename");
                if (dataSource is null || dataSource == ":memory:")
                    return null;

                var fileName = Path.GetFileNameWithoutExtension(dataSource);
                return NullIfWhiteSpace(fileName);
            }

            return TryGetConnectionStringValue(builder, "Database", "Initial Catalog");
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException)
        {
            return null;
        }
    }

    static string? TryGetConnectionStringValue(DbConnectionStringBuilder builder, params string[] keys)
    {
        foreach (var key in keys)
        {
            foreach (string existingKey in builder.Keys)
            {
                if (existingKey.Equals(key, StringComparison.OrdinalIgnoreCase))
                    return NullIfWhiteSpace(builder[existingKey]?.ToString());
            }
        }

        return null;
    }

    static string? TryGetNamedConnectionLeaf(string connectionReference)
    {
        if (!IsNamedConnectionReference(connectionReference))
            return null;

        var name = NullIfWhiteSpace(connectionReference.Trim()["Name=".Length..]);
        if (name is null)
            return null;

        var leaf = name.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).LastOrDefault();
        return NullIfWhiteSpace(leaf);
    }

    static string ToPascalIdentifier(string value)
    {
        var builder = new StringBuilder(value.Length);
        var nextUpper = true;
        foreach (var ch in value)
        {
            if (!char.IsLetterOrDigit(ch))
            {
                nextUpper = true;
                continue;
            }

            builder.Append(nextUpper ? char.ToUpperInvariant(ch) : ch);
            nextUpper = false;
        }

        return ToCSharpIdentifier(builder.Length == 0 ? "AppDb" : builder.ToString());
    }
}
