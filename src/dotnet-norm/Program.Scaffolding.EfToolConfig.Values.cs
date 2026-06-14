using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using nORM.Core;

partial class Program
{
    static string? ReadEfToolConfigString(JsonElement root, string propertyName)
    {
        if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
            return null;

        if (property.ValueKind != JsonValueKind.String)
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a string.");

        var value = NullIfWhiteSpace(property.GetString());
        if (value is null)
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not be blank.");

        return value;
    }

    static string? ReadFirstEfToolConfigString(JsonElement root, params string[] propertyNames)
    {
        foreach (var propertyName in propertyNames)
        {
            var value = ReadEfToolConfigString(root, propertyName);
            if (value is not null)
                return value;
        }

        return null;
    }

    static IReadOnlyList<string> ReadEfToolConfigStringList(JsonElement root, params string[] propertyNames)
    {
        var values = new List<string>();
        foreach (var propertyName in propertyNames)
        {
            if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
                continue;

            if (property.ValueKind == JsonValueKind.String)
            {
                AddEfToolConfigListValue(values, property.GetString(), propertyName);
                continue;
            }

            if (property.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in property.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.String)
                        throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must contain only strings.");

                    AddEfToolConfigListValue(values, item.GetString(), propertyName);
                }

                continue;
            }

            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a string or string array.");
        }

        return values
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static void AddEfToolConfigListValue(List<string> values, string? value, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not contain blank values.");

        var parts = value.Split(',', StringSplitOptions.TrimEntries);
        if (parts.Any(string.IsNullOrWhiteSpace))
            throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must not contain blank values.");

        values.AddRange(parts);
    }

    static bool? ReadEfToolConfigBool(JsonElement root, string propertyName)
    {
        if (!TryGetJsonPropertyIgnoreCase(root, propertyName, out var property) || property.ValueKind == JsonValueKind.Null)
            return null;

        return property.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => throw new NormConfigurationException($"EF tool configuration property '{propertyName}' must be a boolean.")
        };
    }
}
