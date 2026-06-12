using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Core;

partial class Program
{
    static IReadOnlyCollection<string> ParseTableFilters(string? commaSeparatedTables, IReadOnlyCollection<string>? repeatedTables)
    {
        var filters = new List<string>();
        filters.AddRange(ParseCliCsvList(commaSeparatedTables, "--tables"));
        if (repeatedTables is not null)
        {
            foreach (var item in repeatedTables)
            {
                if (string.IsNullOrWhiteSpace(item))
                    throw new NormConfigurationException("Scaffold --table values must be non-empty.");

                filters.Add(item.Trim());
            }
        }

        return filters
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static IReadOnlyCollection<string> ParseSchemaFilters(string? commaSeparatedSchemas, IReadOnlyCollection<string>? repeatedSchemas)
    {
        var filters = new List<string>();
        filters.AddRange(ParseCliCsvList(commaSeparatedSchemas, "--schemas"));
        if (repeatedSchemas is not null)
        {
            foreach (var item in repeatedSchemas)
            {
                if (string.IsNullOrWhiteSpace(item))
                    throw new NormConfigurationException("Scaffold --schema values must be non-empty.");

                filters.Add(item.Trim());
            }
        }

        return filters
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    static IReadOnlyCollection<string> ParseCliCsvList(string? value, string optionName)
    {
        if (value is null)
            return Array.Empty<string>();

        if (string.IsNullOrWhiteSpace(value))
            throw new NormConfigurationException($"Scaffold {optionName} must include at least one non-empty filter.");

        var items = value.Split(',', StringSplitOptions.TrimEntries);
        if (items.Any(string.IsNullOrWhiteSpace))
            throw new NormConfigurationException($"Scaffold {optionName} must not contain empty filter entries.");

        return items;
    }
}
