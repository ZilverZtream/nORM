using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
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

    static bool ScaffoldWarningsExist(string outputDirectory)
        => File.Exists(Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json"))
           || File.Exists(Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"));

    static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    static bool IsScaffoldWarningsFailure(NormConfigurationException exception, string outputDirectory)
        => exception.Message.Contains("Scaffolding produced warnings", StringComparison.Ordinal)
           && ScaffoldWarningsExist(outputDirectory);

    static void PrintScaffoldWarningSummary(string outputDirectory)
    {
        var summary = ReadScaffoldWarningSummary(outputDirectory);
        if (summary.TotalWarnings == 0)
        {
            if (summary.Error is not null)
                Console.WriteLine($"Scaffolding warning summary unavailable: {summary.Error}");
            return;
        }

        Console.WriteLine($"Scaffolding warning summary: {summary.TotalWarnings} warning(s) across {summary.Sections} section(s).");
        Console.WriteLine("Codes: " + string.Join(", ", summary.Codes.Select(pair => $"{pair.Key}={pair.Value}")));
        Console.WriteLine("Categories: " + string.Join(", ", summary.Categories.Select(pair => $"{pair.Key}={pair.Value}")));
    }

    static ScaffoldWarningSummary ReadScaffoldWarningSummary(string outputDirectory)
    {
        var jsonPath = Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json");
        if (!File.Exists(jsonPath))
            return ScaffoldWarningSummary.Empty;

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(jsonPath));
            var root = document.RootElement;
            var sections = new[]
            {
                "compositeForeignKeys",
                "possibleManyToManyJoinTables",
                "providerOwnedSchemaFeatures",
                "skippedDatabaseObjects"
            };
            var codeCounts = new SortedDictionary<string, int>(StringComparer.Ordinal);
            var categoryCounts = new SortedDictionary<string, int>(StringComparer.Ordinal);
            var total = 0;
            var nonEmptySections = 0;

            foreach (var section in sections)
            {
                if (!root.TryGetProperty(section, out var items) || items.ValueKind != JsonValueKind.Array)
                    continue;

                var sectionCount = 0;
                foreach (var item in items.EnumerateArray())
                {
                    total++;
                    sectionCount++;
                    IncrementDiagnosticCount(codeCounts, ReadScaffoldDiagnosticProperty(item, "code", "unknown"));
                    IncrementDiagnosticCount(categoryCounts, ReadScaffoldDiagnosticProperty(item, "category", "uncategorized"));
                }

                if (sectionCount > 0)
                    nonEmptySections++;
            }

            return new ScaffoldWarningSummary(total, nonEmptySections, codeCounts, categoryCounts, Error: null);
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            return new ScaffoldWarningSummary(0, 0, new SortedDictionary<string, int>(StringComparer.Ordinal), new SortedDictionary<string, int>(StringComparer.Ordinal), RedactConnectionStrings(ex.Message));
        }
    }

    static void WriteScaffoldResultJson(
        string status,
        string outputDirectory,
        bool dryRun,
        bool reportsWritten,
        ScaffoldWarningSummary warningSummary,
        Exception? error = null)
    {
        var payload = new
        {
            status,
            outputDirectory = Path.GetFullPath(outputDirectory),
            dryRun,
            warnings = new
            {
                hasDiagnostics = warningSummary.TotalWarnings > 0,
                reportsWritten,
                totalWarnings = warningSummary.TotalWarnings,
                sections = warningSummary.Sections,
                codes = warningSummary.Codes,
                categories = warningSummary.Categories,
                summaryError = warningSummary.Error
            },
            error = error is null ? null : RedactConnectionStrings(error.Message)
        };

        Console.WriteLine(JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }));
    }

    static void IncrementDiagnosticCount(IDictionary<string, int> counts, string key)
    {
        counts.TryGetValue(key, out var current);
        counts[key] = current + 1;
    }

    static string ReadScaffoldDiagnosticProperty(JsonElement item, string propertyName, string fallback)
        => item.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString() ?? fallback
            : fallback;

    sealed record ScaffoldWarningSummary(
        int TotalWarnings,
        int Sections,
        IReadOnlyDictionary<string, int> Codes,
        IReadOnlyDictionary<string, int> Categories,
        string? Error)
    {
        public static ScaffoldWarningSummary Empty { get; } = new(
            0,
            0,
            new SortedDictionary<string, int>(StringComparer.Ordinal),
            new SortedDictionary<string, int>(StringComparer.Ordinal),
            Error: null);
    }
}
