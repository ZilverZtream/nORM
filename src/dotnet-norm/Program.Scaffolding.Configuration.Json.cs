using System;
using System.IO;
using System.Text.Json;
using nORM.Core;

partial class Program
{
    static bool TryReadJsonConfigurationValue(string jsonFile, string key, out string value)
    {
        value = "";
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(jsonFile));
            var current = document.RootElement;
            foreach (var segment in key.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (current.ValueKind != JsonValueKind.Object || !TryGetJsonPropertyIgnoreCase(current, segment, out current))
                    return false;
            }

            if (current.ValueKind != JsonValueKind.String)
                return false;

            var configured = NullIfWhiteSpace(current.GetString());
            if (configured is null)
                return false;

            value = configured;
            return true;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            throw new NormConfigurationException($"Could not read scaffold configuration file '{jsonFile}': {ex.Message}", ex);
        }
    }

    static bool TryGetJsonPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (property.NameEquals(propertyName) || property.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
