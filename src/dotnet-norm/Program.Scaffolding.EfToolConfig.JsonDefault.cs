using System;
using System.IO;
using System.Text.Json;
using nORM.Core;

partial class Program
{
    static bool? ReadEfToolConfigJsonDefault()
    {
        var configPath = FindEfToolConfigPath();
        if (configPath is null)
            return null;

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                return null;

            return ReadEfToolConfigBool(document.RootElement, "json");
        }
        catch (NormConfigurationException)
        {
            throw;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            throw new NormConfigurationException($"Could not read EF tool configuration file '{configPath}': {ex.Message}", ex);
        }
    }
}
