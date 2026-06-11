using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using nORM.Core;

partial class Program
{
    static string? ReadProjectDefaultNamespace(string projectFile)
    {
        try
        {
            var inheritedRootNamespace = ReadNearestDirectoryBuildPropsProperty(projectFile, "RootNamespace", NormalizeProjectNamespace);
            var inheritedAssemblyName = ReadNearestDirectoryBuildPropsProperty(projectFile, "AssemblyName", NormalizeProjectNamespace);
            var document = XDocument.Load(projectFile);
            var propertyGroups = GetPropertyGroups(document);
            var rootNamespace = propertyGroups
                .Elements()
                .Where(static element => element.Name.LocalName == "RootNamespace")
                .Select(static element => NormalizeProjectNamespace(element.Value))
                .LastOrDefault(static value => value is not null);
            if (rootNamespace is not null)
                return rootNamespace;
            if (inheritedRootNamespace is not null)
                return inheritedRootNamespace;

            var assemblyName = propertyGroups
                .Elements()
                .Where(static element => element.Name.LocalName == "AssemblyName")
                .Select(static element => NormalizeProjectNamespace(element.Value))
                .LastOrDefault(static value => value is not null);
            if (assemblyName is not null)
                return assemblyName;
            if (inheritedAssemblyName is not null)
                return inheritedAssemblyName;

            return NormalizeProjectNamespace(Path.GetFileNameWithoutExtension(projectFile));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold --project file '{projectFile}': {ex.Message}", ex);
        }
    }

    static string? ReadProjectUserSecretsId(string projectFile)
    {
        try
        {
            var inheritedUserSecretsId = ReadNearestDirectoryBuildPropsProperty(projectFile, "UserSecretsId", NullIfWhiteSpace);
            var document = XDocument.Load(projectFile);
            return GetPropertyGroups(document)
                .Elements()
                .Where(static element => element.Name.LocalName == "UserSecretsId")
                .Select(static element => NullIfWhiteSpace(element.Value))
                .LastOrDefault(static value => value is not null)
                ?? inheritedUserSecretsId;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold --project file '{projectFile}': {ex.Message}", ex);
        }
    }

    static bool ReadProjectUseNullableReferenceTypes(string projectFile)
    {
        try
        {
            var inheritedNullable = ReadNearestDirectoryBuildPropsProperty(projectFile, "Nullable", NullIfWhiteSpace);
            var projectNullable = ReadNullableProperty(projectFile);
            return IsNullableReferenceTypesEnabled(projectNullable ?? inheritedNullable);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or XmlException)
        {
            throw new NormConfigurationException($"Could not read scaffold nullable settings for project '{projectFile}': {ex.Message}", ex);
        }
    }

    static string? ReadNearestDirectoryBuildPropsProperty(string projectFile, string propertyName, Func<string?, string?> normalize)
    {
        for (var directory = Path.GetDirectoryName(projectFile); !string.IsNullOrWhiteSpace(directory); directory = Directory.GetParent(directory)?.FullName)
        {
            var propsPath = Path.Combine(directory, "Directory.Build.props");
            if (File.Exists(propsPath))
                return ReadProjectProperty(propsPath, propertyName, normalize);
        }

        return null;
    }

    static string? ReadNullableProperty(string projectOrPropsFile)
        => ReadProjectProperty(projectOrPropsFile, "Nullable", NullIfWhiteSpace);

    static string? ReadProjectProperty(string projectOrPropsFile, string propertyName, Func<string?, string?> normalize)
    {
        var document = XDocument.Load(projectOrPropsFile);
        return GetPropertyGroups(document)
            .Elements()
            .Where(element => element.Name.LocalName == propertyName)
            .Select(element => normalize(element.Value))
            .LastOrDefault(static value => value is not null);
    }

    static IEnumerable<XElement> GetPropertyGroups(XDocument document)
        => document.Root?.Elements()
            .Where(static element => element.Name.LocalName == "PropertyGroup")
            ?? Enumerable.Empty<XElement>();

    static bool IsNullableReferenceTypesEnabled(string? nullableValue)
    {
        var normalized = NullIfWhiteSpace(nullableValue)?.Trim().ToLowerInvariant();
        return normalized is "enable" or "annotations";
    }
}
