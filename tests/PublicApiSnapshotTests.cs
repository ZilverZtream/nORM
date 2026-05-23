using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using nORM.Core;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class PublicApiSnapshotTests
{
    [Fact]
    public void Public_api_matches_v1_baseline()
    {
        var baselinePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "PublicApi.Shipped.txt");
        baselinePath = Path.GetFullPath(baselinePath);
        var actual = GetPublicApiLines(typeof(DbContext).Assembly).ToArray();

        if (Environment.GetEnvironmentVariable("NORM_UPDATE_PUBLIC_API") == "1")
        {
            File.WriteAllLines(baselinePath, actual);
            return;
        }

        Assert.True(File.Exists(baselinePath),
            $"Missing public API baseline at {baselinePath}. Run with NORM_UPDATE_PUBLIC_API=1 to create it intentionally.");

        var expected = File.ReadAllLines(baselinePath)
            .Where(static line => line.Length != 0 && !line.StartsWith("#", StringComparison.Ordinal))
            .ToArray();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Public_api_has_no_public_fields_outside_enums()
    {
        var fields = typeof(DbContext).Assembly.GetExportedTypes()
            .Where(static type => !type.IsEnum)
            .SelectMany(static type => type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(field => $"{type.FullName}.{field.Name}"))
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Empty(fields);
    }

    private static IEnumerable<string> GetPublicApiLines(Assembly assembly)
    {
        var lines = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var type in assembly.GetExportedTypes().OrderBy(static t => t.FullName, StringComparer.Ordinal))
        {
            lines.Add("T:" + FormatType(type));

            foreach (var ctor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                         .OrderBy(static c => c.ToString(), StringComparer.Ordinal))
            {
                lines.Add("M:" + FormatType(type) + ".#ctor(" +
                          string.Join(",", ctor.GetParameters().Select(static p => FormatType(p.ParameterType))) + ")");
            }

            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .Where(static m => !m.IsSpecialName)
                         .OrderBy(static m => m.Name, StringComparer.Ordinal)
                         .ThenBy(static m => string.Join(",", m.GetParameters().Select(static p => FormatType(p.ParameterType))), StringComparer.Ordinal))
            {
                lines.Add("M:" + FormatType(type) + "." + FormatMethod(method));
            }

            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static p => p.Name, StringComparer.Ordinal))
            {
                lines.Add("P:" + FormatType(type) + "." + property.Name + ":" + FormatType(property.PropertyType));
            }

            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static f => f.Name, StringComparer.Ordinal))
            {
                lines.Add("F:" + FormatType(type) + "." + field.Name + ":" + FormatType(field.FieldType));
            }

            foreach (var eventInfo in type.GetEvents(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static e => e.Name, StringComparer.Ordinal))
            {
                lines.Add("E:" + FormatType(type) + "." + eventInfo.Name + ":" + FormatType(eventInfo.EventHandlerType!));
            }
        }

        return lines;
    }

    private static string FormatMethod(MethodInfo method)
    {
        var generic = method.IsGenericMethodDefinition ? "``" + method.GetGenericArguments().Length : string.Empty;
        return method.Name + generic + "(" +
               string.Join(",", method.GetParameters().Select(static p => FormatType(p.ParameterType))) +
               "):" + FormatType(method.ReturnType);
    }

    private static string FormatType(Type type)
    {
        if (type.IsByRef)
            return FormatType(type.GetElementType()!) + "&";
        if (type.IsArray)
            return FormatType(type.GetElementType()!) + "[]";
        if (type.IsGenericParameter)
            return "`" + type.GenericParameterPosition;

        var nullable = Nullable.GetUnderlyingType(type);
        if (nullable != null)
            return "System.Nullable{" + FormatType(nullable) + "}";

        if (!type.IsGenericType)
            return (type.FullName ?? type.Name).Replace('+', '.');

        var definition = type.GetGenericTypeDefinition();
        var name = (definition.FullName ?? definition.Name).Replace('+', '.');
        var tick = name.IndexOf('`', StringComparison.Ordinal);
        if (tick >= 0)
            name = name[..tick];

        return name + "{" + string.Join(",", type.GetGenericArguments().Select(FormatType)) + "}";
    }
}
