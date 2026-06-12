using System;
using System.Collections.Generic;

namespace nORM.Cli;

public static partial class ProviderMobilitySchemaInspector
{
    private static readonly HashSet<string> PortableClrTypes = new(StringComparer.Ordinal)
    {
        typeof(bool).FullName!,
        typeof(byte).FullName!,
        typeof(sbyte).FullName!,
        typeof(short).FullName!,
        typeof(ushort).FullName!,
        typeof(int).FullName!,
        typeof(uint).FullName!,
        typeof(long).FullName!,
        typeof(ulong).FullName!,
        typeof(float).FullName!,
        typeof(double).FullName!,
        typeof(decimal).FullName!,
        typeof(string).FullName!,
        typeof(Guid).FullName!,
        typeof(DateTime).FullName!,
        typeof(DateTimeOffset).FullName!,
        typeof(DateOnly).FullName!,
        typeof(TimeOnly).FullName!,
        typeof(TimeSpan).FullName!,
        typeof(char).FullName!,
        typeof(byte[]).FullName!
    };

    private static bool IsPortableClrType(string clrType, out bool unresolvedCustomType)
    {
        unresolvedCustomType = false;
        if (PortableClrTypes.Contains(clrType))
            return true;

        var resolved = ResolveType(clrType);
        if (resolved?.IsEnum == true)
            return true;

        unresolvedCustomType = !clrType.StartsWith("System.", StringComparison.Ordinal);
        return false;
    }

    private static bool IsPortableIdentityType(string clrType)
        => clrType == typeof(byte).FullName ||
           clrType == typeof(short).FullName ||
           clrType == typeof(int).FullName ||
           clrType == typeof(long).FullName;

    private static Type? ResolveType(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        var type = Type.GetType(typeName);
        if (type != null)
            return type;

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = assembly.GetType(typeName);
            if (type != null)
                return type;
        }

        return null;
    }
}
