using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

partial class Program
{
    static string AppendNamespacePath(string baseNamespace, IEnumerable<string> pathSegments)
    {
        var namespaceSegments = NormalizeNamespacePathSegments(pathSegments).ToArray();
        return namespaceSegments.Length == 0
            ? baseNamespace
            : baseNamespace + "." + string.Join(".", namespaceSegments);
    }

    static IEnumerable<string> GetRelativeDirectorySegments(string rootDirectory, string targetDirectory)
    {
        var fullRoot = Path.GetFullPath(rootDirectory);
        var fullTarget = Path.GetFullPath(targetDirectory);
        var relative = Path.GetRelativePath(fullRoot, fullTarget);
        if (relative == "." || IsParentRelativePath(relative) || Path.IsPathRooted(relative))
            return Array.Empty<string>();

        return SplitDirectorySegments(relative);
    }

    static IEnumerable<string> SplitDirectorySegments(string path)
        => path.Split(new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static segment => segment is not "." and not "..");

    static bool IsParentRelativePath(string relative)
        => relative == ".."
           || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)
           || relative.StartsWith(".." + Path.AltDirectorySeparatorChar, StringComparison.Ordinal);

    static IEnumerable<string> NormalizeNamespacePathSegments(IEnumerable<string> segments)
        => segments
            .Select(NormalizeProjectNamespace)
            .Where(static segment => segment is not null)
            .SelectMany(static segment => segment!.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

    static string? NormalizeProjectNamespace(string? value)
    {
        var trimmed = NullIfWhiteSpace(value);
        if (trimmed is null)
            return null;

        var parts = trimmed.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ToCSharpIdentifier)
            .Where(static segment => !string.IsNullOrWhiteSpace(segment))
            .ToArray();
        return parts.Length == 0 ? null : string.Join(".", parts);
    }
}
