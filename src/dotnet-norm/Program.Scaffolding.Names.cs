using System;
using System.Linq;
using nORM.Core;

partial class Program
{
    static string ResolveScaffoldNamespace(string? explicitNamespace, ScaffoldProjectInfo? projectInfo, string outputDirectory)
    {
        var explicitValue = NullIfWhiteSpace(explicitNamespace);
        if (explicitValue is not null)
            return explicitValue;

        if (projectInfo is null)
            return "Scaffolded";

        var baseNamespace = FirstNonBlank(projectInfo.DefaultNamespace, "Scaffolded")!;
        return AppendNamespacePath(baseNamespace, GetRelativeDirectorySegments(projectInfo.ProjectDirectory, outputDirectory));
    }

    static string ValidateScaffoldNamespaceName(string namespaceName, string source)
    {
        if (IsValidScaffoldNamespaceName(namespaceName))
            return namespaceName;

        throw new NormConfigurationException(
            $"Scaffold {source} '{namespaceName}' is not a valid C# namespace. Use a dot-separated namespace such as 'MyApp.Data'.");
    }

    static string ValidateScaffoldContextClassName(string contextClassName)
    {
        if (IsValidScaffoldIdentifier(contextClassName))
            return contextClassName;

        throw new NormConfigurationException(
            $"Scaffold --context class name '{contextClassName}' is not a valid C# type identifier. Use a class name such as 'AppDbContext' or a namespace-qualified value such as 'MyApp.Data.AppDbContext'.");
    }

    static bool IsValidScaffoldNamespaceName(string namespaceName)
    {
        if (string.IsNullOrWhiteSpace(namespaceName))
            return false;

        return namespaceName.Split('.').All(IsValidScaffoldNamespaceSegment);
    }

    static bool IsValidScaffoldNamespaceSegment(string segment)
        => IsValidScaffoldIdentifier(segment);

    static bool IsValidScaffoldIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var start = value[0] == '@' ? 1 : 0;
        if (start == value.Length)
            return false;

        if (!(char.IsLetter(value[start]) || value[start] == '_'))
            return false;

        for (var i = start + 1; i < value.Length; i++)
        {
            if (!(char.IsLetterOrDigit(value[i]) || value[i] == '_'))
                return false;
        }

        return start != 0 || !IsCSharpKeyword(value);
    }

    static bool IsCSharpKeyword(string value)
        => value is
            "abstract" or "as" or "base" or "bool" or "break" or "byte" or "case" or "catch" or "char" or "checked" or
            "class" or "const" or "continue" or "decimal" or "default" or "delegate" or "do" or "double" or "else" or
            "enum" or "event" or "explicit" or "extern" or "false" or "finally" or "fixed" or "float" or "for" or
            "foreach" or "goto" or "if" or "implicit" or "in" or "int" or "interface" or "internal" or "is" or "lock" or
            "long" or "namespace" or "new" or "null" or "object" or "operator" or "out" or "override" or "params" or
            "private" or "protected" or "public" or "readonly" or "ref" or "return" or "sbyte" or "sealed" or "short" or
            "sizeof" or "stackalloc" or "static" or "string" or "struct" or "switch" or "this" or "throw" or "true" or
            "try" or "typeof" or "uint" or "ulong" or "unchecked" or "unsafe" or "ushort" or "using" or "virtual" or
            "void" or "volatile" or "while" or
            "record" or "partial" or "var" or "dynamic" or "async" or "await" or "nameof" or "when" or "and" or "or" or
            "not" or "with" or "init" or "required" or "file" or "scoped" or "global" or "managed" or "unmanaged" or
            "nint" or "nuint" or "value" or "yield";

}
