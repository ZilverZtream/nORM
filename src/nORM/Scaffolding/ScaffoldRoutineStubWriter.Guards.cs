#nullable enable
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static void AppendRoutineParameterGuard(StringBuilder sb, string nullableObjectType)
        {
            sb.AppendLine();
            sb.AppendLine($"    private static {nullableObjectType} RequireScaffoldedRoutineParameters({nullableObjectType} parameters, int expectedInputCount, string routineName)");
            sb.AppendLine("    {");
            sb.AppendLine("        if (expectedInputCount <= 0)");
            sb.AppendLine("            return parameters;");
            sb.AppendLine();
            sb.AppendLine("        if (parameters is null)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass a parameter object containing the scaffolded inputs.\");");
            sb.AppendLine();
            sb.AppendLine($"        if (parameters is IReadOnlyDictionary<string, {nullableObjectType}> dictionary && dictionary.Count != expectedInputCount)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass exactly {expectedInputCount} dictionary entries using the provider parameter names.\");");
            sb.AppendLine();
            sb.AppendLine("        return parameters;");
            sb.AppendLine("    }");
        }
    }
}
