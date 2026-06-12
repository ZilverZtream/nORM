#nullable enable
using System;
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static void AppendRoutineParameterType(StringBuilder sb, string? parameterType, IReadOnlyList<RoutineStubParameter> inputParameters)
        {
            if (parameterType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    public sealed class {parameterType}");
            sb.AppendLine("    {");
            foreach (var parameter in inputParameters)
                sb.AppendLine($"        public {parameter.TypeName} {parameter.Name} {{ get; init; }}");
            sb.AppendLine("    }");
        }

        private static void AppendRoutineResultType(StringBuilder sb, string? resultType, IReadOnlyList<RoutineResultColumn> resultColumns, bool useNullableReferenceTypes)
        {
            if (resultType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    public sealed class {resultType}");
            sb.AppendLine("    {");
            foreach (var column in resultColumns)
            {
                var initializer = RequiresDefaultInitializer(column.TypeName, useNullableReferenceTypes) ? " = default!;" : string.Empty;
                sb.AppendLine($"        public {column.TypeName} {column.Name} {{ get; set; }}{initializer}");
            }
            sb.AppendLine("    }");
        }

        private static bool RequiresDefaultInitializer(string typeName, bool useNullableReferenceTypes = true)
            => useNullableReferenceTypes
               && !typeName.EndsWith("?", StringComparison.Ordinal)
               && (typeName == "string" || typeName.EndsWith("[]", StringComparison.Ordinal));
    }
}
