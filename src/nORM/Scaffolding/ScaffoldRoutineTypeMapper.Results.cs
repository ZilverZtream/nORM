#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        public static string GetRoutineResultColumnTypeName(string? dataType, bool nullable, bool useNullableReferenceTypes = true)
        {
            var typeName = GetRoutineParameterTypeName(dataType, useNullableReferenceTypes);
            if (typeName.EndsWith("?", StringComparison.Ordinal))
                return nullable ? typeName : typeName[..^1];

            return nullable && useNullableReferenceTypes && typeName is not "object" ? typeName + "?" : typeName;
        }

        public static bool IsNonScalarRoutineResultDataType(string dataType)
            => NormalizeRoutineDataType(dataType) is "record" or "table" or "void";
    }
}
