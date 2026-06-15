#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static string BuildRoutineParameterSignature(
            bool requiresPositionalFunctionArguments,
            bool requiresDictionaryRoutineArguments,
            string? parameterType,
            string nullableObjectType,
            string nullableReferenceSuffix)
        {
            if (requiresPositionalFunctionArguments)
                return $"{nullableObjectType}[]{nullableReferenceSuffix} arguments = null";

            if (requiresDictionaryRoutineArguments)
                return $"IReadOnlyDictionary<string, {nullableObjectType}>{nullableReferenceSuffix} parameters = null";

            return parameterType == null
                ? $"{nullableObjectType} parameters = null"
                : $"{parameterType}{nullableReferenceSuffix} parameters = null";
        }
    }
}
