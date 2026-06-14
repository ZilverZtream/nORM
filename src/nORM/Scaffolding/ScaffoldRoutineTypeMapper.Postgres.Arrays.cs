#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        private static bool TryMapPostgresFunctionArrayArgumentCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

        private static bool TryMapPostgresArrayRoutineType(string normalized, out string typeName)
        {
            typeName = string.Empty;
            if (!ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(normalized, out var arrayType)
                || !arrayType.IsArray)
            {
                return false;
            }

            var elementType = arrayType.GetElementType();
            if (elementType is null)
                return false;

            typeName = elementType switch
            {
                var type when type == typeof(short) => "short[]",
                var type when type == typeof(int) => "int[]",
                var type when type == typeof(long) => "long[]",
                var type when type == typeof(float) => "float[]",
                var type when type == typeof(double) => "double[]",
                var type when type == typeof(decimal) => "decimal[]",
                var type when type == typeof(bool) => "bool[]",
                var type when type == typeof(Guid) => "Guid[]",
                var type when type == typeof(string) => "string[]",
                var type when type == typeof(byte[]) => "byte[][]",
                var type when type == typeof(DateOnly) => "DateOnly[]",
                var type when type == typeof(TimeOnly) => "TimeOnly[]",
                var type when type == typeof(TimeSpan) => "TimeSpan[]",
                var type when type == typeof(DateTime) => "DateTime[]",
                var type when type == typeof(DateTimeOffset) => "DateTimeOffset[]",
                _ => string.Empty
            };

            return typeName.Length > 0;
        }
    }
}
