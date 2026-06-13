#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        public static IReadOnlyList<string> GetPostgresFunctionArgumentCastTypes(
            ScaffoldRoutineStubInfo routine,
            IReadOnlyList<string> inputParameterDataTypes,
            int expectedArgumentCount)
        {
            if (!routine.Detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase)
                || expectedArgumentCount <= 0
                || inputParameterDataTypes.Count != expectedArgumentCount)
            {
                return Array.Empty<string>();
            }

            var castTypes = new List<string>(inputParameterDataTypes.Count);
            foreach (var dataType in inputParameterDataTypes)
            {
                if (!TryMapPostgresFunctionArgumentCastType(dataType, out var castType))
                    return Array.Empty<string>();

                castTypes.Add(castType);
            }

            return castTypes;
        }

        private static bool TryMapPostgresFunctionArgumentCastType(string? dataType, out string castType)
        {
            var normalized = NormalizeRoutineDataType(dataType ?? string.Empty);
            if (TryMapPostgresFunctionArrayArgumentCastType(normalized, out castType))
                return true;

            castType = normalized switch
            {
                "integer" or "int" or "int4" => "integer",
                "bigint" or "int8" => "bigint",
                "smallint" or "int2" => "smallint",
                "boolean" or "bool" => "boolean",
                "uuid" => "uuid",
                "date" => "date",
                "text" => "text",
                "citext" => "citext",
                "json" => "json",
                "jsonb" => "jsonb",
                "xml" => "xml",
                "character varying" or "varchar" => "character varying",
                "character" or "char" => "character",
                "numeric" or "decimal" => "numeric",
                "real" or "float4" => "real",
                "double precision" or "float8" => "double precision",
                "bytea" => "bytea",
                "timestamp without time zone" or "timestamp" => "timestamp without time zone",
                "timestamp with time zone" or "timestamptz" => "timestamp with time zone",
                "time without time zone" or "time" => "time without time zone",
                "time with time zone" or "timetz" => "time with time zone",
                "interval" => "interval",
                _ => string.Empty
            };

            return castType.Length > 0;
        }

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
