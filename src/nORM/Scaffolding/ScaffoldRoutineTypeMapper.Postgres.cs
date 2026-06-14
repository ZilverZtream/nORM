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
            if (TryMapPostgresDomainFunctionArgumentCastType(dataType, out castType))
                return true;

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

    }
}
