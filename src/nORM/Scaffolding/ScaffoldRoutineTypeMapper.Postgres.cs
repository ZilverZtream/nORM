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
        {
            castType = string.Empty;
            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            if (open < 0)
                return false;

            var close = normalized.IndexOf(')', open + 1);
            var element = (close > open
                    ? normalized.Substring(open + 1, close - open - 1)
                    : normalized[(open + 1)..])
                .Trim()
                .TrimStart('_');

            castType = element switch
            {
                "int2" or "smallint" => "smallint[]",
                "int4" or "integer" => "integer[]",
                "int8" or "bigint" => "bigint[]",
                "float4" or "real" => "real[]",
                "float8" or "double precision" => "double precision[]",
                "numeric" or "decimal" => "numeric[]",
                "bool" or "boolean" => "boolean[]",
                "uuid" => "uuid[]",
                "text" => "text[]",
                "varchar" or "character varying" => "character varying[]",
                "bpchar" or "char" or "character" => "character[]",
                "citext" => "citext[]",
                "bytea" => "bytea[]",
                "date" => "date[]",
                "time" or "time without time zone" => "time without time zone[]",
                "timetz" or "time with time zone" => "time with time zone[]",
                "interval" => "interval[]",
                "timestamp" or "timestamp without time zone" => "timestamp without time zone[]",
                "timestamptz" or "timestamp with time zone" => "timestamp with time zone[]",
                _ => string.Empty
            };

            return castType.Length > 0;
        }

        private static bool TryMapPostgresArrayRoutineType(string normalized, out string typeName)
        {
            typeName = string.Empty;
            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            var close = normalized.IndexOf(')', open + 1);
            if (open < 0 || close <= open)
                return false;

            var element = normalized.Substring(open + 1, close - open - 1).Trim().TrimStart('_');
            typeName = element switch
            {
                "int2" or "smallint" => "short[]",
                "int4" or "integer" => "int[]",
                "int8" or "bigint" => "long[]",
                "float4" or "real" => "float[]",
                "float8" or "double precision" => "double[]",
                "numeric" or "decimal" => "decimal[]",
                "bool" or "boolean" => "bool[]",
                "uuid" => "Guid[]",
                "text" or "varchar" or "character varying" or "bpchar" or "char" or "character" or "citext" => "string[]",
                "bytea" => "byte[][]",
                "date" => "DateOnly[]",
                "time" or "time without time zone" or "time with time zone" => "TimeOnly[]",
                "interval" => "TimeSpan[]",
                "timestamp" or "timestamp without time zone" => "DateTime[]",
                "timestamptz" or "timestamp with time zone" => "DateTimeOffset[]",
                _ => string.Empty
            };

            return typeName.Length > 0;
        }
    }
}
