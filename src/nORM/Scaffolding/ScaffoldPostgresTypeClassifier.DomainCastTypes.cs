#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        private const string PostgresUserDefinedPrefix = "user-defined (";

        public static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
        {
            if (TryParsePostgresEnumValues(typeText, out _))
            {
                castType = "text";
                return true;
            }

            var normalized = typeText.Trim().ToLowerInvariant();
            if (TryNormalizePostgresParameterizedProbeCastType(normalized, out castType))
                return true;

            if (TryMapPostgresArrayProbeCastType(normalized, out castType))
                return true;

            if (TryNormalizePostgresUserDefinedScalarCastType(normalized, out castType))
                return true;

            return TryNormalizePostgresScalarProbeCastType(normalized, out castType);
        }

        public static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
            => TryGetPostgresUserDefinedTypeName(normalized, out var typeName)
               && TryNormalizePostgresSafeUserDefinedTypeName(typeName, out _);

        private static bool TryNormalizePostgresUserDefinedScalarCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            return TryGetPostgresUserDefinedTypeName(normalized, out var typeName)
                   && TryNormalizePostgresSafeUserDefinedTypeName(typeName, out castType);
        }

        private static bool TryGetPostgresUserDefinedTypeName(string normalized, out string typeName)
        {
            typeName = string.Empty;
            if (!normalized.StartsWith(PostgresUserDefinedPrefix, StringComparison.Ordinal)
                || !normalized.EndsWith(")", StringComparison.Ordinal))
            {
                return false;
            }

            typeName = normalized
                .Substring(PostgresUserDefinedPrefix.Length, normalized.Length - PostgresUserDefinedPrefix.Length - 1)
                .Trim()
                .TrimStart('_');
            return typeName.Length > 0;
        }

        private static bool TryNormalizePostgresSafeUserDefinedTypeName(string typeName, out string castType)
        {
            castType = typeName switch
            {
                "citext" => "citext",
                "json" => "json",
                "jsonb" => "jsonb",
                "xml" => "xml",
                "uuid" => "uuid",
                _ => string.Empty
            };

            return castType.Length > 0;
        }

        private static bool TryNormalizePostgresScalarProbeCastType(string normalized, out string castType)
        {
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
