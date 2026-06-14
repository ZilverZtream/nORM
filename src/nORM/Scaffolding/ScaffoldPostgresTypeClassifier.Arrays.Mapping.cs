#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        private static readonly PostgresArrayElementMapping[] PostgresArrayElementMappings =
        {
            new(new[] { "int2", "smallint" }, "smallint", typeof(short)),
            new(new[] { "int4", "integer" }, "integer", typeof(int)),
            new(new[] { "int8", "bigint" }, "bigint", typeof(long)),
            new(new[] { "float4", "real" }, "real", typeof(float)),
            new(new[] { "float8", "double precision" }, "double precision", typeof(double)),
            new(new[] { "numeric", "decimal" }, "numeric", typeof(decimal)),
            new(new[] { "bool", "boolean" }, "boolean", typeof(bool)),
            new(new[] { "uuid" }, "uuid", typeof(Guid)),
            new(new[] { "text" }, "text", typeof(string)),
            new(new[] { "varchar", "character varying" }, "character varying", typeof(string)),
            new(new[] { "bpchar", "char", "character" }, "character", typeof(string)),
            new(new[] { "citext" }, "citext", typeof(string)),
            new(new[] { "bytea" }, "bytea", typeof(byte[])),
            new(new[] { "date" }, "date", typeof(DateOnly)),
            new(new[] { "time", "time without time zone" }, "time without time zone", typeof(TimeOnly)),
            new(new[] { "timetz", "time with time zone" }, "time with time zone", typeof(DateTimeOffset)),
            new(new[] { "interval" }, "interval", typeof(TimeSpan)),
            new(new[] { "timestamp", "timestamp without time zone" }, "timestamp without time zone", typeof(DateTime)),
            new(new[] { "timestamptz", "timestamp with time zone" }, "timestamp with time zone", typeof(DateTimeOffset))
        };

        private static bool TryNormalizePostgresArrayElementCastType(string element, out string castType)
        {
            castType = string.Empty;
            if (!TryGetPostgresArrayElementMapping(element, out var mapping))
                return false;

            castType = mapping.CanonicalCastType + "[]";
            return true;
        }

        private static bool TryMapPostgresArrayElementClrType(string element, out Type elementType)
        {
            elementType = typeof(object);
            if (!TryGetPostgresArrayElementMapping(element, out var mapping))
                return false;

            elementType = mapping.ClrType;
            return true;
        }

        private static bool TryGetPostgresArrayElementMapping(string element, out PostgresArrayElementMapping mapping)
        {
            var normalized = NormalizePostgresArrayElementType(element);
            foreach (var candidate in PostgresArrayElementMappings)
            {
                if (candidate.Matches(normalized))
                {
                    mapping = candidate;
                    return true;
                }
            }

            mapping = default;
            return false;
        }

        private static string NormalizePostgresArrayElementType(string element)
        {
            element = element.Trim().TrimStart('_');
            if (TryNormalizePostgresParameterizedProbeCastType(element, out var parameterizedCastType))
                element = StripPostgresTypeArguments(parameterizedCastType);

            return element;
        }

        private readonly record struct PostgresArrayElementMapping(string[] Aliases, string CanonicalCastType, Type ClrType)
        {
            public bool Matches(string element)
            {
                foreach (var alias in Aliases)
                {
                    if (string.Equals(alias, element, StringComparison.Ordinal))
                        return true;
                }

                return false;
            }
        }
    }
}
