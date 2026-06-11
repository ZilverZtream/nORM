#nullable enable
using System;
using System.Data;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        public static (byte? Precision, byte? Scale) GetRoutineParameterPrecisionScale(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return (null, null);

            var trimmed = dataType.Trim();
            var open = trimmed.IndexOf('(');
            if (open < 0)
                return (null, null);

            var close = trimmed.IndexOf(')', open + 1);
            if (close < 0)
                return (null, null);

            var normalized = trimmed[..open].Trim().ToLowerInvariant();
            if (normalized is not ("decimal" or "numeric"))
                return (null, null);

            var parts = trimmed.Substring(open + 1, close - open - 1)
                .Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length is not (1 or 2)
                || parts.Any(static part => part.Length == 0))
                return (null, null);

            if (!byte.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var precision)
                || precision == 0)
            {
                return (null, null);
            }

            if (parts.Length == 1)
                return (precision, null);

            return byte.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var scale)
                   && scale <= precision
                ? (precision, scale)
                : (null, null);
        }

        public static string GetRoutineParameterDirection(string? mode)
            => mode?.Trim().ToUpperInvariant() switch
            {
                "INOUT" => nameof(ParameterDirection.InputOutput),
                "RETURN" => nameof(ParameterDirection.ReturnValue),
                _ => nameof(ParameterDirection.Output)
            };

        public static int? GetRoutineParameterSize(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return null;

            var trimmed = dataType.Trim();
            var open = trimmed.IndexOf('(');
            if (open < 0)
                return null;

            var close = trimmed.IndexOf(')', open + 1);
            if (close < 0)
                return null;

            var normalized = trimmed[..open].Trim().ToLowerInvariant();
            if (normalized is "character varying" or "varying character")
                normalized = "varchar";
            else if (normalized is "national character varying")
                normalized = "nvarchar";
            else if (normalized is "character")
                normalized = "char";

            if (normalized is not ("char" or "varchar" or "nchar" or "nvarchar" or "binary" or "varbinary"))
                return null;

            var value = trimmed.Substring(open + 1, close - open - 1).Trim();
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var size) && size > 0
                ? size
                : null;
        }

        public static string GetRoutineParameterTypeName(string? dataType, bool useNullableReferenceTypes = true)
        {
            var nullableReferenceSuffix = useNullableReferenceTypes ? "?" : string.Empty;
            if (string.IsNullOrWhiteSpace(dataType))
                return "object" + nullableReferenceSuffix;

            var normalized = NormalizeRoutineDataType(dataType);
            if (TryMapPostgresArrayRoutineType(normalized, out var arrayTypeName))
                return arrayTypeName + nullableReferenceSuffix;

            normalized = NormalizeProviderAlias(normalized);

            return normalized switch
            {
                "int" or "integer" or "int4" or "mediumint" => "int?",
                "int unsigned" or "mediumint unsigned" => "uint?",
                "bigint" or "int8" => "long?",
                "bigint unsigned" => "ulong?",
                "smallint" or "int2" => "short?",
                "smallint unsigned" => "ushort?",
                "tinyint" => "byte?",
                "tinyint unsigned" => "byte?",
                "bit" or "bool" or "boolean" => "bool?",
                "decimal" or "numeric" or "money" or "smallmoney" => "decimal?",
                "float" or "float8" or "double" => "double?",
                "real" or "float4" => "float?",
                "date" => "DateOnly?",
                "time" => "TimeOnly?",
                "interval" => "TimeSpan?",
                "datetime" or "datetime2" or "smalldatetime" or "timestamp" => "DateTime?",
                "datetimeoffset" or "timestamptz" => "DateTimeOffset?",
                "uniqueidentifier" or "uuid" => "Guid?",
                "table type" => "DbParameter" + nullableReferenceSuffix,
                "sysname" => "string" + nullableReferenceSuffix,
                "bpchar" => "string" + nullableReferenceSuffix,
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "citext" or "xml" or "json" or "jsonb" or "enum" or "set" => "string" + nullableReferenceSuffix,
                "binary" or "varbinary" or "image" or "bytea" or "blob" or "longblob" or "mediumblob" or "tinyblob" => "byte[]" + nullableReferenceSuffix,
                _ => "object" + nullableReferenceSuffix
            };
        }

        public static string GetRoutineParameterDbTypeName(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return nameof(DbType.Object);

            var normalized = NormalizeRoutineDataType(dataType);
            if (TryMapPostgresArrayRoutineType(normalized, out _))
                return nameof(DbType.Object);

            normalized = NormalizeProviderAlias(normalized);

            return normalized switch
            {
                "int" or "integer" or "int4" or "mediumint" => nameof(DbType.Int32),
                "int unsigned" or "mediumint unsigned" => nameof(DbType.UInt32),
                "bigint" or "int8" => nameof(DbType.Int64),
                "bigint unsigned" => nameof(DbType.UInt64),
                "smallint" or "int2" => nameof(DbType.Int16),
                "smallint unsigned" => nameof(DbType.UInt16),
                "tinyint" => nameof(DbType.Byte),
                "tinyint unsigned" => nameof(DbType.Byte),
                "bit" or "bool" or "boolean" => nameof(DbType.Boolean),
                "decimal" or "numeric" or "money" or "smallmoney" => nameof(DbType.Decimal),
                "float" or "float8" or "double" => nameof(DbType.Double),
                "real" or "float4" => nameof(DbType.Single),
                "date" => nameof(DbType.Date),
                "time" or "interval" => nameof(DbType.Time),
                "datetime" or "datetime2" or "smalldatetime" or "timestamp" => nameof(DbType.DateTime),
                "datetimeoffset" or "timestamptz" => nameof(DbType.DateTimeOffset),
                "uniqueidentifier" or "uuid" => nameof(DbType.Guid),
                "sysname" => nameof(DbType.String),
                "bpchar" => nameof(DbType.String),
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "citext" or "xml" or "json" or "jsonb" or "enum" or "set" => nameof(DbType.String),
                "binary" or "varbinary" or "image" or "bytea" or "blob" or "longblob" or "mediumblob" or "tinyblob" => nameof(DbType.Binary),
                _ => nameof(DbType.Object)
            };
        }

        public static string GetRoutineResultColumnTypeName(string? dataType, bool nullable, bool useNullableReferenceTypes = true)
        {
            var typeName = GetRoutineParameterTypeName(dataType, useNullableReferenceTypes);
            if (typeName.EndsWith("?", StringComparison.Ordinal))
                return nullable ? typeName : typeName[..^1];

            return nullable && useNullableReferenceTypes && typeName is not "object" ? typeName + "?" : typeName;
        }

        public static bool IsNonScalarRoutineResultDataType(string dataType)
            => NormalizeRoutineDataType(dataType) is "record" or "table" or "void";

        private static string NormalizeRoutineDataType(string dataType)
        {
            var normalized = dataType.Trim().ToLowerInvariant();
            var isUnsigned = normalized.Contains(" unsigned", StringComparison.Ordinal);
            var paren = normalized.IndexOf('(');
            var baseType = paren >= 0 ? normalized[..paren].Trim() : normalized;

            if (paren >= 0 && (baseType == "array" || baseType == "user-defined" || baseType == "table type"))
            {
                var close = normalized.IndexOf(')', paren + 1);
                if (close > paren)
                {
                    var inner = normalized.Substring(paren + 1, close - paren - 1).Trim();
                    if (!string.IsNullOrWhiteSpace(inner))
                    {
                        if (baseType == "array")
                            return "array (" + inner + ")";

                        if (baseType == "table type")
                            return "table type";

                        var userDefined = inner.TrimStart('_');
                        if (userDefined is "citext" or "json" or "jsonb" or "xml" or "uuid")
                            return userDefined;
                    }
                }
            }

            normalized = isUnsigned && !baseType.EndsWith(" unsigned", StringComparison.Ordinal)
                ? baseType + " unsigned"
                : baseType;

            return NormalizeProviderAlias(normalized);
        }

        private static string NormalizeProviderAlias(string normalized)
            => normalized switch
            {
                "integer unsigned" => "int unsigned",
                "character varying" or "varying character" => "varchar",
                "national character varying" => "nvarchar",
                "character" => "char",
                "double precision" => "double",
                "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                "time without time zone" or "time with time zone" => "time",
                _ => normalized
            };

    }
}
