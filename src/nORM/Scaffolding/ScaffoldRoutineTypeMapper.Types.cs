#nullable enable
using System.Data;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
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
                "datetimeoffset" or "timestamptz" or "timetz" => "DateTimeOffset?",
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
                "datetimeoffset" or "timestamptz" or "timetz" => nameof(DbType.DateTimeOffset),
                "uniqueidentifier" or "uuid" => nameof(DbType.Guid),
                "sysname" => nameof(DbType.String),
                "bpchar" => nameof(DbType.String),
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "citext" or "xml" or "json" or "jsonb" or "enum" or "set" => nameof(DbType.String),
                "binary" or "varbinary" or "image" or "bytea" or "blob" or "longblob" or "mediumblob" or "tinyblob" => nameof(DbType.Binary),
                _ => nameof(DbType.Object)
            };
        }
    }
}
