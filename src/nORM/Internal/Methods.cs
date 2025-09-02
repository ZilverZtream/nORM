using System;
using System.Data;
using System.Data.Common;
using System.Reflection;

#nullable enable

namespace nORM.Internal
{
    internal static class Methods
    {
        public static readonly MethodInfo GetValue = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue))!;
        public static readonly MethodInfo IsDbNull = typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull))!;
        public static readonly MethodInfo GetBoolean = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetBoolean))!;
        public static readonly MethodInfo GetByte = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetByte))!;
        public static readonly MethodInfo GetInt16 = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetInt16))!;
        public static readonly MethodInfo GetInt32 = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetInt32))!;
        public static readonly MethodInfo GetInt64 = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetInt64))!;
        public static readonly MethodInfo GetFloat = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetFloat))!;
        public static readonly MethodInfo GetDouble = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetDouble))!;
        public static readonly MethodInfo GetDecimal = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetDecimal))!;
        public static readonly MethodInfo GetDateTime = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetDateTime))!;
        public static readonly MethodInfo GetGuid = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetGuid))!;
        public static readonly MethodInfo GetString = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetString))!;
        public static readonly MethodInfo GetBytes = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue))!;
        public static readonly MethodInfo GetFieldValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldValue))!;

        internal static MethodInfo GetReaderMethod(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum) return GetValue;
            return Type.GetTypeCode(type) switch
            {
                TypeCode.Boolean => GetBoolean,
                TypeCode.Byte => GetByte,
                TypeCode.Int16 => GetInt16,
                TypeCode.Int32 => GetInt32,
                TypeCode.Int64 => GetInt64,
                TypeCode.Single => GetFloat,
                TypeCode.Double => GetDouble,
                TypeCode.Decimal => GetDecimal,
                TypeCode.DateTime => GetDateTime,
                TypeCode.String => GetString,
                _ when type == typeof(Guid) => GetGuid,
                _ when type == typeof(byte[]) => GetBytes,
                _ => GetValue
            };
        }
    }
}