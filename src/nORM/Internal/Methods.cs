using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;

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
        public static readonly MethodInfo GetDecimal = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDecimal))!;
        public static readonly MethodInfo GetDateTime = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetDateTime))!;
        public static readonly MethodInfo GetGuid = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetGuid))!;
        public static readonly MethodInfo GetString = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetString))!;
        public static readonly MethodInfo GetBytes = typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue))!;
        public static readonly MethodInfo GetFieldValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldValue))!;
        public static readonly MethodInfo SetShadowValue = typeof(ShadowPropertyStore).GetMethod(nameof(ShadowPropertyStore.Set))!;

        // PERFORMANCE OPTIMIZATION 12: Aggressive inlining for hot path method resolution
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static MethodInfo GetReaderMethod(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

            // PERFORMANCE OPTIMIZATION 13: Fast path for most common types (int, string, long, bool)
            // Avoid TypeCode lookup for these frequent cases
            if (type == typeof(int)) return GetInt32;
            if (type == typeof(string)) return GetString;
            if (type == typeof(long)) return GetInt64;
            if (type == typeof(bool)) return GetBoolean;
            if (type == typeof(decimal)) return GetDecimal;
            if (type == typeof(DateTime)) return GetDateTime;
            if (type == typeof(double)) return GetDouble;
            if (type == typeof(Guid)) return GetGuid;

            if (type.IsEnum) return GetValue;

            return Type.GetTypeCode(type) switch
            {
                TypeCode.Byte => GetByte,
                TypeCode.Int16 => GetInt16,
                TypeCode.Single => GetFloat,
                _ when type == typeof(byte[]) => GetBytes,
                _ => GetValue
            };
        }
    }
}