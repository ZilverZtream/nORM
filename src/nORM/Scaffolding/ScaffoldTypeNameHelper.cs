#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static class ScaffoldTypeNameHelper
    {
        public static string GetTypeName(Type type, bool allowNull, bool useNullableReferenceTypes = true)
        {
            string name = type.IsArray && type != typeof(byte[])
                ? GetTypeName(type.GetElementType()!, allowNull: false, useNullableReferenceTypes: useNullableReferenceTypes) + "[]"
                : type == typeof(byte[]) ? "byte[]" : type switch
            {
                var t when t == typeof(int) => "int",
                var t when t == typeof(long) => "long",
                var t when t == typeof(short) => "short",
                var t when t == typeof(byte) => "byte",
                var t when t == typeof(sbyte) => "sbyte",
                var t when t == typeof(uint) => "uint",
                var t when t == typeof(ulong) => "ulong",
                var t when t == typeof(ushort) => "ushort",
                var t when t == typeof(bool) => "bool",
                var t when t == typeof(char) => "char",
                var t when t == typeof(string) => "string",
                var t when t == typeof(DateTime) => "DateTime",
                var t when t == typeof(DateOnly) => "DateOnly",
                var t when t == typeof(DateTimeOffset) => "DateTimeOffset",
                var t when t == typeof(TimeOnly) => "TimeOnly",
                var t when t == typeof(TimeSpan) => "TimeSpan",
                var t when t == typeof(decimal) => "decimal",
                var t when t == typeof(double) => "double",
                var t when t == typeof(float) => "float",
                var t when t == typeof(Guid) => "Guid",
                _ => type.FullName ?? type.Name
            };

            if (allowNull && (type.IsValueType || useNullableReferenceTypes))
                name += "?";

            return name;
        }
    }
}
