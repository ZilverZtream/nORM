#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Reflection.Emit;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTypeBuilder
    {
        private static void ApplyPropertyAttributes(
            PropertyBuilder propertyBuilder,
            DynamicEntityTypeGenerator.ColumnInfo column,
            Type propertyType)
        {
            ApplyColumnAttribute(propertyBuilder, column);

            if (column.IsKey)
                ApplyKeyAttribute(propertyBuilder);

            if (column.IsRowVersion)
                ApplyTimestampAttribute(propertyBuilder);

            if (column.IsAuto || column.IsComputed || column.IsRowVersion)
                ApplyDatabaseGeneratedAttribute(propertyBuilder, column);

            if (column.MaxLength.HasValue)
                ApplyMaxLengthAttribute(propertyBuilder, column.MaxLength.Value);

            if (!propertyType.IsValueType && !column.AllowsNull)
                ApplyRequiredAttribute(propertyBuilder);
        }

        private static void ApplyColumnAttribute(PropertyBuilder propertyBuilder, DynamicEntityTypeGenerator.ColumnInfo column)
        {
            var columnAttrCtor = typeof(ColumnAttribute).GetConstructor(new[] { typeof(string) })!;
            var columnAttr = column.DecimalPrecision is { } decimalPrecision
                ? new CustomAttributeBuilder(
                    columnAttrCtor,
                    new object[] { column.ColumnName },
                    new[] { typeof(ColumnAttribute).GetProperty(nameof(ColumnAttribute.TypeName))! },
                    new object[]
                    {
                        FormatDecimalTypeName(decimalPrecision)
                    })
                : new CustomAttributeBuilder(columnAttrCtor, new object[] { column.ColumnName });
            propertyBuilder.SetCustomAttribute(columnAttr);
        }

        private static void ApplyKeyAttribute(PropertyBuilder propertyBuilder)
        {
            var keyAttrCtor = typeof(KeyAttribute).GetConstructor(Type.EmptyTypes)!;
            var keyAttr = new CustomAttributeBuilder(keyAttrCtor, Array.Empty<object>());
            propertyBuilder.SetCustomAttribute(keyAttr);
        }

        private static void ApplyTimestampAttribute(PropertyBuilder propertyBuilder)
        {
            var timestampAttrCtor = typeof(TimestampAttribute).GetConstructor(Type.EmptyTypes)!;
            var timestampAttr = new CustomAttributeBuilder(timestampAttrCtor, Array.Empty<object>());
            propertyBuilder.SetCustomAttribute(timestampAttr);
        }

        private static void ApplyDatabaseGeneratedAttribute(PropertyBuilder propertyBuilder, DynamicEntityTypeGenerator.ColumnInfo column)
        {
            var dbGenAttrCtor = typeof(DatabaseGeneratedAttribute).GetConstructor(new[] { typeof(DatabaseGeneratedOption) })!;
            var option = column.IsAuto ? DatabaseGeneratedOption.Identity : DatabaseGeneratedOption.Computed;
            var dbGenAttr = new CustomAttributeBuilder(dbGenAttrCtor, new object[] { option });
            propertyBuilder.SetCustomAttribute(dbGenAttr);
        }

        private static void ApplyMaxLengthAttribute(PropertyBuilder propertyBuilder, int maxLength)
        {
            var maxLenAttrCtor = typeof(MaxLengthAttribute).GetConstructor(new[] { typeof(int) })!;
            var maxLenAttr = new CustomAttributeBuilder(maxLenAttrCtor, new object[] { maxLength });
            propertyBuilder.SetCustomAttribute(maxLenAttr);
        }

        private static void ApplyRequiredAttribute(PropertyBuilder propertyBuilder)
        {
            var requiredAttrCtor = typeof(RequiredAttribute).GetConstructor(Type.EmptyTypes)!;
            var requiredAttr = new CustomAttributeBuilder(requiredAttrCtor, Array.Empty<object>());
            propertyBuilder.SetCustomAttribute(requiredAttr);
        }

        private static string FormatDecimalTypeName(DynamicEntityTypeGenerator.ScaffoldDecimalPrecision decimalPrecision)
        {
            var precision = decimalPrecision.Precision.ToString(CultureInfo.InvariantCulture);
            return decimalPrecision.Scale.HasValue
                ? $"decimal({precision},{decimalPrecision.Scale.Value.ToString(CultureInfo.InvariantCulture)})"
                : $"decimal({precision})";
        }
    }
}
