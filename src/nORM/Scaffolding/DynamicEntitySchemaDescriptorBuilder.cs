#nullable enable
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class DynamicEntitySchemaDescriptorBuilder
    {
        public static string BuildDescriptor(
            string? schemaName,
            string tableName,
            IReadOnlyList<DynamicEntityTypeGenerator.ColumnInfo> columns,
            bool isReadOnlyEntity)
        {
            var sb = new StringBuilder();
            AppendDescriptorPart(sb, schemaName ?? string.Empty);
            AppendDescriptorPart(sb, tableName);
            AppendDescriptorPart(sb, isReadOnlyEntity ? "RO" : "RW");
            foreach (var column in columns)
            {
                AppendDescriptorPart(sb, column.ColumnName);
                AppendDescriptorPart(sb, column.PropertyType.FullName ?? string.Empty);
                AppendDescriptorPart(sb, column.IsKey ? "PK" : "C");
                AppendDescriptorPart(sb, column.IsKey ? column.KeyOrdinal.ToString(CultureInfo.InvariantCulture) : "-");
                AppendDescriptorPart(sb, column.AllowsNull ? "N" : "NN");
                AppendDescriptorPart(sb, column.IsAuto ? "AI" : "NA");
                AppendDescriptorPart(sb, column.IsComputed ? "CMP" : "NCMP");
                AppendDescriptorPart(sb, column.ComputedColumn?.Sql ?? "-");
                AppendDescriptorPart(sb, column.ComputedColumn is { } computedColumn ? computedColumn.Stored ? "STORED" : "VIRTUAL" : "-");
                AppendDescriptorPart(sb, column.IsRowVersion ? "RV" : "NRV");
                AppendDescriptorPart(sb, column.MaxLength?.ToString(CultureInfo.InvariantCulture) ?? "-");
                AppendDescriptorPart(sb, column.IsUnicode.HasValue ? column.IsUnicode.Value ? "U" : "NU" : "-");
                AppendDescriptorPart(sb, column.IsFixedLength ? "FIX" : "VAR");
                AppendDescriptorPart(
                    sb,
                    column.DecimalPrecision is { } decimalPrecision
                        ? FormatDecimalTypeName(decimalPrecision)
                        : "-");
            }

            return sb.ToString();
        }

        private static void AppendDescriptorPart(StringBuilder builder, string value)
        {
            builder.Append(value.Length.ToString(CultureInfo.InvariantCulture));
            builder.Append(':');
            builder.Append(value);
            builder.Append(';');
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
