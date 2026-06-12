#nullable enable
using System;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityWriter
    {
        private static void AppendColumn(StringBuilder sb, ScaffoldEntityColumnInfo column, bool useNullableReferenceTypes)
        {
            var typeName = ScaffoldTypeNameHelper.GetTypeName(column.ClrType, column.EffectiveAllowNull, useNullableReferenceTypes);
            AppendColumnXmlDocumentation(sb, column.ColumnName, column.Comment);
            if (column.IsKey)
                sb.AppendLine("    [Key]");
            if (column.IsRowVersion)
                sb.AppendLine("    [Timestamp]");
            if (column.IsAutoIncrement)
                sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]");
            else if (column.IsComputed)
                sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]");
            if (column.MaxLength.HasValue)
                sb.AppendLine($"    [MaxLength({column.MaxLength.Value})]");
            if (!column.ClrType.IsValueType && !column.EffectiveAllowNull)
                sb.AppendLine("    [Required]");

            AppendIndexes(sb, column.Indexes);
            if (column.ClrType == typeof(decimal) && column.DecimalPrecision.HasValue)
            {
                sb.AppendLine($"    [Column(\"{EscapeStringLiteral(column.ColumnName)}\", TypeName = \"{FormatDecimalTypeName(column.DecimalPrecision.Value)}\")]");
            }
            else
            {
                sb.AppendLine($"    [Column(\"{EscapeStringLiteral(column.ColumnName)}\")]");
            }

            var initializer = column.ClrType == typeof(byte[]) && !column.EffectiveAllowNull
                ? " = Array.Empty<byte>();"
                : useNullableReferenceTypes && !column.ClrType.IsValueType && !column.EffectiveAllowNull ? " = default!;" : string.Empty;
            sb.AppendLine($"    public {typeName} {column.PropertyName} {{ get; set; }}{initializer}");
            sb.AppendLine();
        }

        private static void AppendColumnXmlDocumentation(StringBuilder sb, string columnName, string? comment)
        {
            if (string.IsNullOrWhiteSpace(comment))
            {
                AppendXmlSummary(sb, "    ", "Maps to column " + columnName);
                return;
            }

            AppendXmlSummary(sb, "    ", comment!);
            sb.AppendLine($"    /// <remarks>Maps to column {EscapeXmlDocumentation(columnName)}</remarks>");
        }
    }
}
