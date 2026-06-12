#nullable enable
using System.Globalization;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityWriter
    {
        private static void AppendNullableDirective(StringBuilder sb, bool useNullableReferenceTypes)
            => sb.AppendLine(useNullableReferenceTypes ? "#nullable enable" : "#nullable disable");

        private static string FormatDecimalTypeName(ScaffoldEntityDecimalPrecisionInfo decimalPrecision)
        {
            var precision = decimalPrecision.Precision.ToString(CultureInfo.InvariantCulture);
            return decimalPrecision.Scale.HasValue
                ? $"decimal({precision},{decimalPrecision.Scale.Value.ToString(CultureInfo.InvariantCulture)})"
                : $"decimal({precision})";
        }
    }
}
