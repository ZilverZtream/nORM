#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldProviderSpecificTypeClassifier
    {
        public static bool IsSafeMySqlUnsignedDecimalType(string? detail)
            => ScaffoldMySqlTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail);

        public static bool TryParseMySqlEnumValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlEnumValues(detail, out values);

        public static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        public static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldMySqlTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        public static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldMySqlTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        public static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldMySqlTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);
    }
}
