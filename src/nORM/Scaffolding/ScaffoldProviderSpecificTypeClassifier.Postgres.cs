#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldProviderSpecificTypeClassifier
    {
        public static bool RequiresPostgresSchemaProbeCast(string detail)
            => ScaffoldPostgresTypeClassifier.RequiresPostgresSchemaProbeCast(detail);

        public static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
            => ScaffoldPostgresTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out castType);

        public static string GetPostgresDomainProbeCastType(string detail)
            => ScaffoldPostgresTypeClassifier.GetPostgresDomainProbeCastType(detail);

        public static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
            => ScaffoldPostgresTypeClassifier.TryGetPostgresDomainBaseTypeText(detail, out typeText);

        public static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldPostgresTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        public static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
            => ScaffoldPostgresTypeClassifier.TryNormalizeSafePostgresDomainProbeCastType(typeText, out castType);

        public static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldPostgresTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        public static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

        public static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
            => ScaffoldPostgresTypeClassifier.IsSafePostgresUserDefinedScalarColumnType(normalized);

        public static bool IsSafePostgresDomainColumnType(string? detail)
            => ScaffoldPostgresTypeClassifier.IsSafePostgresDomainColumnType(detail);

        public static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresDomainEnumValues(detail, out values);

        public static bool TryParsePostgresEnumValues(string? detail, out string[] values)
            => ScaffoldPostgresTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        public static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayType(detail, out arrayType);

        public static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
            => ScaffoldPostgresTypeClassifier.TryMapPostgresArrayCastType(normalized, out arrayType);
    }
}
