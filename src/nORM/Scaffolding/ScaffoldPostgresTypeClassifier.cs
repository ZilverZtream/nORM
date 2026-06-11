#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool RequiresPostgresSchemaProbeCast(string detail)
            => TryGetPostgresSchemaProbeCastType(detail, out _);

        public static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
        {
            if (detail.Contains("DOMAIN", StringComparison.OrdinalIgnoreCase))
            {
                castType = GetPostgresDomainProbeCastType(detail);
                return true;
            }

            if (TryParsePostgresEnumValues(detail, out _))
            {
                castType = "text";
                return true;
            }

            if (detail.StartsWith("USER-DEFINED", StringComparison.OrdinalIgnoreCase))
            {
                castType = TryNormalizeSafePostgresDomainProbeCastType(detail, out var safeCastType)
                    ? safeCastType
                    : "text";
                return true;
            }

            castType = string.Empty;
            return false;
        }
    }
}
