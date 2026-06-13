#nullable enable
using System;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
        private static bool TryNormalizeSqliteScaffoldClrType(
            DatabaseProvider provider,
            Type clrType,
            bool allowNull,
            bool isKey,
            bool isAuto,
            string? declaredType,
            out Type normalizedClrType)
        {
            normalizedClrType = clrType;
            if (!ScaffoldProviderKind.IsSqlite(provider))
                return false;

            if (IsSqliteUuidDeclaredType(declaredType))
            {
                normalizedClrType = typeof(Guid);
                return true;
            }

            if (isKey && isAuto && !allowNull && clrType == typeof(int))
            {
                normalizedClrType = typeof(long);
                return true;
            }

            return false;
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }
    }
}
