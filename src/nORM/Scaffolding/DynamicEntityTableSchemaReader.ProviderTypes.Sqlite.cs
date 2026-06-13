#nullable enable
using System;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityTableSchemaReader
    {
        private static bool TryNormalizeSqliteScaffoldClrType(
            DbConnection connection,
            Type clrType,
            bool allowNull,
            bool isKey,
            bool isAuto,
            string? declaredType,
            out Type normalizedClrType)
        {
            normalizedClrType = clrType;
            if (!DynamicEntityConnectionKind.IsSqlite(connection))
                return false;

            if (IsSqliteUuidDeclaredType(declaredType))
            {
                normalizedClrType = typeof(Guid);
                return true;
            }

            if (isKey && isAuto && !allowNull && clrType == typeof(int))
            {
                // SQLite INTEGER PRIMARY KEY aliases the 64-bit rowid even when
                // provider schema metadata reports Int32 for small test values.
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
