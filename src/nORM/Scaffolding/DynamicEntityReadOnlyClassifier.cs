#nullable enable
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        public static bool IsReadOnlyDynamicObject(DbConnection connection, string? schemaName, string tableName)
            => IsDynamicQueryObject(connection, schemaName, tableName)
               || IsProviderOwnedSynonym(connection, schemaName, tableName)
               || IsProviderNativeTemporalTable(connection, schemaName, tableName)
               || HasProviderOwnedTriggers(connection, schemaName, tableName)
               || HasUnmodeledDefaults(connection, schemaName, tableName)
               || HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

        public static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityWriteBlockingClassifier.HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

        public static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityWriteBlockingClassifier.HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);

        public static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
            => DynamicEntityWriteBlockingClassifier.IsWriteBlockingSqliteDeclaredType(declaredType);

        public static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => DynamicEntityWriteBlockingClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalizedDeclaredType);

        public static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => DynamicEntityWriteBlockingClassifier.ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, token);

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
            => DynamicEntityWriteBlockingClassifier.IsSqliteUuidDeclaredType(declaredType);
    }
}
