using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static bool IsReadOnlyDynamicObject(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsReadOnlyDynamicObject(connection, schemaName, tableName);

        private static bool IsDynamicQueryObject(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsDynamicQueryObject(connection, schemaName, tableName);

        private static bool IsProviderOwnedSynonym(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsProviderOwnedSynonym(connection, schemaName, tableName);

        private static bool IsProviderNativeTemporalTable(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.IsProviderNativeTemporalTable(connection, schemaName, tableName);

        private static bool HasProviderOwnedTriggers(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasProviderOwnedTriggers(connection, schemaName, tableName);

        private static bool HasUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasUnmodeledDefaults(connection, schemaName, tableName);

        private static bool HasUnmodeledDefaultSql(string? raw)
            => !string.IsNullOrWhiteSpace(raw)
               && !TryNormalizeDynamicDefaultSql(raw, out _);

        private static bool TryNormalizeDynamicDefaultSql(string? raw, out string defaultValueSql)
            => DynamicEntityReadOnlyClassifier.TryNormalizeDynamicDefaultSql(raw, out defaultValueSql);

        private static bool HasWriteBlockingProviderSpecificColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasWriteBlockingProviderSpecificColumns(connection, schemaName, tableName);

        private static bool HasWriteBlockingMySqlSetColumns(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityReadOnlyClassifier.HasWriteBlockingMySqlSetColumns(connection, schemaName, tableName);

        private static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
            => DynamicEntityReadOnlyClassifier.IsWriteBlockingSqliteDeclaredType(declaredType);

        private static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalizedDeclaredType);

        private static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, token);
    }
}
