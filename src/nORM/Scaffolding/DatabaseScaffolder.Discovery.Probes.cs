#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static string BuildSchemaProbeSql(
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlySet<string>? postgresTextCastColumns = null)
            => ScaffoldEntitySourceBuilder.BuildSchemaProbeSql(
                provider,
                schemaName,
                tableName,
                columnPropertyNames,
                providerSpecificColumnTypes,
                postgresTextCastColumns);

        private static Task<IReadOnlySet<string>> GetPostgresUserDefinedColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName)
            => ScaffoldEntitySourceBuilder.GetPostgresUserDefinedColumnNamesAsync(connection, provider, schemaName, tableName);

        private static bool RequiresPostgresSchemaProbeCast(string detail)
            => ScaffoldProviderSpecificTypeClassifier.RequiresPostgresSchemaProbeCast(detail);

        private static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out castType);

        private static string GetPostgresDomainProbeCastType(string detail)
            => ScaffoldProviderSpecificTypeClassifier.GetPostgresDomainProbeCastType(detail);

        private static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
            => ScaffoldProviderSpecificTypeClassifier.TryGetPostgresDomainBaseTypeText(detail, out typeText);

        private static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        private static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizeSafePostgresDomainProbeCastType(typeText, out castType);

        private static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        private static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        private static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);
    }
}
