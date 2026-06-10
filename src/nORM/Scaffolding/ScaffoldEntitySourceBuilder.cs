#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldEntitySourceBuilder
    {
        private static readonly IReadOnlySet<string> EmptyStringSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public static async Task<string> BuildAsync(ScaffoldEntitySourceInfo entity)
        {
            var postgresTextCastColumns = await GetPostgresUserDefinedColumnNamesAsync(
                entity.Connection,
                entity.Provider,
                entity.SchemaName,
                entity.TableName).ConfigureAwait(false);

            await using var cmd = entity.Connection.CreateCommand();
            cmd.CommandText = BuildSchemaProbeSql(
                entity.Provider,
                entity.SchemaName,
                entity.TableName,
                entity.ColumnPropertyNames,
                entity.ProviderSpecificColumnTypes,
                postgresTextCastColumns);
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
            var schema = reader.GetSchemaTable()!;
            var columns = new List<ScaffoldEntityColumnInfo>();
            foreach (DataRow row in schema.Rows)
            {
                var colName = row["ColumnName"]!.ToString()!;
                var propName = entity.ColumnPropertyNames is not null && entity.ColumnPropertyNames.TryGetValue(colName, out var mappedProperty)
                    ? mappedProperty
                    : ScaffoldNameHelper.EscapeCSharpIdentifier(ScaffoldNameHelper.ToPascalCase(colName));
                var allowNull = row["AllowDBNull"] is bool b && b;
                if (entity.NonNullableColumns is not null)
                    allowNull = !entity.NonNullableColumns.Contains(colName);

                var isKey = row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                var isAuto = (row.Table.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai)
                    || entity.IdentityColumns?.Contains(colName) == true;
                var isComputed = entity.ComputedColumns?.Contains(colName) == true;
                var isRowVersion = entity.RowVersionColumns?.Contains(colName) == true;
                var effectiveAllowNull = allowNull && !isKey && !isRowVersion;
                string? declaredType = null;
                entity.SqliteDeclaredTypes?.TryGetValue(colName, out declaredType);
                string? providerSpecificType = null;
                entity.ProviderSpecificColumnTypes?.TryGetValue(colName, out providerSpecificType);
                var rawClrType = row["DataType"] is Type type ? type : typeof(object);
                var clrType = NormalizeScaffoldClrType(entity.Provider, rawClrType, effectiveAllowNull, isKey, isAuto, declaredType, providerSpecificType);

                var columnFacet = entity.ColumnFacets is not null && entity.ColumnFacets.TryGetValue(colName, out var resolvedFacet)
                    ? resolvedFacet
                    : default;
                var maxLength = GetScaffoldMaxLength(clrType, row)
                    ?? ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLength(providerSpecificType);
                if (!maxLength.HasValue && columnFacet.MaxLength.HasValue)
                    maxLength = columnFacet.MaxLength;

                string? columnComment = null;
                entity.Comments?.ColumnComments.TryGetValue(colName, out columnComment);
                var decimalPrecisionInfo = entity.DecimalPrecisions is not null
                                           && entity.DecimalPrecisions.TryGetValue(colName, out var decimalPrecision)
                    ? new ScaffoldEntityDecimalPrecisionInfo(decimalPrecision.Precision, decimalPrecision.Scale)
                    : (ScaffoldEntityDecimalPrecisionInfo?)null;
                columns.Add(new ScaffoldEntityColumnInfo(
                    colName,
                    propName,
                    clrType,
                    effectiveAllowNull,
                    isKey,
                    isAuto,
                    isComputed,
                    isRowVersion,
                    maxLength,
                    decimalPrecisionInfo,
                    columnComment,
                    GetColumnIndexes(entity.Indexes, colName)));
            }

            return ScaffoldEntityWriter.Write(new ScaffoldEntityInfo(
                entity.NamespaceName,
                entity.EntityName,
                entity.TableName,
                entity.SchemaName,
                entity.Comments?.TableComment,
                entity.Indexes?.Count > 0,
                entity.IsReadOnlyEntity,
                entity.UseNullableReferenceTypes,
                columns,
                entity.References ?? Array.Empty<ScaffoldEntityReferenceInfo>(),
                entity.Collections ?? Array.Empty<ScaffoldEntityCollectionInfo>(),
                entity.ManyToManyCollections ?? Array.Empty<ScaffoldEntityManyToManyNavigationInfo>()));
        }

        public static string BuildSchemaProbeSql(
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlySet<string>? postgresTextCastColumns = null)
        {
            var qualified = EscapeQualified(provider, schemaName, tableName);
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase)
                || ((providerSpecificColumnTypes is null
                     || providerSpecificColumnTypes.Count == 0
                     || !providerSpecificColumnTypes.Values.Any(ScaffoldProviderSpecificTypeClassifier.RequiresPostgresSchemaProbeCast))
                    && (postgresTextCastColumns is null || postgresTextCastColumns.Count == 0))
                || columnPropertyNames is null
                || columnPropertyNames.Count == 0)
            {
                return $"SELECT * FROM {qualified} WHERE 1=0";
            }

            var columns = columnPropertyNames.Keys.Select(column =>
            {
                var escaped = provider.Escape(column);
                return providerSpecificColumnTypes is not null
                       && providerSpecificColumnTypes.TryGetValue(column, out var detail)
                       && ScaffoldProviderSpecificTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out var castType)
                    ? $"{escaped}::{castType} AS {escaped}"
                    : postgresTextCastColumns is not null && postgresTextCastColumns.Contains(column)
                        ? $"{escaped}::text AS {escaped}"
                        : escaped;
            });

            return $"SELECT {string.Join(", ", columns)} FROM {qualified} WHERE 1=0";
        }

        public static async Task<IReadOnlySet<string>> GetPostgresUserDefinedColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName)
        {
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return EmptyStringSet;

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = @tableName
                  AND (@schemaName IS NULL OR table_schema = @schemaName)
                  AND domain_name IS NULL
                  AND data_type = 'USER-DEFINED'
                """;
            AddParameter(cmd, "@tableName", tableName);
            AddParameter(cmd, "@schemaName", string.IsNullOrWhiteSpace(schemaName) ? DBNull.Value : schemaName);
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var columnName = Convert.ToString(reader["column_name"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                    result.Add(columnName);
            }

            return result.Count == 0 ? EmptyStringSet : result;
        }

        public static int? GetScaffoldMaxLength(Type clrType, DataRow row)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            if (!row.Table.Columns.Contains("ColumnSize") || row["ColumnSize"] == DBNull.Value)
                return null;

            return int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0 && !IsUnboundedScaffoldMaxLength(size)
                ? size
                : null;
        }

        public static bool IsUnboundedScaffoldMaxLength(int size)
            => size == int.MaxValue
               || size == 1073741823;

        public static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null, string? providerSpecificColumnType = null)
        {
            if (provider is SqliteProvider && IsSqliteUuidDeclaredType(declaredType))
                return typeof(Guid);

            if (provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase)
                && ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayType(providerSpecificColumnType, out var arrayType))
            {
                return arrayType;
            }

            if (provider.GetType().Name.Contains("SqlServer", StringComparison.OrdinalIgnoreCase)
                && ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrType(providerSpecificColumnType, out var aliasBaseType))
            {
                return aliasBaseType;
            }

            if (provider.GetType().Name.Contains("MySql", StringComparison.OrdinalIgnoreCase)
                && ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(providerSpecificColumnType, out var unsignedType))
            {
                return unsignedType;
            }

            if (provider is SqliteProvider
                && isKey
                && isAuto
                && !allowNull
                && clrType == typeof(int))
            {
                return typeof(long);
            }

            return clrType;
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !DynamicEntityReadOnlyClassifier.IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && DynamicEntityReadOnlyClassifier.ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static IReadOnlyList<ScaffoldEntityIndexInfo> GetColumnIndexes(
            IReadOnlyList<ScaffoldEntityIndexSourceInfo>? indexes,
            string columnName)
            => (indexes ?? Array.Empty<ScaffoldEntityIndexSourceInfo>())
                .Where(index => string.Equals(index.ColumnName, columnName, StringComparison.Ordinal))
                .Select(index => new ScaffoldEntityIndexInfo(
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql))
                .ToArray();

        private static void AddParameter(DbCommand command, string name, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        private static string EscapeQualified(DatabaseProvider provider, string? schema, string table)
            => string.IsNullOrWhiteSpace(schema)
                ? IdentifierEscaping.EscapeSingle(provider, table)
                : $"{provider.Escape(schema!)}.{IdentifierEscaping.EscapeSingle(provider, table)}";
    }
}
