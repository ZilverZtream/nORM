#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntitySourceBuilder
    {
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
    }
}
