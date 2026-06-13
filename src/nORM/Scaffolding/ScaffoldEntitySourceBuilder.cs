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
            var schema = await ReadSchemaTableAsync(entity).ConfigureAwait(false);
            var columns = BuildEntityColumns(entity, schema);

            return ScaffoldEntityWriter.Write(BuildEntityInfo(entity, columns));
        }

        private static async Task<DataTable> ReadSchemaTableAsync(ScaffoldEntitySourceInfo entity)
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
            return reader.GetSchemaTable()!;
        }

        private static ScaffoldEntityInfo BuildEntityInfo(
            ScaffoldEntitySourceInfo entity,
            IReadOnlyList<ScaffoldEntityColumnInfo> columns)
            => new(
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
                entity.ManyToManyCollections ?? Array.Empty<ScaffoldEntityManyToManyNavigationInfo>());
    }
}
