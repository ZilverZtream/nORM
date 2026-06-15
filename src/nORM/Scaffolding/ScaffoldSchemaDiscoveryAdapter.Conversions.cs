#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSchemaDiscoveryAdapter
    {
        public static IReadOnlyList<ScaffoldSkippedObjectInfo> ConvertSkippedObjectInfos(
            IReadOnlyList<ScaffoldSkippedObject> objects)
        {
            var converted = new ScaffoldSkippedObjectInfo[objects.Count];
            for (var i = 0; i < objects.Count; i++)
            {
                var obj = objects[i];
                converted[i] = new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment)
                {
                    Metadata = BuildSkippedObjectMetadata(obj)
                };
            }

            return converted;
        }

        public static ScaffoldTableInfo[] ToScaffoldTableInfos(IEnumerable<ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        public static ScaffoldTable ToScaffoldTable(ScaffoldTableInfo table)
            => new(table.Name, table.Schema);

        public static ScaffoldSkippedObjectInfo[] ToSkippedObjectInfos(
            IEnumerable<ScaffoldSkippedObject> objects)
            => objects.Select(ToSkippedObjectInfo).ToArray();

        public static ScaffoldSkippedObjectInfo ToSkippedObjectInfo(ScaffoldSkippedObject obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static ScaffoldSkippedObject ToScaffoldSkippedObject(ScaffoldSkippedObjectInfo obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static IReadOnlyList<ScaffoldIndex> ConvertIndexes(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var converted = new ScaffoldIndex[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new ScaffoldIndex(
                    index.TableKey,
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql,
                    index.IsSyntheticName);
            }

            return converted;
        }

        public static IReadOnlyList<ScaffoldForeignKey> ConvertForeignKeys(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var converted = new ScaffoldForeignKey[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new ScaffoldForeignKey(
                    foreignKey.DependentSchema,
                    foreignKey.DependentTable,
                    foreignKey.DependentColumn,
                    foreignKey.PrincipalSchema,
                    foreignKey.PrincipalTable,
                    foreignKey.PrincipalColumn,
                    foreignKey.ConstraintName,
                    foreignKey.ColumnCount,
                    foreignKey.OnDelete,
                    foreignKey.OnUpdate,
                    foreignKey.IsSyntheticConstraintName);
            }

            return converted;
        }

        public static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(
            ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));
    }
}
