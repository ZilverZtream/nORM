#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSchemaDiscoveryAdapter
    {
        public static IReadOnlyList<ScaffoldSkippedObjectInfo> ConvertSkippedObjectInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> objects)
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

        public static ScaffoldTableInfo[] ToScaffoldTableInfos(IEnumerable<DatabaseScaffolder.ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        public static DatabaseScaffolder.ScaffoldTable ToScaffoldTable(ScaffoldTableInfo table)
            => new(table.Name, table.Schema);

        public static ScaffoldSkippedObjectInfo[] ToSkippedObjectInfos(
            IEnumerable<DatabaseScaffolder.ScaffoldSkippedObject> objects)
            => objects.Select(ToSkippedObjectInfo).ToArray();

        public static ScaffoldSkippedObjectInfo ToSkippedObjectInfo(DatabaseScaffolder.ScaffoldSkippedObject obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static DatabaseScaffolder.ScaffoldSkippedObject ToScaffoldSkippedObject(ScaffoldSkippedObjectInfo obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> ConvertIndexes(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var converted = new DatabaseScaffolder.ScaffoldIndex[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new DatabaseScaffolder.ScaffoldIndex(
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

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> ConvertForeignKeys(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var converted = new DatabaseScaffolder.ScaffoldForeignKey[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new DatabaseScaffolder.ScaffoldForeignKey(
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
            DatabaseScaffolder.ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));
    }
}
