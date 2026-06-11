using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldUnsupportedFeatureDiscoveryReader;

namespace nORM.Scaffolding
{
    internal static class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables,
            HashSet<string> tableKeys)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            await AddCatalogFeaturesAsync(connection, features, tableKeys).ConfigureAwait(false);
            features.AddRange(await GetExpressionIndexFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
            return features;
        }

        private static Task AddCatalogFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => AddFeaturesAsync(connection, features, tableKeys, """
                SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind,
                    CASE
                        WHEN data_type IN ('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'enum', 'set')
                        THEN QUOTE(column_default)
                        WHEN data_type IN ('date', 'datetime', 'timestamp', 'time')
                             AND LOWER(column_default) NOT LIKE 'current_timestamp%'
                             AND LOWER(column_default) NOT LIKE 'current_time%'
                             AND LOWER(column_default) NOT LIKE 'localtime%'
                             AND LOWER(column_default) NOT LIKE 'localtimestamp%'
                             AND LOWER(column_default) NOT LIKE 'now(%'
                             AND LOWER(column_default) NOT LIKE 'utc_timestamp(%'
                             AND LOWER(column_default) NOT LIKE 'sysdate(%'
                             AND LOWER(column_default) NOT LIKE 'current_date%'
                             AND column_default NOT LIKE '%()'
                        THEN QUOTE(column_default)
                        ELSE column_default
                    END AS Detail
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND column_default IS NOT NULL
                UNION ALL
                SELECT NULL, table_name, column_name, 'Computed',
                    CONCAT(
                        generation_expression,
                        CASE
                            WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN ' STORED'
                            WHEN LOWER(COALESCE(extra, '')) LIKE '%virtual generated%' THEN ' VIRTUAL'
                            ELSE ''
                        END)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND generation_expression IS NOT NULL AND generation_expression <> ''
                UNION ALL
                SELECT NULL, event_object_table, trigger_name, 'Trigger',
                    CONCAT('MySQL trigger; timing=', action_timing, '; event=', event_manipulation, '; orientation=', action_orientation)
                FROM information_schema.triggers
                WHERE trigger_schema = DATABASE()
                UNION ALL
                SELECT NULL, tc.table_name, tc.constraint_name, 'CheckConstraint', cc.check_clause
                FROM information_schema.table_constraints tc
                INNER JOIN information_schema.check_constraints cc
                    ON cc.constraint_schema = tc.constraint_schema
                   AND cc.constraint_name = tc.constraint_name
                WHERE tc.table_schema = DATABASE() AND tc.constraint_type = 'CHECK'
                UNION ALL
                SELECT NULL, c.table_name, c.column_name, 'Collation', c.collation_name
                FROM information_schema.columns c
                INNER JOIN information_schema.schemata s ON s.schema_name = c.table_schema
                WHERE c.table_schema = DATABASE()
                  AND c.collation_name IS NOT NULL
                  AND c.collation_name <> s.default_collation_name
                UNION ALL
                SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType',
                    CASE
                        WHEN data_type IN ('enum', 'set')
                             AND column_type IS NOT NULL
                             AND column_type <> ''
                        THEN column_type
                        ELSE data_type
                    END
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND data_type IN (
                      'json',
                      'geometry',
                      'point',
                      'linestring',
                      'polygon',
                      'multipoint',
                      'multilinestring',
                      'multipolygon',
                      'geometrycollection',
                      'enum',
                      'set',
                      'year'
                  )
                UNION ALL
                SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType', column_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND LOWER(COALESCE(column_type, '')) LIKE '%unsigned%'
                  AND data_type IN ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint', 'decimal', 'numeric')
                UNION ALL
                SELECT NULL, table_name, column_name, 'PrecisionScale',
                    CASE
                        WHEN numeric_scale IS NULL THEN CONCAT(data_type, '(', numeric_precision, ')')
                        ELSE CONCAT(data_type, '(', numeric_precision, ',', numeric_scale, ')')
                    END
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND data_type IN ('decimal', 'numeric')
                  AND numeric_precision IS NOT NULL
                UNION ALL
                SELECT DISTINCT NULL, table_name, index_name, 'DescendingIndex', 'MySQL descending index key'
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND index_name <> 'PRIMARY'
                  AND collation = 'D'
                UNION ALL
                SELECT NULL, s.table_name, s.index_name, 'PrefixIndex',
                    CONCAT(
                        'MySQL prefix index; prefixColumns=',
                        GROUP_CONCAT(
                            CONCAT(
                                s.column_name,
                                ':',
                                s.sub_part,
                                '/',
                                COALESCE(CAST(c.character_maximum_length AS CHAR), '')
                            )
                            ORDER BY s.seq_in_index
                            SEPARATOR ','
                        )
                    )
                FROM information_schema.statistics s
                INNER JOIN information_schema.columns c
                    ON c.table_schema = s.table_schema
                   AND c.table_name = s.table_name
                   AND c.column_name = s.column_name
                WHERE s.table_schema = DATABASE()
                  AND s.index_name <> 'PRIMARY'
                  AND s.sub_part IS NOT NULL
                  AND (
                      c.character_maximum_length IS NULL
                      OR s.sub_part < c.character_maximum_length
                  )
                GROUP BY s.table_name, s.index_name
                UNION ALL
                SELECT DISTINCT NULL, table_name, index_name, 'ProviderSpecificIndex',
                    CONCAT('MySQL provider-specific index; indexType=', index_type)
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND index_name <> 'PRIMARY'
                  AND UPPER(COALESCE(NULLIF(index_type, ''), 'BTREE')) <> 'BTREE'
                """);

        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeatureInfo>> GetExpressionIndexFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var features = new List<ScaffoldUnsupportedFeatureInfo>();
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SHOW INDEX FROM {provider.Escape(table.Name)}";
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                if (!ReaderHasColumn(reader, "Expression"))
                    continue;

                var byIndex = new Dictionary<string, (bool IsUnique, List<(int Ordinal, string KeySql)> Parts, bool HasExpression)>(StringComparer.OrdinalIgnoreCase);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var expression = NullIfWhiteSpace(Convert.ToString(reader["Expression"]));
                    var indexName = Convert.ToString(reader["Key_name"]);
                    if (string.IsNullOrWhiteSpace(indexName)
                        || string.Equals(indexName, "PRIMARY", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var columnName = NullIfWhiteSpace(Convert.ToString(reader["Column_name"]));
                    if (expression is null && columnName is null)
                        continue;

                    var isUnique = ReaderHasColumn(reader, "Non_unique")
                        && Convert.ToInt32(reader["Non_unique"], CultureInfo.InvariantCulture) == 0;
                    var ordinal = ReaderHasColumn(reader, "Seq_in_index")
                        ? Convert.ToInt32(reader["Seq_in_index"], CultureInfo.InvariantCulture)
                        : int.MaxValue;
                    var descending = ReaderHasColumn(reader, "Collation")
                        && string.Equals(Convert.ToString(reader["Collation"]), "D", StringComparison.OrdinalIgnoreCase);
                    var keySql = expression is not null
                        ? "(" + expression.Trim() + ")"
                        : provider.Escape(columnName!);
                    if (descending)
                        keySql += " DESC";

                    if (!byIndex.TryGetValue(indexName, out var entry))
                        entry = (isUnique, new List<(int Ordinal, string KeySql)>(), false);

                    entry.IsUnique = entry.IsUnique || isUnique;
                    entry.HasExpression = entry.HasExpression || expression is not null;
                    entry.Parts.Add((ordinal, keySql));
                    byIndex[indexName] = entry;
                }

                foreach (var (indexName, entry) in byIndex)
                {
                    if (!entry.HasExpression)
                        continue;

                    var expressionSql = string.Join(", ", entry.Parts.OrderBy(static part => part.Ordinal).Select(static part => part.KeySql));
                    features.Add(new ScaffoldUnsupportedFeatureInfo(
                        TableKey(table.Schema, table.Name),
                        "ExpressionIndex",
                        indexName,
                        "MySQL expression index; expression=" + expressionSql + "; isUnique=" + (entry.IsUnique ? "true" : "false")));
                }
            }

            return features;
        }

    }
}
