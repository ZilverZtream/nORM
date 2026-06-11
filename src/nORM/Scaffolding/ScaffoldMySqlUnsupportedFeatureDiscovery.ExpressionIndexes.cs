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
    internal static partial class ScaffoldMySqlUnsupportedFeatureDiscovery
    {
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
