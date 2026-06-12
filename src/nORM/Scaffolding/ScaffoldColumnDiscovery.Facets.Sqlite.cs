#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetSqliteDeclaredStringBinaryFacetsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                var columns = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var name = Convert.ToString(reader["name"]);
                    var declaredType = Convert.ToString(reader["type"]);
                    if (!string.IsNullOrWhiteSpace(name)
                        && ScaffoldSqliteDdlParser.TryParseDeclaredStringBinaryFacet(declaredType, out var facet))
                    {
                        columns[name!] = facet;
                    }
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
        }
    }
}
