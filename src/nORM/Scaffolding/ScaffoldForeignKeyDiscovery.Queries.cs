#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> QueryForeignKeysAsync(DbConnection connection, string sql)
        {
            var foreignKeys = new List<ScaffoldForeignKeyInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var dependentTable = Convert.ToString(reader["DependentTable"]);
                var dependentColumn = Convert.ToString(reader["DependentColumn"]);
                var principalTable = Convert.ToString(reader["PrincipalTable"]);
                var principalColumn = Convert.ToString(reader["PrincipalColumn"]);
                if (string.IsNullOrWhiteSpace(dependentTable)
                    || string.IsNullOrWhiteSpace(dependentColumn)
                    || string.IsNullOrWhiteSpace(principalTable)
                    || string.IsNullOrWhiteSpace(principalColumn))
                {
                    continue;
                }

                foreignKeys.Add(new ScaffoldForeignKeyInfo(
                    NullIfWhiteSpace(Convert.ToString(reader["DependentSchema"])),
                    dependentTable,
                    dependentColumn,
                    NullIfWhiteSpace(Convert.ToString(reader["PrincipalSchema"])),
                    principalTable,
                    principalColumn,
                    Convert.ToString(reader["ConstraintName"]) ?? string.Empty,
                    Convert.ToInt32(reader["ColumnCount"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "OnDelete") ? NormalizeReferentialAction(Convert.ToString(reader["OnDelete"])) : "NO ACTION",
                    ReaderHasColumn(reader, "OnUpdate") ? NormalizeReferentialAction(Convert.ToString(reader["OnUpdate"])) : "NO ACTION",
                    ReaderHasColumn(reader, "IsSyntheticConstraintName")
                        && Convert.ToBoolean(reader["IsSyntheticConstraintName"], CultureInfo.InvariantCulture)));
            }

            return foreignKeys;
        }

        private static string NormalizeReferentialAction(string? action)
        {
            if (string.IsNullOrWhiteSpace(action))
                return "NO ACTION";

            return action.Replace('_', ' ').Trim().ToUpperInvariant();
        }
    }
}
