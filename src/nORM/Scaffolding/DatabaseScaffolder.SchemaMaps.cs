#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static IReadOnlyDictionary<string, string> BuildEntityNameMap(IReadOnlyList<ScaffoldTable> tables, bool useDatabaseNames)
            => ScaffoldEntityNameBuilder.BuildEntityNameMap(
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                useDatabaseNames);

        private static Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetNonNullableColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldColumnDiscovery.GetNonNullableColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetColumnPropertyNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
            => await ScaffoldColumnPropertyDiscovery.GetColumnPropertyNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                entityByTable,
                useDatabaseNames).ConfigureAwait(false);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildColumnPropertyNameMap(
            IReadOnlyDictionary<string, IReadOnlyList<string>> orderedColumns,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
            => ScaffoldColumnPropertyNameBuilder.BuildColumnPropertyNameMap(
                orderedColumns,
                entityByTable,
                useDatabaseNames);

        private static Dictionary<string, HashSet<string>> BuildMemberNameMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, string> entityByTable)
            => ScaffoldColumnPropertyNameBuilder.BuildMemberNameMap(columnPropertiesByTable, entityByTable);

        private static HashSet<string> CreateReservedContextMemberNameSet()
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var member in typeof(DbContext).GetMembers())
                names.Add(member.Name);
            foreach (var member in typeof(object).GetMembers())
                names.Add(member.Name);
            names.Add("ConfigureOptions");
            return names;
        }

        private static HashSet<string> GetOrCreateMemberNames(
            Dictionary<string, HashSet<string>> memberNamesByTable,
            string tableKey)
            => ScaffoldColumnPropertyNameBuilder.GetOrCreateMemberNames(memberNamesByTable, tableKey);

        private static HashSet<string> CreateReservedMemberNameSet()
            => ScaffoldColumnPropertyNameBuilder.CreateReservedMemberNameSet();

        private static Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldKeyDiscovery.GetPrimaryKeyColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static Task<IReadOnlyDictionary<string, string>> GetPrimaryKeyConstraintNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldColumnDiscovery.GetIdentityColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static string GetColumnPropertyName(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            string tableKey,
            string columnName)
        {
            if (columnPropertiesByTable.TryGetValue(tableKey, out var properties)
                && properties.TryGetValue(columnName, out var propertyName))
            {
                return propertyName;
            }

            return EscapeCSharpIdentifier(ToPascalCase(columnName));
        }
    }
}
