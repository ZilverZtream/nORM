#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldColumnPropertyNameBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildColumnPropertyNameMap(
            IReadOnlyDictionary<string, IReadOnlyList<string>> orderedColumns,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var (tableKey, columns) in orderedColumns)
            {
                var existingNames = CreateReservedMemberNameSet();
                if (entityByTable.TryGetValue(tableKey, out var entityName))
                    existingNames.Add(entityName);
                result[tableKey] = BuildColumnPropertyNames(columns, existingNames, useDatabaseNames);
            }

            return result;
        }

        public static IReadOnlyDictionary<string, string> BuildColumnPropertyNames(
            IEnumerable<string> columns,
            HashSet<string> existingNames,
            bool useDatabaseNames)
        {
            var properties = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var columnName in columns)
            {
                var baseName = ScaffoldNameHelper.ToScaffoldClrName(columnName, useDatabaseNames);
                properties[columnName] = ScaffoldNameHelper.MakeUnique(baseName, existingNames);
            }

            return properties;
        }

        public static Dictionary<string, HashSet<string>> BuildMemberNameMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, string> entityByTable)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var (tableKey, columns) in columnPropertiesByTable)
            {
                var names = CreateReservedMemberNameSet();
                if (entityByTable.TryGetValue(tableKey, out var entityName))
                    names.Add(entityName);
                foreach (var column in columns.Values)
                    names.Add(column);
                result[tableKey] = names;
            }

            return result;
        }

        public static HashSet<string> GetOrCreateMemberNames(
            Dictionary<string, HashSet<string>> memberNamesByTable,
            string tableKey)
        {
            if (!memberNamesByTable.TryGetValue(tableKey, out var names))
            {
                names = CreateReservedMemberNameSet();
                memberNamesByTable[tableKey] = names;
            }

            return names;
        }

        public static HashSet<string> CreateReservedMemberNameSet()
            => typeof(object)
                .GetMembers(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                .Select(member => member.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }
}
