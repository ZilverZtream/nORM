using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    public partial class DynamicEntityTypeGenerator
    {
        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> QueryComputedColumnMap(DbConnection connection, string sql, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.QueryComputedColumnMap(connection, sql, schemaName, tableName);

        private static string? GetSqliteCreateTableSql(DbConnection connection, string? schemaName, string tableName)
            => DynamicEntityComputedColumnReader.GetSqliteCreateTableSql(connection, schemaName, tableName);

        private static IReadOnlyDictionary<string, ScaffoldComputedColumn> ExtractSqliteGeneratedColumns(string? createTableSql)
            => DynamicEntityComputedColumnReader.ExtractSqliteGeneratedColumns(createTableSql);

        private static (string Sql, bool Stored) NormalizeComputedColumnSql(string raw)
            => DynamicEntityComputedColumnReader.NormalizeComputedColumnSql(raw);

        private static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
            => ScaffoldSqlMetadataParser.TryTrimTrailingComputedStorageToken(sql, token, out trimmedSql);

        private static bool HasBalancedOuterParentheses(string value)
            => ScaffoldSqlMetadataParser.HasBalancedOuterParentheses(value);

        private static int FindMatchingParenthesis(string sql, int openIndex)
            => ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);

        private static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
            => ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, keyword, startIndex);

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string text)
            => ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(text);
    }
}
