#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCommentDiscovery
    {
        private static Task<IReadOnlyDictionary<string, ScaffoldComments>> GetSqlServerScaffoldCommentsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys)
            => QueryScaffoldCommentsAsync(connection, tableKeys, """
                SELECT SCHEMA_NAME(o.schema_id) AS TableSchema,
                       o.name AS TableName,
                       CAST(table_comment.value AS nvarchar(max)) AS TableComment,
                       c.name AS ColumnName,
                       CAST(column_comment.value AS nvarchar(max)) AS ColumnComment
                FROM sys.objects o
                INNER JOIN sys.columns c ON c.object_id = o.object_id
                LEFT JOIN sys.extended_properties table_comment
                  ON table_comment.major_id = o.object_id
                 AND table_comment.minor_id = 0
                 AND table_comment.name = N'MS_Description'
                LEFT JOIN sys.extended_properties column_comment
                  ON column_comment.major_id = o.object_id
                 AND column_comment.minor_id = c.column_id
                 AND column_comment.name = N'MS_Description'
                WHERE o.is_ms_shipped = 0
                  AND o.type IN ('U', 'V')
                UNION ALL
                SELECT SCHEMA_NAME(syn.schema_id) AS TableSchema,
                       syn.name AS TableName,
                       CAST(table_comment.value AS nvarchar(max)) AS TableComment,
                       c.name AS ColumnName,
                       CAST(column_comment.value AS nvarchar(max)) AS ColumnComment
                FROM sys.synonyms syn
                INNER JOIN sys.objects base_object
                  ON base_object.object_id = OBJECT_ID(syn.base_object_name)
                INNER JOIN sys.columns c ON c.object_id = base_object.object_id
                LEFT JOIN sys.extended_properties table_comment
                  ON table_comment.major_id = base_object.object_id
                 AND table_comment.minor_id = 0
                 AND table_comment.name = N'MS_Description'
                LEFT JOIN sys.extended_properties column_comment
                  ON column_comment.major_id = base_object.object_id
                 AND column_comment.minor_id = c.column_id
                 AND column_comment.name = N'MS_Description'
                WHERE base_object.is_ms_shipped = 0
                  AND base_object.type IN ('U', 'V')
                """);
    }
}
