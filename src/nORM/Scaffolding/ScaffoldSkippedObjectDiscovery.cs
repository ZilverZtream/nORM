#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldSkippedObjectInfo(string? Schema, string Name, string Kind, string Detail, string? Comment)
    {
        public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
    }

    internal static class ScaffoldSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await GetSqliteSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await GetSqlServerSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await GetPostgresSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await GetMySqlSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

            return Array.Empty<ScaffoldSkippedObjectInfo>();
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSqliteSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = new List<ScaffoldSkippedObjectInfo>();
            foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
            {
                objects.AddRange(await QuerySkippedObjectsAsync(
                    connection,
                    $"""
                    SELECT {SqliteSchemaResult(schema)} AS ObjectSchema, name AS ObjectName, 'View' AS Kind, 'SQLite view' AS Detail
                    FROM {provider.Escape(schema)}.sqlite_master
                    WHERE type = 'view'
                    UNION ALL
                    SELECT {SqliteSchemaResult(schema)}, name, 'VirtualTable', 'SQLite virtual table'
                    FROM {provider.Escape(schema)}.sqlite_master
                    WHERE type = 'table' AND UPPER(sql) LIKE 'CREATE VIRTUAL TABLE%'
                    UNION ALL
                    SELECT {SqliteSchemaResult(schema)}, m.name, 'VirtualTableShadow', 'SQLite virtual table shadow table'
                    FROM {provider.Escape(schema)}.sqlite_master m
                    WHERE m.type = 'table'
                      AND m.name NOT LIKE 'sqlite_%'
                      AND UPPER(COALESCE(m.sql, '')) NOT LIKE 'CREATE VIRTUAL TABLE%'
                      AND EXISTS (
                          SELECT 1
                          FROM {provider.Escape(schema)}.sqlite_master vt
                          WHERE vt.type = 'table'
                            AND UPPER(COALESCE(vt.sql, '')) LIKE 'CREATE VIRTUAL TABLE%'
                            AND m.name IN (
                                vt.name || '_data',
                                vt.name || '_idx',
                                vt.name || '_content',
                                vt.name || '_docsize',
                                vt.name || '_config',
                                vt.name || '_segments',
                                vt.name || '_segdir',
                                vt.name || '_stat',
                                vt.name || '_node',
                                vt.name || '_parent',
                                vt.name || '_rowid'
                            )
                      )
                    ORDER BY ObjectName
                    """).ConfigureAwait(false));
            }

            return objects;
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSqlServerSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = await QuerySkippedObjectsAsync(connection, """
                SELECT SCHEMA_NAME(v.schema_id) AS ObjectSchema, v.name AS ObjectName, 'View' AS Kind, 'SQL Server view' AS Detail
                FROM sys.views v
                WHERE v.is_ms_shipped = 0
                UNION ALL
                SELECT SCHEMA_NAME(p.schema_id), p.name, 'Routine',
                       CONCAT('SQL Server stored procedure; parameters=',
                              (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = p.object_id),
                              '; outputParameters=',
                              (SELECT COUNT(*) + 1 FROM sys.parameters pa WHERE pa.object_id = p.object_id AND pa.is_output = 1),
                              '; parameterModes=',
                              COALESCE(NULLIF((
                                  SELECT STRING_AGG(CONCAT(
                                      pa.name, ':', CASE WHEN pa.is_output = 1 THEN 'OUT' ELSE 'IN' END, ':',
                                      CASE
                                          WHEN ty.is_table_type = 1 THEN CONCAT('table type (', SCHEMA_NAME(ty.schema_id), '.', ty.name, ')')
                                          ELSE COALESCE(base_ty.name, ty.name)
                                      END,
                                      CASE
                                          WHEN ty.is_table_type = 1 THEN ''
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length) END, ')')
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length / 2) END, ')')
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric') THEN CONCAT('(', pa.precision, ',', pa.scale, ')')
                                          ELSE ''
                                      END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                                  FROM sys.parameters pa
                                  INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                  LEFT JOIN sys.types base_ty
                                    ON ty.is_user_defined = 1
                                   AND ty.is_table_type = 0
                                   AND base_ty.user_type_id = ty.system_type_id
                                   AND base_ty.is_user_defined = 0
                                  WHERE pa.object_id = p.object_id
                              ), '') + ',', '') + 'return:RETURN:int',
                              '; resultColumns=',
                              COALESCE(NULLIF((
                                  SELECT STRING_AGG(CONCAT(
                                      COALESCE(rs.name, ''),
                                      ':',
                                      COALESCE(rs.system_type_name, ''),
                                      ':',
                                      CONVERT(varchar(1), COALESCE(rs.is_nullable, 0))), '|') WITHIN GROUP (ORDER BY rs.column_ordinal)
                                  FROM sys.dm_exec_describe_first_result_set_for_object(p.object_id, NULL) rs
                                  WHERE rs.error_number IS NULL
                                    AND rs.is_hidden = 0
                              ), ''), ''))
                FROM sys.procedures p
                WHERE p.is_ms_shipped = 0
                UNION ALL
                SELECT SCHEMA_NAME(o.schema_id), o.name, 'Routine',
                       CONCAT('SQL Server ',
                              CASE
                                  WHEN o.type IN ('IF', 'TF') THEN 'table-valued function'
                                  ELSE 'scalar function'
                              END,
                              '; parameters=',
                              (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = o.object_id AND pa.parameter_id > 0),
                              '; outputParameters=',
                              CASE WHEN o.type = 'FN' THEN 1 ELSE 0 END,
                              '; callShape=',
                              CASE
                                  WHEN o.type IN ('IF', 'TF') THEN 'table-valued-function'
                                  ELSE 'scalar-function'
                              END,
                              '; parameterModes=',
                              COALESCE((
                                  SELECT STRING_AGG(CONCAT(
                                      pa.name, ':',
                                      CASE WHEN pa.parameter_id = 0 THEN 'RETURN' WHEN pa.is_output = 1 THEN 'OUT' ELSE 'IN' END,
                                      ':',
                                      CASE
                                          WHEN ty.is_table_type = 1 THEN CONCAT('table type (', SCHEMA_NAME(ty.schema_id), '.', ty.name, ')')
                                          ELSE COALESCE(base_ty.name, ty.name)
                                      END,
                                      CASE
                                          WHEN ty.is_table_type = 1 THEN ''
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length) END, ')')
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length / 2) END, ')')
                                          WHEN COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric') THEN CONCAT('(', pa.precision, ',', pa.scale, ')')
                                          ELSE ''
                                      END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                                  FROM sys.parameters pa
                                  INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                  LEFT JOIN sys.types base_ty
                                    ON ty.is_user_defined = 1
                                   AND ty.is_table_type = 0
                                   AND base_ty.user_type_id = ty.system_type_id
                                   AND base_ty.is_user_defined = 0
                                  WHERE pa.object_id = o.object_id
                              ), ''),
                              '; dataType=',
                              COALESCE((
                                  SELECT TOP (1) ty.name
                                  FROM sys.parameters pa
                                  INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                  WHERE pa.object_id = o.object_id
                                    AND pa.parameter_id = 0
                              ), CASE WHEN o.type IN ('IF', 'TF') THEN 'TABLE' ELSE '' END),
                              '; resultColumns=',
                              COALESCE(NULLIF((
                                  SELECT STRING_AGG(CONCAT(
                                      COALESCE(rs.name, ''),
                                      ':',
                                      COALESCE(rs.system_type_name, ''),
                                      ':',
                                      CONVERT(varchar(1), COALESCE(rs.is_nullable, 0))), '|') WITHIN GROUP (ORDER BY rs.column_ordinal)
                                  FROM sys.dm_exec_describe_first_result_set_for_object(o.object_id, NULL) rs
                                  WHERE rs.error_number IS NULL
                                    AND rs.is_hidden = 0
                              ), ''), NULLIF((
                                  SELECT STRING_AGG(CONCAT(
                                      c.name,
                                      ':',
                                      ty.name,
                                      CASE
                                          WHEN ty.name IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), c.max_length) END, ')')
                                          WHEN ty.name IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), c.max_length / 2) END, ')')
                                          WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', c.precision, ',', c.scale, ')')
                                          ELSE ''
                                      END,
                                      ':',
                                      CONVERT(varchar(1), c.is_nullable)), '|') WITHIN GROUP (ORDER BY c.column_id)
                                  FROM sys.columns c
                                  INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                                  WHERE c.object_id = o.object_id
                                    AND c.is_hidden = 0
                                    AND o.type IN ('IF', 'TF')
                              ), ''), ''))
                FROM sys.objects o
                WHERE o.is_ms_shipped = 0
                  AND o.type IN ('FN', 'IF', 'TF')
                UNION ALL
                SELECT SCHEMA_NAME(s.schema_id), s.name, 'Sequence',
                       CONCAT('SQL Server sequence; dataType=', ty.name,
                              CASE
                                  WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', s.precision, ',', s.scale, ')')
                                  ELSE ''
                              END)
                FROM sys.sequences s
                INNER JOIN sys.types ty ON ty.user_type_id = s.user_type_id
                UNION ALL
                SELECT SCHEMA_NAME(s.schema_id), s.name, 'Synonym',
                       CONCAT('SQL Server synonym; baseObject=', s.base_object_name,
                              '; baseType=', COALESCE(CONVERT(nvarchar(20), OBJECTPROPERTYEX(OBJECT_ID(s.base_object_name), 'BaseType')), ''))
                FROM sys.synonyms s
                ORDER BY ObjectSchema, ObjectName
                """).ConfigureAwait(false);
            return await AttachSkippedObjectCommentsAsync(connection, provider, objects).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetPostgresSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = await QuerySkippedObjectsAsync(connection, """
                SELECT table_schema AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'PostgreSQL view' AS Detail
                FROM information_schema.views
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                UNION ALL
                SELECT sequence_schema, sequence_name, 'Sequence', 'PostgreSQL sequence; dataType=' || data_type
                FROM information_schema.sequences seq
                WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_class sequence_class
                      INNER JOIN pg_namespace sequence_schema_ns ON sequence_schema_ns.oid = sequence_class.relnamespace
                      INNER JOIN pg_depend dependency ON dependency.objid = sequence_class.oid
                      WHERE sequence_class.relkind = 'S'
                        AND sequence_schema_ns.nspname = seq.sequence_schema
                        AND sequence_class.relname = seq.sequence_name
                        AND dependency.deptype IN ('a', 'i')
                  )
                UNION ALL
                SELECT schemaname, matviewname, 'MaterializedView', 'PostgreSQL materialized view'
                FROM pg_matviews
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                UNION ALL
                SELECT r.routine_schema, r.routine_name, 'Routine',
                       'PostgreSQL ' || LOWER(r.routine_type) || '; parameters=' ||
                       COALESCE((
                           SELECT COUNT(*)
                           FROM information_schema.parameters p
                           WHERE p.specific_schema = r.specific_schema
                             AND p.specific_name = r.specific_name
                             AND p.parameter_mode IS NOT NULL
                       ), 0)::text ||
                       '; outputParameters=' ||
                       COALESCE((
                           SELECT COUNT(*)
                           FROM information_schema.parameters p
                           WHERE p.specific_schema = r.specific_schema
                             AND p.specific_name = r.specific_name
                             AND p.parameter_mode IN ('OUT', 'INOUT')
                       ), 0)::text ||
                       '; parameterModes=' ||
                       COALESCE((
                           SELECT string_agg(
                               COALESCE(p.parameter_name, 'return') || ':' || COALESCE(p.parameter_mode, 'RETURN') || ':' ||
                               CASE
                                   WHEN p.data_type IN ('ARRAY', 'USER-DEFINED')
                                        AND p.udt_name IS NOT NULL
                                        AND p.udt_name <> ''
                                   THEN p.data_type || ' (' || p.udt_name || ')'
                                   ELSE COALESCE(p.data_type, '')
                               END ||
                               CASE
                                   WHEN p.character_maximum_length IS NOT NULL THEN '(' || p.character_maximum_length::text || ')'
                                   WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NULL THEN '(' || p.numeric_precision::text || ')'
                                   WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN '(' || p.numeric_precision::text || ',' || p.numeric_scale::text || ')'
                                   ELSE ''
                               END,
                               ',' ORDER BY p.ordinal_position)
                           FROM information_schema.parameters p
                           WHERE p.specific_schema = r.specific_schema
                             AND p.specific_name = r.specific_name
                             AND p.parameter_mode IS NOT NULL
                       ), '') ||
                       '; callShape=' ||
                       CASE
                           WHEN UPPER(r.routine_type) = 'FUNCTION' AND EXISTS (
                               SELECT 1
                               FROM pg_proc routine_proc
                               INNER JOIN pg_namespace routine_ns ON routine_ns.oid = routine_proc.pronamespace
                               WHERE routine_ns.nspname = r.specific_schema
                                 AND routine_proc.proname = r.routine_name
                                 AND routine_proc.proretset
                           ) THEN 'table-valued-function'
                           WHEN UPPER(r.routine_type) = 'FUNCTION' AND LOWER(COALESCE(r.data_type, '')) IN ('record', 'table') THEN 'table-valued-function'
                           WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                           ELSE ''
                       END ||
                       '; dataType=' || COALESCE(r.data_type, '')
                FROM information_schema.routines r
                WHERE r.routine_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY ObjectSchema, ObjectName
                """).ConfigureAwait(false);
            return await AttachSkippedObjectCommentsAsync(connection, provider, objects).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetMySqlSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = await QuerySkippedObjectsAsync(connection, """
                SELECT NULL AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'MySQL view' AS Detail
                FROM information_schema.views
                WHERE table_schema = DATABASE()
                UNION ALL
                SELECT NULL, r.routine_name, 'Routine',
                       CONCAT('MySQL ', r.routine_type, '; parameters=',
                              (SELECT COUNT(*)
                               FROM information_schema.parameters p
                               WHERE p.specific_schema = r.routine_schema
                                 AND p.specific_name = r.specific_name
                                 AND p.parameter_mode IS NOT NULL),
                              '; outputParameters=',
                              (SELECT COUNT(*)
                               FROM information_schema.parameters p
                               WHERE p.specific_schema = r.routine_schema
                                 AND p.specific_name = r.specific_name
                                 AND p.parameter_mode IN ('OUT', 'INOUT')),
                              '; parameterModes=',
                              COALESCE((SELECT GROUP_CONCAT(CONCAT(
                                            COALESCE(p.parameter_name, 'return'), ':', COALESCE(p.parameter_mode, 'RETURN'), ':',
                                            CASE
                                                WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN COALESCE(p.dtd_identifier, p.data_type, '')
                                                ELSE COALESCE(p.data_type, '')
                                            END,
                                            CASE
                                                WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN ''
                                                WHEN p.character_maximum_length IS NOT NULL THEN CONCAT('(', p.character_maximum_length, ')')
                                                WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NULL THEN CONCAT('(', p.numeric_precision, ')')
                                                WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN CONCAT('(', p.numeric_precision, ',', p.numeric_scale, ')')
                                                ELSE ''
                                            END) ORDER BY p.ordinal_position SEPARATOR ',')
                                        FROM information_schema.parameters p
                                        WHERE p.specific_schema = r.routine_schema
                                          AND p.specific_name = r.specific_name
                                          AND p.parameter_mode IS NOT NULL), ''),
                              '; callShape=',
                              CASE
                                  WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                                  ELSE ''
                              END,
                              '; dataType=', COALESCE(r.data_type, ''))
                FROM information_schema.routines r
                WHERE r.routine_schema = DATABASE()
                UNION ALL
                SELECT NULL, event_name, 'Event',
                       CONCAT('MySQL event; eventType=', COALESCE(event_type, ''),
                              '; status=', COALESCE(status, ''),
                              '; intervalValue=', COALESCE(interval_value, ''),
                              '; intervalField=', COALESCE(interval_field, ''),
                              '; executeAt=', COALESCE(DATE_FORMAT(execute_at, '%Y-%m-%d %H:%i:%s'), ''),
                              '; starts=', COALESCE(DATE_FORMAT(starts, '%Y-%m-%d %H:%i:%s'), ''),
                              '; ends=', COALESCE(DATE_FORMAT(ends, '%Y-%m-%d %H:%i:%s'), ''))
                FROM information_schema.events
                WHERE event_schema = DATABASE()
                ORDER BY ObjectSchema, ObjectName
                """).ConfigureAwait(false);
            return await AttachSkippedObjectCommentsAsync(connection, provider, objects).ConfigureAwait(false);
        }
        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> QuerySkippedObjectsAsync(DbConnection connection, string sql)
        {
            var objects = new List<ScaffoldSkippedObjectInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var objectName = Convert.ToString(reader["ObjectName"]);
                if (string.IsNullOrWhiteSpace(objectName))
                    continue;

                objects.Add(new ScaffoldSkippedObjectInfo(
                    NullIfWhiteSpace(Convert.ToString(reader["ObjectSchema"])),
                    objectName,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty,
                    null));
            }

            return objects;
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> AttachSkippedObjectCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldSkippedObjectInfo> objects)
        {
            if (objects.Count == 0)
                return objects;

            var comments = await GetSkippedObjectCommentsAsync(connection, provider).ConfigureAwait(false);
            if (comments.Count == 0)
                return objects;

            var result = new ScaffoldSkippedObjectInfo[objects.Count];
            for (var i = 0; i < objects.Count; i++)
            {
                var obj = objects[i];
                result[i] = comments.TryGetValue(SkippedObjectCommentKey(obj.Schema, obj.Name, obj.Kind), out var comment)
                    ? obj with { Comment = comment }
                    : obj;
            }

            return result;
        }

        private static async Task<IReadOnlyDictionary<string, string>> GetSkippedObjectCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT SCHEMA_NAME(o.schema_id) AS ObjectSchema,
                           o.name AS ObjectName,
                           'Routine' AS Kind,
                           CAST(comment.value AS nvarchar(max)) AS ObjectComment
                    FROM sys.objects o
                    LEFT JOIN sys.extended_properties comment
                      ON comment.major_id = o.object_id
                     AND comment.minor_id = 0
                     AND comment.name = N'MS_Description'
                    WHERE o.is_ms_shipped = 0
                      AND o.type IN ('P', 'FN', 'IF', 'TF')
                    UNION ALL
                    SELECT SCHEMA_NAME(seq.schema_id),
                           seq.name,
                           'Sequence',
                           CAST(comment.value AS nvarchar(max))
                    FROM sys.sequences seq
                    LEFT JOIN sys.extended_properties comment
                      ON comment.major_id = seq.object_id
                     AND comment.minor_id = 0
                     AND comment.name = N'MS_Description'
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT n.nspname AS ObjectSchema,
                           p.proname AS ObjectName,
                           'Routine' AS Kind,
                           obj_description(p.oid, 'pg_proc') AS ObjectComment
                    FROM pg_proc p
                    INNER JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pg_proc sibling
                          WHERE sibling.pronamespace = p.pronamespace
                            AND sibling.proname = p.proname
                            AND sibling.oid <> p.oid
                      )
                    UNION ALL
                    SELECT n.nspname,
                           cls.relname,
                           'Sequence',
                           obj_description(cls.oid, 'pg_class')
                    FROM pg_class cls
                    INNER JOIN pg_namespace n ON n.oid = cls.relnamespace
                    WHERE cls.relkind = 'S'
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT NULL AS ObjectSchema,
                           routine_name AS ObjectName,
                           'Routine' AS Kind,
                           routine_comment AS ObjectComment
                    FROM information_schema.routines
                    WHERE routine_schema = DATABASE()
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, string>> QuerySkippedObjectCommentsAsync(DbConnection connection, string sql)
        {
            var comments = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var objectName = Convert.ToString(reader["ObjectName"]);
                var kind = Convert.ToString(reader["Kind"]);
                var comment = NullIfWhiteSpace(Convert.ToString(reader["ObjectComment"]));
                if (string.IsNullOrWhiteSpace(objectName)
                    || string.IsNullOrWhiteSpace(kind)
                    || comment is null)
                {
                    continue;
                }

                var key = SkippedObjectCommentKey(
                    NullIfWhiteSpace(Convert.ToString(reader["ObjectSchema"])),
                    objectName!,
                    kind!);
                if (!comments.ContainsKey(key))
                    comments[key] = comment;
            }

            return comments;
        }

        private static string SkippedObjectCommentKey(string? schema, string name, string kind)
            => TableKey(schema, name) + "|" + kind.Trim();

        public static async Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
        {
            var schemas = new List<string>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA database_list";
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = Convert.ToString(reader["name"]);
                if (string.IsNullOrWhiteSpace(schema)
                    || string.Equals(schema, "temp", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                schemas.Add(schema);
            }

            return schemas.Count == 0 ? new[] { "main" } : schemas;
        }

        public static string SqliteSchemaResult(string schema)
            => string.Equals(schema, "main", StringComparison.OrdinalIgnoreCase)
                ? "NULL"
                : "'" + schema.Replace("'", "''") + "'";

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
