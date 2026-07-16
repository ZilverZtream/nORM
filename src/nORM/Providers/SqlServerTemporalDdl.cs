using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Single source of truth for SQL Server temporal-versioning trigger DDL, shared by the
    /// runtime bootstrap (<see cref="SqlServerProvider.GenerateTemporalTriggersSql"/>) and the
    /// migration generator, which re-emits the triggers from the post-migration schema whenever
    /// a temporal table's column set changes.
    /// </summary>
    internal static class SqlServerTemporalDdl
    {
        /// <summary>The three versioning trigger names for a temporal table, in emit order.</summary>
        internal static string[] GetTriggerNames(string tableName)
            => new[] { tableName + "_TemporalInsert", tableName + "_TemporalUpdate", tableName + "_TemporalDelete" };

        /// <summary>
        /// Builds the trigger maintenance statements as (DROP, CREATE) pairs, one statement per
        /// element: drop-insert, create-insert, drop-update, create-update, drop-delete,
        /// create-delete. Callers batch them (the bootstrap joins with GO separators; the
        /// migration generator emits each as its own statement).
        /// </summary>
        internal static IReadOnlyList<string> BuildTriggerStatements(
            Func<string, string> escape,
            string tableName,
            IReadOnlyList<string> columnNames,
            IReadOnlyList<string> keyColumnNames,
            string? tenantColumnName)
        {
            var table = escape(tableName);
            var history = escape(tableName + "_History");
            var columns = string.Join(", ", columnNames.Select(escape));
            var insertedColumns = string.Join(", ", columnNames.Select(c => "i." + escape(c)));
            var deletedColumns = string.Join(", ", columnNames.Select(c => "d." + escape(c)));
            var keyCondition = string.Join(" AND ", keyColumnNames.Select(c => $"h.{escape(c)} = d.{escape(c)}"));
            var keyConditionH2 = string.Join(" AND ", keyColumnNames.Select(c => $"h2.{escape(c)} = d.{escape(c)}"));
            // Scope the history close to the same tenant (SEC-MT): the history table is not PK-unique
            // (its key includes the validity range), so matching on the entity key alone could close a
            // different tenant's open row. No-op when the tenant column is already part of the key.
            if (tenantColumnName is { } tc && !keyColumnNames.Any(k => string.Equals(k, tc, StringComparison.Ordinal)))
            {
                keyCondition += $" AND h.{escape(tc)} = d.{escape(tc)}";
                keyConditionH2 += $" AND h2.{escape(tc)} = d.{escape(tc)}";
            }

            return new[]
            {
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_TemporalInsert")};",
                $@"CREATE TRIGGER {escape(tableName + "_TemporalInsert")} ON {table} AFTER INSERT AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'I', {insertedColumns} FROM inserted i;
END;",
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_TemporalUpdate")};",
                $@"CREATE TRIGGER {escape(tableName + "_TemporalUpdate")} ON {table} AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'U', {insertedColumns} FROM inserted i;
END;",
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_TemporalDelete")};",
                $@"CREATE TRIGGER {escape(tableName + "_TemporalDelete")} ON {table} AFTER DELETE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, @Now, 'D', {deletedColumns} FROM deleted d;
END;"
            };
        }
    }

    /// <summary>
    /// Single source of truth for PostgreSQL temporal-versioning trigger DDL, shared by the
    /// runtime bootstrap and the migration generator.
    /// </summary>
    internal static class PostgresTemporalDdl
    {
        /// <summary>
        /// Builds the trigger maintenance statements: the CREATE OR REPLACE trigger function
        /// (idempotent regeneration), the DROP TRIGGER IF EXISTS, and the CREATE TRIGGER.
        /// </summary>
        internal static IReadOnlyList<string> BuildTriggerStatements(
            Func<string, string> escape,
            string tableName,
            IReadOnlyList<string> columnNames,
            IReadOnlyList<string> keyColumnNames,
            string? tenantColumnName)
        {
            var table = escape(tableName);
            var history = escape(tableName + "_History");
            var columns = string.Join(", ", columnNames.Select(escape));
            var newColumns = string.Join(", ", columnNames.Select(c => "NEW." + escape(c)));
            var oldColumns = string.Join(", ", columnNames.Select(c => "OLD." + escape(c)));
            var keyCondition = string.Join(" AND ", keyColumnNames.Select(c => $"{escape(c)} = OLD.{escape(c)}"));
            // Scope the history close to the same tenant (SEC-MT); see the SQL Server core.
            if (tenantColumnName is { } tc && !keyColumnNames.Any(k => string.Equals(k, tc, StringComparison.Ordinal)))
                keyCondition += $" AND {escape(tc)} = OLD.{escape(tc)}";
            var functionName = escape(tableName + "_TemporalFunction");

            return new[]
            {
                $@"CREATE OR REPLACE FUNCTION {functionName}() RETURNS TRIGGER AS $$
DECLARE v_now TIMESTAMP := (now() at time zone 'utc');
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'I', {newColumns});
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition};
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'U', {newColumns});
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition};
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, v_now, 'D', {oldColumns});
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;",
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_TemporalTrigger")} ON {table};",
                $@"CREATE TRIGGER {escape(tableName + "_TemporalTrigger")}
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION {functionName}();"
            };
        }
    }

    /// <summary>
    /// Single source of truth for MySQL temporal-versioning trigger DDL, shared by the runtime
    /// bootstrap and the migration generator.
    /// </summary>
    internal static class MySqlTemporalDdl
    {
        /// <summary>The three versioning trigger names for a temporal table, in emit order.</summary>
        internal static string[] GetTriggerNames(string tableName)
            => new[] { tableName + "_ai", tableName + "_au", tableName + "_ad" };

        /// <summary>
        /// Builds the trigger maintenance statements as (DROP, CREATE) pairs, one statement per
        /// element (the bootstrap joins with the '-- DELIMITER' separator; the migration
        /// generator emits each as its own statement).
        /// </summary>
        internal static IReadOnlyList<string> BuildTriggerStatements(
            Func<string, string> escape,
            string tableName,
            IReadOnlyList<string> columnNames,
            IReadOnlyList<string> keyColumnNames,
            string? tenantColumnName)
        {
            var table = escape(tableName);
            var history = escape(tableName + "_History");
            var columns = string.Join(", ", columnNames.Select(escape));
            var newColumns = string.Join(", ", columnNames.Select(c => "NEW." + escape(c)));
            var oldColumns = string.Join(", ", columnNames.Select(c => "OLD." + escape(c)));
            var keyCondition = string.Join(" AND ", keyColumnNames.Select(c => $"{escape(c)} = OLD.{escape(c)}"));
            // Scope the history close to the same tenant (SEC-MT); see the SQL Server core.
            if (tenantColumnName is { } tc && !keyColumnNames.Any(k => string.Equals(k, tc, StringComparison.Ordinal)))
                keyCondition += $" AND {escape(tc)} = OLD.{escape(tc)}";

            return new[]
            {
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_ai")};",
                $@"CREATE TRIGGER {escape(tableName + "_ai")} AFTER INSERT ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), '9999-12-31', 'I', {newColumns});
END;",
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_au")};",
                $@"CREATE TRIGGER {escape(tableName + "_au")} AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP(6) WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), '9999-12-31', 'U', {newColumns});
END;",
                $"DROP TRIGGER IF EXISTS {escape(tableName + "_ad")};",
                $@"CREATE TRIGGER {escape(tableName + "_ad")} AFTER DELETE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP(6) WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), 'D', {oldColumns});
END;"
            };
        }
    }
}
